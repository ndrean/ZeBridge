#!/usr/bin/env python3
"""Mint a RESPONDER credential without nsc (§10hk).

    scripts/native/mint_responder.py --seed "$ZB_RESPONDER_SEED" --account "$ZB_ACCOUNT_PUB" \\
        --name pois --tenant kilo [--tenant acme] [--ttl-days 3650] > pois.creds

A responder is a service that answers `query.<tenant>.<name>` from a replica: it reads
like a client and answers, never writes. Its permissions are the account's responder
signing-key TEMPLATE (`bridge --init-nats --mode operator` renders it; so does
scripts/native/jwt-bootstrap.sh) — this script only names the user and tags its
tenants; it cannot widen what the user may do. The JWT is the same document
src/jwt_mint.zig mints: jti = base32(sha256) of the claims, ed25519-nkey signature over
`header.claims`, tags `tenant:<t>` expanded by the server once per value.

With nsc (the dev stack) the same is `nsc add user --account ZEBRIDGE --name pois
-K <responder key> --tag tenant:kilo` — this script is for the stack the bridge
generated, which has no nsc store.
"""
import argparse, base64, hashlib, json, os, sys, time

try:
    import nkeys
except ImportError:
    sys.exit("pip install nkeys (the nats-py extra: nats-py[nkeys])")

PREFIX_SEED, PREFIX_USER = 18 << 3, 20 << 3   # 'S' and 'U' in the nkeys alphabet


def b32(raw: bytes) -> str:
    return base64.b32encode(raw).decode().rstrip("=")


def b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def encode_user_seed(raw32: bytes) -> bytes:
    b1 = PREFIX_SEED | (PREFIX_USER >> 5)
    b2 = (PREFIX_USER & 31) << 3
    body = bytes([b1, b2]) + raw32
    return b32(body + nkeys.crc16(body).to_bytes(2, "little")).encode()


def encode_user_public(pub32: bytes) -> str:
    body = bytes([PREFIX_USER]) + pub32
    return b32(body + nkeys.crc16(body).to_bytes(2, "little"))


def mint(signing_seed: bytes, account_pub: str, name: str, tenants: list, ttl_s: int, now: int) -> tuple:
    signer = nkeys.from_seed(signing_seed)
    issuer = signer.public_key.decode()
    user_seed = encode_user_seed(os.urandom(32))
    user = nkeys.from_seed(user_seed)
    user_pub = user.public_key.decode()
    tags = ",".join(json.dumps(f"tenant:{t}") for t in tenants)
    claims = ('{"jti":"%s","iat":%d,"exp":%d,"iss":"%s","name":"%s","sub":"%s",'
              '"nats":{"pub":{},"sub":{},"issuer_account":"%s","tags":[%s],"type":"user","version":2}}')
    hashed = claims % ("", now, now + ttl_s, issuer, name, user_pub, account_pub, tags)
    jti = b32(hashlib.sha256(hashed.encode()).digest())
    body = claims % (jti, now, now + ttl_s, issuer, name, user_pub, account_pub, tags)
    header = b64url(b'{"typ":"JWT","alg":"ed25519-nkey"}')
    signing_input = f"{header}.{b64url(body.encode())}"
    sig = signer.sign(signing_input.encode())
    return f"{signing_input}.{b64url(sig)}", user_seed.decode()


def creds_file(jwt: str, seed: str) -> str:
    return ("-----BEGIN NATS USER JWT-----\n" + jwt + "\n------END NATS USER JWT------\n\n"
            "************************* IMPORTANT *************************\n"
            "NKEY Seed printed below can be used to sign and prove identity.\n"
            "NKEYs are sensitive and should be treated as secrets.\n\n"
            "-----BEGIN USER NKEY SEED-----\n" + seed + "\n------END USER NKEY SEED------\n\n"
            "*************************************************************\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed", required=True, help="the account's RESPONDER signing-key seed (SA…), or a path to a .nk file holding it")
    ap.add_argument("--account", required=True, help="the account's public key (A…), ZB_ACCOUNT_PUB")
    ap.add_argument("--name", required=True, help="the principal — the JWT's user name")
    ap.add_argument("--tenant", action="append", default=[], help="a tenant the responder answers for (repeatable); the open tenant is always granted")
    ap.add_argument("--ttl-days", type=int, default=3650, help="a service rotates with a redeploy, not a TTL")
    a = ap.parse_args()
    seed = a.seed.strip()
    if os.path.exists(seed):
        seed = open(seed).read().strip()
    if not seed.startswith("SA"):
        sys.exit("the seed must be an ACCOUNT signing-key seed (SA…) — the responder key, not a user seed")
    if not a.account.startswith("A"):
        sys.exit("the account public key starts with A")
    jwt, user_seed = mint(seed.encode(), a.account, a.name, a.tenant, a.ttl_days * 86400, int(time.time()))
    sys.stdout.write(creds_file(jwt, user_seed))


if __name__ == "__main__":
    main()
