#!/usr/bin/env python3
"""Point the bridge's invite signing key at this dev stack's account.

GET /enroll signs each device's JWT with ZB_SIGNING_SEED and names ZB_ACCOUNT_PUB as its account.
A key from another operator (an older stack, another deployment) makes the dev NATS refuse every
enrolled device with "Authorization Violation" ("account missing" in its log), while the scenarios'
.creds files still work. The bootstrap adds client signing keys to the ZEBRIDGE account; any of them
whose seed is in the store will do. Leaves the env file alone when its key is already one of them,
otherwise replaces those two lines. Prints public keys only.

    python3 scripts/dev/enroll_key.py [env-file]     # default: the repository's bridge env file
"""
import base64
import json
import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
STORE = ROOT / "scripts/dev/state/nsc-store/data/nats/nsc"
ENV = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / ("." + "env.bridge")


def claims(jwt: str) -> dict:
    part = jwt.strip().split(".")[1]
    return json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))


def seed_of(pub: str) -> str | None:
    path = STORE / "keys/keys" / pub[0] / pub[1:3] / f"{pub}.nk"
    return path.read_text().strip() if path.exists() else None


def main() -> int:
    account_jwt = STORE / "stores/ZeBridgeOp/accounts/ZEBRIDGE/ZEBRIDGE.jwt"
    if not account_jwt.exists():
        print(f"enroll_key: no dev account at {account_jwt} — run scripts/dev/up.sh first", file=sys.stderr)
        return 1
    if not ENV.exists():
        print(f"enroll_key: {ENV} does not exist", file=sys.stderr)
        return 1
    c = claims(account_jwt.read_text())
    account = c["sub"]
    clients = [k["key"] for k in c["nats"].get("signing_keys", []) if isinstance(k, dict) and k.get("role") == "client"]
    seeds = {pub: s for pub in clients if (s := seed_of(pub))}
    if not seeds:
        print("enroll_key: the dev account has no client signing key with a seed in the store", file=sys.stderr)
        return 1

    lines = ENV.read_text().splitlines(keepends=True)
    current = {l.split("=", 1)[0]: l.split("=", 1)[1].strip() for l in lines if l.startswith(("ZB_SIGNING_SEED=", "ZB_ACCOUNT_PUB="))}
    in_use = next((pub for pub, s in seeds.items() if s == current.get("ZB_SIGNING_SEED")), None)
    if in_use and current.get("ZB_ACCOUNT_PUB") == account:
        print(f"enroll_key: already this stack's key ({in_use}, account {account})")
        return 0

    pub = in_use or sorted(seeds)[0]
    wanted = {"ZB_SIGNING_SEED": seeds[pub], "ZB_ACCOUNT_PUB": account}
    out, seen = [], set()
    for l in lines:
        name = l.split("=", 1)[0]
        if name in wanted:
            out.append(f"{name}={wanted[name]}\n")
            seen.add(name)
        else:
            out.append(l)
    if out and not out[-1].endswith("\n"):
        out[-1] += "\n"
    out += [f"{n}={v}\n" for n, v in wanted.items() if n not in seen]
    tmp = ENV.with_name(ENV.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, ENV.stat().st_mode & 0o777)
    with os.fdopen(fd, "w") as f:
        f.writelines(out)
    os.replace(tmp, ENV)
    print(f"enroll_key: {ENV.name} now signs invites with {pub} (account {account}); restart a running bridge")
    return 0


if __name__ == "__main__":
    sys.exit(main())
