#!/usr/bin/env python3
"""Supervise a leaf NATS server using refreshed, pinned-operator account claims.

Only public JWTs travel over HTTPS. The operator remains pinned to the offline
trust file; signatures, increasing revisions and revocation monotonicity are
checked before NATS validates and reloads the candidate. A stale feed eventually
stops the child, and a successful refresh starts it again.
"""
import argparse
import base64
import binascii
import concurrent.futures
import hashlib
import json
import os
import pathlib
import re
import signal
import ssl
import subprocess
import time
import urllib.parse
import urllib.request

from nacl.signing import VerifyKey

TOKEN = r"[A-Za-z0-9_.-]+"


def b64(value):
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def claims(token):
    head, body, signature = token.split(".")
    if json.loads(b64(head)).get("alg") != "ed25519-nkey":
        raise ValueError("unexpected JWT algorithm")
    return json.loads(b64(body))


def public_key(nkey, prefix=112):
    raw = base64.b32decode(nkey + "=" * (-len(nkey) % 8))
    if len(raw) != 35 or raw[0] != prefix or int.from_bytes(raw[-2:], "little") != binascii.crc_hqx(raw[:-2], 0):
        raise ValueError("invalid pinned operator nkey")
    return VerifyKey(raw[1:-2])


def verify(token, issuer, prefix=112):
    head, body, signature = token.split(".")
    result = claims(token)
    if result.get("iss") != issuer:
        raise ValueError("JWT issuer differs from pinned operator")
    public_key(issuer, prefix).verify(f"{head}.{body}".encode(), b64(signature))
    return result


def account_token(text, account):
    match = re.search(r"(?m)^\s*" + re.escape(account) + r':\s*"?(' + TOKEN + r")", text)
    if not match:
        raise ValueError("pinned account absent from trust")
    return match.group(1), match.span(1)


def pinned(text):
    operator = re.search(r"(?m)^operator:\s*\"?(" + TOKEN + r")", text)
    account = re.search(r'Account "ZEBRIDGE"\s*\n\s*(A[A-Z2-7]{55}):', text)
    if not operator or not account:
        raise ValueError("offline trust lacks the operator or ZEBRIDGE account")
    identity = claims(operator.group(1)).get("sub")
    root = verify(operator.group(1), identity)
    if root.get("nats", {}).get("type") != "operator":
        raise ValueError("invalid pinned operator claim")
    return identity, account.group(1)


def account_claims(token, operator, account):
    result = verify(token, operator)
    if result.get("sub") != account or result.get("nats", {}).get("type") != "account":
        raise ValueError("wrong account claim")
    issued = result.get("iat")
    if type(issued) is not int or issued < 0 or issued > time.time() + 30:
        raise ValueError("invalid account revision timestamp")
    if "exp" in result and (type(result["exp"]) is not int or result["exp"] <= time.time()):
        raise ValueError("account claim expired")
    if "nbf" in result and (type(result["nbf"]) is not int or result["nbf"] > time.time()):
        raise ValueError("account claim is not yet valid")
    revocations = result["nats"].get("revocations", {})
    if not isinstance(revocations, dict) or any(not isinstance(key, str) or type(value) is not int or value < 0 for key, value in revocations.items()):
        raise ValueError("invalid revocation map")
    return result


def updated_trust(old_text, new_token, operator, account):
    previous_token, (start, end) = account_token(old_text, account)
    previous = account_claims(previous_token, operator, account)
    current = account_claims(new_token, operator, account)
    if previous_token == new_token:
        return old_text, False
    if current["iat"] <= previous["iat"]:
        raise ValueError("account revision is older or reuses its predecessor's timestamp")
    old_revocations = previous["nats"].get("revocations", {})
    new_revocations = current["nats"].get("revocations", {})
    if any(type(value) is not int or type(new_revocations.get(key)) is not int or
           new_revocations[key] < value for key, value in old_revocations.items()):
        raise ValueError("account update removes or decreases a revocation")
    return old_text[:start] + new_token + old_text[end:], True


def atomic_write(path, content):
    path = pathlib.Path(path)
    temporary = path.with_name(path.name + ".new")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o640)
    try:
        with os.fdopen(fd, "w") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


class Trust:
    def __init__(self, args):
        self.args = args
        self.directory = pathlib.Path(args.state_dir)
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o750)
        self.path = self.directory / "trust.conf"
        self.runtime = self.directory / "nats-runtime.conf"
        self.status = self.directory / "last-success.json"
        offline = pathlib.Path(args.pinned).read_text()
        self.operator, self.account = pinned(offline)
        account_claims(account_token(offline, self.account)[0], self.operator, self.account)
        if not self.path.exists():
            atomic_write(self.path, offline)
        active = self.path.read_text()
        if pinned(active) != (self.operator, self.account):
            raise ValueError("active trust differs from the offline operator pin")
        account_claims(account_token(active, self.account)[0], self.operator, self.account)
        # All generated leaf configurations have exactly this trust include. NATS
        # interprets includes relative to the config, including absolute paths.
        self.config = pathlib.Path(args.config).read_text()
        self.config, count = re.subn(r"(?m)^\s*include\s+[\"']?\./trust\.conf[\"']?\s*$", "include ./trust.conf", self.config)
        if count != 1:
            raise ValueError("expected exactly one ./trust.conf include")
        atomic_write(self.runtime, self.config)
        credential = pathlib.Path(args.link_creds).read_text()
        match = re.search(r"-----BEGIN NATS USER JWT-----\s*(\S+)", credential)
        if not match:
            raise ValueError("leaf link credentials lack a public JWT")
        self.link = verify(match.group(1), self.account, prefix=0)
        expected_tags = sorted(["leaf-policy:1", "leaf:" + args.leaf_name] + ["tenant:" + tenant for tenant in args.tenants.split()])
        if not args.tenants.split() or self.link.get("name") != args.leaf_name or self.link["nats"].get("allowed_connection_types") != ["LEAFNODE"] or sorted(self.link["nats"].get("tags", [])) != expected_tags:
            raise ValueError("leaf credential scope differs from its configured namespace and tenant list")
        self.context = ssl.create_default_context(cafile=args.ca)
        self.last_success = None
        try:
            status = json.loads(self.status.read_text())
            token = account_token(active, self.account)[0]
            age = time.time() - status["time"]
            if status["sha256"] == hashlib.sha256(token.encode()).hexdigest() and 0 <= age < args.max_stale:
                self.last_success = time.monotonic() - age
        except (OSError, ValueError, KeyError, TypeError):
            pass

    def refresh(self):
        request = urllib.request.Request(self.args.feed, headers={"Cache-Control": "no-cache", "Pragma": "no-cache"})
        with urllib.request.urlopen(request, context=self.context, timeout=5) as response:
            original_url = urllib.parse.urlsplit(self.args.feed)
            final_url = urllib.parse.urlsplit(response.geturl())
            if final_url.scheme != "https" or final_url.netloc != original_url.netloc:
                raise ValueError("account feed redirected outside its pinned HTTPS origin")
            data = response.read((1 << 20) + 1)
        if len(data) > 1 << 20:
            raise ValueError("account response exceeds its size limit")
        result = json.loads(data)
        if result.get("account") != self.account or not isinstance(result.get("jwt"), str):
            raise ValueError("wrong account response")
        token = result["jwt"]
        text, changed = updated_trust(self.path.read_text(), token, self.operator, self.account)
        if changed:
            candidate = self.directory / "trust.candidate.conf"
            check_config = self.directory / "nats-check.conf"
            try:
                atomic_write(candidate, text)
                atomic_write(check_config, self.config.replace("include ./trust.conf", "include ./trust.candidate.conf"))
                checked = subprocess.run([self.args.server, "-t", "-c", str(check_config)],
                                         capture_output=True, text=True, timeout=10)
                if checked.returncode:
                    raise ValueError("NATS rejected the candidate configuration")
                atomic_write(self.path, text)
            finally:
                candidate.unlink(missing_ok=True)
                check_config.unlink(missing_ok=True)
            print("leaf account trust updated", flush=True)
        atomic_write(self.status, json.dumps({"time": time.time(), "sha256": hashlib.sha256(token.encode()).hexdigest()}))
        self.last_success = time.monotonic()
        return changed

    def fresh(self):
        active = account_claims(account_token(self.path.read_text(), self.account)[0], self.operator, self.account)
        if active["nats"].get("revocations", {}).get(self.link["sub"], -1) >= self.link["iat"] or self.link.get("exp", 0) <= time.time():
            return False
        return self.last_success is not None and time.monotonic() - self.last_success < self.args.max_stale


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--pinned", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--link-creds", required=True)
    parser.add_argument("--leaf-name", default=os.environ.get("LEAF_NAME"), required=not bool(os.environ.get("LEAF_NAME")))
    parser.add_argument("--tenants", default=os.environ.get("LEAF_TENANTS"), required=not bool(os.environ.get("LEAF_TENANTS")))
    parser.add_argument("--feed", default=os.environ.get("TRUST_FEED_URL"), required=not bool(os.environ.get("TRUST_FEED_URL")))
    parser.add_argument("--ca", default=os.environ.get("TRUST_CA_FILE"))
    parser.add_argument("--server", default="nats-server")
    parser.add_argument("--interval", type=float, default=30)
    parser.add_argument("--max-stale", type=float, default=float(os.environ.get("TRUST_MAX_STALE_SECONDS", "300")))
    parser.add_argument("--once", action="store_true", help="validate and refresh, without starting a server")
    args = parser.parse_args()
    url = urllib.parse.urlsplit(args.feed)
    if url.scheme != "https" or not url.hostname or url.username or url.password or args.interval <= 0 or args.max_stale <= args.interval:
        parser.error("a public HTTPS feed and a staleness limit greater than the refresh interval are required")
    trust = Trust(args)
    if args.once:
        trust.refresh()
        return
    stopping = False
    child = None

    def terminate(_sig, _frame):
        nonlocal stopping
        stopping = True

    def reload(_sig, _frame):
        if child and child.poll() is None:
            child.send_signal(signal.SIGHUP)

    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    signal.signal(signal.SIGHUP, reload)
    next_refresh = next_start = 0
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    refresh = None
    try:
        while not stopping:
            now = time.monotonic()
            # Keep the lease watchdog responsive while HTTPS or NATS validation
            # is blocked. Only one refresh writes trust state at a time.
            if refresh is not None and refresh.done():
                try:
                    if refresh.result() and child and child.poll() is None:
                        child.send_signal(signal.SIGHUP)
                except Exception as error:
                    print(f"leaf trust refresh failed: {type(error).__name__}: {str(error)[:160]}", flush=True)
                refresh = None
                next_refresh = time.monotonic() + args.interval
            if refresh is None and now >= next_refresh:
                refresh = pool.submit(trust.refresh)
            if child and child.poll() is not None:
                print(f"leaf NATS exited ({child.returncode})", flush=True)
                child = None
                next_start = time.monotonic() + 5
            if not trust.fresh() and child:
                print("leaf trust is stale or its link identity is revoked: stopping NATS until a verified refresh succeeds", flush=True)
                child.terminate()
                child.wait(timeout=10)
                child = None
            if trust.fresh() and child is None and now >= next_start:
                child = subprocess.Popen([args.server, "-c", str(trust.runtime)])
            time.sleep(.25)
    finally:
        if child and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        pool.shutdown(wait=True, cancel_futures=True)


if __name__ == "__main__":
    main()
