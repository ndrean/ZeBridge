#!/usr/bin/env python3
"""`bridge --demo-invites`: a public demo's front door, its own process.

  A  GET /demo answers 302 to <redirect>?invite=<code>, the code is a fresh unused invite
     for a principal demo-<12 hex> in the demo tenant, lasting the asked time;
  B  GET /health answers 200; another path 404; another method 405;
  C  20 visitors at once get 20 different invites (one shared connection, under a lock);
  D  a client that connects and sends nothing does not hold the others up;
  E  a login without zebridge_inviter: the visitor gets a 503 and a plain sentence, the
     process keeps serving;
  F  a POST that declares no body is answered 405 and the process keeps serving (std.http
     asserted on it when keeping the connection alive).

Needs the dev database with the init SQL applied (zebridge_invite) and the bridge built.

Usage:  scripts/scenarios/.venv/bin/python scripts/scenarios/demo_invites.py
"""
import concurrent.futures, http.client, os, re, secrets, socket, subprocess, sys, time

import zb

BRIDGE = zb.ROOT / "zig-out" / "bin" / "bridge"
HOST = os.environ.get("ZB_PG_HOST", "127.0.0.1")
PG_PORT = os.environ.get("ZB_PG_PORT", "5432")
PORT = 27499
INVITER, STRANGER = "zz_demo_inviter", "zz_demo_stranger"
REDIRECT = "http://localhost:5177/"


def get(path: str, method: str = "GET"):
    c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=10)
    c.request(method, path)
    r = c.getresponse()
    body = r.read().decode(errors="replace")
    c.close()
    return r.status, r.getheader("location") or "", body


def start(role: str, password: str) -> subprocess.Popen:
    env = dict(os.environ, INVITER_DATABASE_URL=f"postgres://{role}:{password}@{HOST}:{PG_PORT}/postgres")
    p = subprocess.Popen([str(BRIDGE), "--demo-invites", "--redirect", REDIRECT, "--tenant", "demo", "--expires", "1 hour",
                          "--listen", f"127.0.0.1:{PORT}"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    for _ in range(50):
        try:
            socket.create_connection(("127.0.0.1", PORT), 0.2).close()
            return p
        except OSError:
            if p.poll() is not None:
                raise SystemExit(f"the demo process exited: {p.stderr.read()[:300]}")
            time.sleep(0.1)
    raise SystemExit("the demo process never listened")


def cleanup():
    zb.psql("DELETE FROM public.zebridge_invites WHERE principal LIKE 'demo-%' AND tenant_id = 'demo' AND used_at IS NULL", quiet=True)
    for role in (INVITER, STRANGER):
        zb.psql(f"DROP ROLE IF EXISTS {role}", quiet=True)


def main() -> int:
    failed = 0

    def check(good, label):
        nonlocal failed
        (zb.ok if good else zb.bad)(label)
        failed += 0 if good else 1

    cleanup()
    pw1, pw2 = secrets.token_hex(16), secrets.token_hex(16)
    zb.psql(f"CREATE ROLE {INVITER} LOGIN PASSWORD '{pw1}'; GRANT zebridge_inviter TO {INVITER}", quiet=True)
    zb.psql(f"CREATE ROLE {STRANGER} LOGIN PASSWORD '{pw2}'", quiet=True)
    p = start(INVITER, pw1)
    try:
        # A
        status, loc, _ = get("/demo")
        m = re.fullmatch(re.escape(REDIRECT) + r"\?invite=([0-9a-f]{32})", loc)
        row = zb.psql(f"SELECT principal, tenant_id, round(extract(epoch FROM expires_at - created_at)), used_at IS NULL "
                      f"FROM public.zebridge_invites WHERE code = '{m.group(1)}'", quiet=True).strip() if m else ""
        good_row = bool(re.fullmatch(r"demo-[0-9a-f]{12}\|demo\|3600\|t", row))
        check(status == 302 and m and good_row, f"A 302 to the page with a fresh invite: {row or loc or status}")

        # B
        s1, _, _ = get("/health")
        s2, _, _ = get("/admin")
        s3, _, _ = get("/demo", "POST")
        check((s1, s2, s3) == (200, 404, 405), f"B /health 200, another path 404, POST 405 ({s1}, {s2}, {s3})")

        # C
        with concurrent.futures.ThreadPoolExecutor(20) as ex:
            got = list(ex.map(lambda _: get("/demo"), range(20)))
        codes = {re.search(r"invite=([0-9a-f]{32})", loc).group(1) for st, loc, _ in got if st == 302}
        check(len(codes) == 20, f"C 20 visitors at once, 20 different invites ({len(codes)})")

        # D
        silent = socket.create_connection(("127.0.0.1", PORT), 2)
        t0 = time.monotonic()
        s, _, _ = get("/demo")
        check(s == 302 and time.monotonic() - t0 < 2, f"D a silent client holds no one up ({s}, {time.monotonic() - t0:.2f} s)")
        silent.close()

        # F — a POST that declares no body (curl -X POST sends exactly this): std.http's
        # keep-alive path asserted on it, a Debug build panicked, a release one had
        # undefined behaviour. Answered 405, and the process still serves.
        raw = socket.create_connection(("127.0.0.1", PORT), 5)
        raw.sendall(b"POST /demo HTTP/1.1\r\nHost: x\r\n\r\n")
        head = raw.recv(64).decode(errors="replace").split("\r\n")[0]
        raw.close()
        s, _, _ = get("/health")
        check("405" in head and s == 200 and p.poll() is None, f"F a POST with no body declared: '{head}', still serving (health {s})")
    finally:
        p.terminate()
        p.wait(5)

    # E
    p = start(STRANGER, pw2)
    try:
        s, loc, body = get("/demo")
        s2, _, _ = get("/health")
        check(s == 503 and "try again" in body and not loc and s2 == 200,
              f"E without the grant: 503 and a sentence, still serving ({s}, health {s2})")
    finally:
        p.terminate()
        p.wait(5)
        cleanup()
    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    sys.exit(main())
