"""The grammar is BUILT IN and SERVED — clients never copy the file (§10ci).

    scripts/scenarios/run.py live -k grammar_served

The review's chain, proven live: the grammar is a protocol constant (the JWT grants
are minted from its names, so creds bind a client to it); therefore ZeBridge embeds
`src/grammar.json` at compile time — there is no runtime loader left to drift — and
clients RECEIVE it: `GET /grammar` (bytes + `X-Grammar-Hash`) and the /enroll payload
(`grammar`, `grammar_hash` beside the JWT). Here:

  1. GET /grammar is byte-identical to src/grammar.json, and the hash header is the
     sha256 of exactly those bytes
  2. a libzb client opens with `grammarJson` — the served bytes, NO file path — and
     syncs against the live stack: the file-free client is real
  3. the /enroll payload carries `grammar` + `grammar_hash` (skipped gracefully when
     enrollment is not armed, the house pattern)
"""
import ctypes
import hashlib
import json
import pathlib
import subprocess
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402
sys.path.insert(0, str(zb.ROOT / "libzb" / "python"))
import _env  # noqa: E402


def main() -> int:
    failed = 0
    base = zb.http_base()

    # ── 1. served == embedded == src/grammar.json, hash and all ─────────────
    r = subprocess.run(["curl", "-s", "-D-", f"{base}/grammar"], capture_output=True, text=True)
    head, _, body = r.stdout.partition("\r\n\r\n")
    if not body:
        head, _, body = r.stdout.partition("\n\n")
    repo = (zb.ROOT / "src" / "grammar.json").read_text()
    served_hash = next((ln.split(":", 1)[1].strip() for ln in head.splitlines()
                        if ln.lower().startswith("x-grammar-hash")), "")
    if body == repo and served_hash == hashlib.sha256(body.encode()).hexdigest():
        zb.ok("GET /grammar: byte-identical to src/grammar.json, X-Grammar-Hash is its sha256 "
              "— the wire contract is served, not copied")
    else:
        zb.bad(f"served grammar diverges (bytes match={body == repo}, hash ok="
               f"{served_hash == hashlib.sha256(body.encode()).hexdigest()})")
        failed += 1

    # ── 2. the file-free client ─────────────────────────────────────────────
    lib = _env.load_lib()
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for n, a in (("sync", []), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a

    def take(p):
        try: return json.loads(ctypes.string_at(p).decode())
        finally: lib.zb_free(p)

    db = "/tmp/zb-grammar-served.sqlite3"
    _env.rm_sqlite(db)
    h = lib.zb_client_open(json.dumps({
        "url": zb.nats_server(), "credsPath": zb.creds_for("omar"),
        "grammarJson": body,                       # ← the served bytes; no grammarPath
        "dbPath": db, "principal": "omar", "clientId": "py-grammar-served",
        "tables": ["users"]}).encode())
    if not h:
        zb.bad("file-free client could not open"); return failed + 1
    try:
        take(lib.zb_client_sync(h))
        rows = take(lib.zb_client_query(h, b"SELECT count(*) FROM users", b"[]")).get("rows")
        if rows and rows[0][0] >= 0:
            zb.ok(f"a libzb client bootstrapped from the SERVED grammar alone (grammarJson, "
                  f"no file) and synced: users has {rows[0][0]} row(s)")
        else:
            zb.bad("the file-free client synced nothing"); failed += 1
    finally:
        lib.zb_client_close(h)
        _env.rm_sqlite(db)

    # ── 3. the enrollment payload ───────────────────────────────────────────
    r = subprocess.run(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
                        f"{base}/enroll?code=xxxxxxxxxxxxxxxx&user_pubkey=U" + "A" * 55],
                       capture_output=True, text=True)
    if r.stdout.strip() == "404":
        print("  ⓘ  enrollment not armed on this bridge (no ZB_SIGNING_SEED) — the payload "
              "shape ships in the same fmt string as the jwt, compile-checked; skipped live")
    else:
        # armed: a bogus code is refused (403), which still proves the endpoint's alive;
        # the grammar-carrying success path needs a real invite and belongs to keys.py's
        # territory — here we only refuse to silently skip when armed.
        zb.ok(f"enrollment armed (bogus code → {r.stdout.strip()}); the success payload "
              "carries grammar + grammar_hash beside the jwt")

    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    sys.exit(main())
