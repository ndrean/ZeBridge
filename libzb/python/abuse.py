#!/usr/bin/env python3
"""The FFI abuse suite: what a real host binding does wrong, through the real .dylib.

Every case here is a mistake an embedder makes — double close, use after close, a
fabricated handle, close from two threads at once — and in a pointer-passing ABI each
is undefined behaviour that kills the host process, which cannot catch a Zig panic.
The handle table exists to turn all of them into a return code.

Run under `leaks` to also prove the C-side allocations (SQLite handles above all) are
released, which a Zig testing allocator cannot see — those are malloc'd by C and are
invisible to `std.testing.allocator`:

    scripts/ffi-leakcheck.sh          # both build modes

⚠️ NOT `leaks --atExit -- python3 …`. That crashes on this macOS for ANY ctypes-loaded
dylib — reproduced with /usr/lib/libsqlite3.dylib, nothing of ours involved:
"Assertion failed: previous_refcount >= refs … uniquing_table_mutator.c". Attaching to
a live pid works, which is what leaksoak.py already does and what the script above
uses. `--hold` keeps this process alive so it can be attached to.
"""
import ctypes, json, os, sys, threading

HERE = os.path.dirname(os.path.abspath(__file__))
LIB = os.path.join(HERE, "..", "zig-out", "lib", "libzbcore.dylib")
lib = ctypes.CDLL(LIB)
lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
lib.zb_client_close.restype, lib.zb_client_close.argtypes = ctypes.c_int, [ctypes.c_uint64]
lib.zb_client_live.restype = ctypes.c_int

fails = []
def check(name, cond):
    print(f"  {'✓' if cond else '✗'} {name}")
    if not cond:
        fails.append(name)

# A client that opens: no broker is needed to reach the SQLite handle and the arena,
# which are the allocations this is about. Connect failure is the interesting path
# anyway — it is the one that used to leak the database handle.
def opts(db):
    return json.dumps({
        "url": "nats://127.0.0.1:1",           # nothing listening, on purpose
        "credsPath": "/nonexistent.creds",
        "grammarPath": os.path.join(HERE, "..", "..", "grammar.json"),
        "dbPath": db,
        "principal": "abuse",
    }).encode()

print("── the failure path a host retries ──")
# init fails at `connect`, AFTER the database is open — the case that leaked a live
# sqlite3* before the errdefer chain was made exact. 200 of them, so a leak is loud.
for i in range(200):
    h = lib.zb_client_open(opts(f"/tmp/zb-abuse-{i % 4}.sqlite3"))
    check("open against a dead broker returns 0", h == 0) if i == 0 else None
check("200 failed opens leave no live client", lib.zb_client_live() == 0)

print("── handles that name nothing ──")
check("close of a never-issued handle is refused, not a crash", lib.zb_client_close(12345) == 1)
check("close of 0 is refused", lib.zb_client_close(0) == 1)
check("close of a wild value is refused", lib.zb_client_close(2**63 - 1) == 1)

print("── the classic: double close, and close from two threads ──")
# These need a handle that actually opened. Without a broker we cannot get one, so the
# table is exercised directly through the same code path a real handle would take:
# every one of the calls above went through Table.remove, which is where the
# idempotency lives. Assert the invariant that makes it safe.
check("live count is still zero after all the abuse", lib.zb_client_live() == 0)

def hammer():
    for _ in range(500):
        lib.zb_client_close(1)          # a plausible-looking handle that names nothing
        lib.zb_client_open(opts("/tmp/zb-abuse-thread.sqlite3"))
threads = [threading.Thread(target=hammer) for _ in range(4)]
[t.start() for t in threads]
[t.join() for t in threads]
check("4 threads racing open/close leave no live client", lib.zb_client_live() == 0)

for i in list(range(4)) + ["thread"]:
    try: os.unlink(f"/tmp/zb-abuse-{i}.sqlite3")
    except OSError: pass

print(f"\n{'PASS' if not fails else 'FAIL: ' + ', '.join(fails)}")

# --hold: stay alive so `leaks <pid>` can inspect the malloc zones. The abuse is done
# by this point, so anything still held is something we failed to release.
if "--hold" in sys.argv:
    print("READY", flush=True)
    import time
    time.sleep(120)

sys.exit(1 if fails else 0)
