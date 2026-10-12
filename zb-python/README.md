# zebridge (Python)

A ZeBridge client for Python: a local replica that follows PostgreSQL through the
bridge, and writes back through it. Pure Python over libzb's C ABI — no ctypes in your
code. Which library for which host: [DISTRIBUTION.md](https://github.com/ndrean/zebridge/blob/main/DISTRIBUTION.md).

```sh
pip install zebridge
```

The wheels carry libzb for Linux (x86_64, aarch64; glibc 2.35+) and macOS (Apple silicon,
Intel; 13+). Python 3.10 or later.

## Use

```python
from zebridge import ZeBridge

with ZeBridge(bridge_url="https://zb.example.com", invite=code, tables=["orders"],
              on_change=lambda r: print("changed:", r.get("changed_tables"))) as zb:
    rows = zb.query("SELECT * FROM orders WHERE status = ?", "open")   # list of dicts
    zb.mutate("orders", "UPDATE", {"id": 7}, {"status": "done"})
```

- **The options** are the ones every ZeBridge client reads ([CLIENTS.md](https://github.com/ndrean/zebridge/blob/main/CLIENTS.md)), in camelCase or
  snake_case (`bridge_url` is `bridgeUrl`). A dict works too: `ZeBridge({"bridgeUrl": …})`.
- **Enrollment.** The first run redeems `invite` at the bridge and keeps the identity next
  to the replica (`identity_path`, default `<db_path>.identity`, mode 0600). Later runs need
  neither the invite nor a NATS URL, and the JWT renews itself before it expires.
- **The thread.** libzb drives a client from one thread; `ZeBridge` owns it. Any thread may
  call it. An idle client polls every `poll_ms` (250 ms); while calls keep coming, the
  thread serves each one at once and still polls every `poll_ms`, so a writer in a loop runs
  at its own pace (measured: 1,700 writes/s, all settled). `on_change` hears every poll
  that applied rows, settled writes or brought requests — on the worker thread (calling
  back into `zb` from it is fine).
- **Errors** raise `ZeBridgeError` with libzb's words. `on_error` hears the loop's.
- **Revocation.** Once revoked, `zb.revoked` is true and the loop stops; `wipe()` removes the
  rows.
- **Asking a service.** `request(subject, payload)`, and `ingest` to keep the answer's rows in
  an on-demand table.

## A service

A service answers questions that clients ask with `request("query.<tenant>.<name>", …)`.
It needs a responder's creds (`bridge --mint-responder`). Write one handler per question:

```python
from zebridge import ZeBridge

with ZeBridge(nats_url="tls://nats.example.com:4222", creds_path="airports.creds",
              db_path="airports.sqlite3", tables=["airports"]) as zb:

    def by_code(q: dict, tenant: str):
        return zb.query("SELECT * FROM airports WHERE code = ?", q["code"])

    zb.serve(["_default"], {"airport_by_code": by_code}, queue="airports")
    zb.wait()   # until SIGINT or SIGTERM
```

- A handler gets the asker's payload and the tenant from the subject (never from the
  payload), and returns any JSON.
- A handler that raises answers `{"error": "<Type>: <message>"}`, so the asker never waits
  out a timeout.
- Handlers run on the client's thread, between two polls. Instances with the same `queue`
  share the questions.
- An answer too large for one message goes through the tenant's object store; the asking
  library fetches it.

Example: [examples/10-airports/airport_service.py](https://github.com/ndrean/zebridge/blob/main/examples/10-airports/airport_service.py).

## The library

`ZB_LIB` names libzb's shared library; else a copy bundled in the package
(`zebridge/lib/`), else the repository's own build (`cd libzb && zig build
-Doptimize=ReleaseFast`), else the system's: on Debian and Ubuntu, `sudo apt install
./libzb_<version>_<arch>.deb` puts it where the loader finds it. `ZB_ABI` pins the C ABI this package speaks; libzb/python/abi_check.py
checks it with the other bindings.
