# zebridge (Python)

A ZeBridge client for Python: a local replica that follows PostgreSQL through the
bridge, and writes back through it. Pure Python over libzb's C ABI — no ctypes in your
code. Which library for which host: [DISTRIBUTION.md](../DISTRIBUTION.md).

## Use

```python
from zebridge import ZeBridge

with ZeBridge(bridge_url="https://zb.example.com", invite=code, tables=["orders"],
              on_change=lambda r: print("changed:", r.get("changed_tables"))) as zb:
    rows = zb.query("SELECT * FROM orders WHERE status = ?", "open")   # list of dicts
    zb.mutate("orders", "UPDATE", {"id": 7}, {"status": "done"})
```

- **The options** are the ones every ZeBridge client reads (CLIENTS.md), in camelCase or
  snake_case (`bridge_url` is `bridgeUrl`). A dict works too: `ZeBridge({"bridgeUrl": …})`.
- **Enrollment.** The first run redeems `invite` at the bridge and keeps the identity next
  to the replica (`identity_path`, default `<db_path>.identity`, mode 0600). Later runs need
  neither the invite nor a NATS URL, and the JWT renews itself before it expires.
- **The thread.** libzb drives a client from one thread; `ZeBridge` owns it. Any thread may
  call it; a call waits at most one poll (`poll_ms`, 250 ms). Between calls the worker
  polls, and `on_change` hears every poll that applied rows, settled writes or brought
  requests — on the worker thread (calling back into `zb` from it is fine).
- **Errors** raise `ZeBridgeError` with libzb's words. `on_error` hears the loop's.
- **Revocation.** Once revoked, `zb.revoked` is true and the loop stops; `wipe()` removes the
  rows.
- **Services.** `serve({...})`, then answer `on_change`'s `requests` with `reply(id, answer)`;
  `request` and `ingest` for on-demand tables.

## The library

`ZB_LIB` names libzb's shared library; else a copy bundled in the package
(`zebridge/lib/`), else the repository's own build (`cd libzb && zig build
-Doptimize=ReleaseFast`). `ZB_ABI` pins the C ABI this package speaks; libzb/python/abi_check.py
checks it with the other bindings.
