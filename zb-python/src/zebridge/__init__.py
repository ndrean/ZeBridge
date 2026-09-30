"""zebridge — a ZeBridge client for Python: a local replica that follows PostgreSQL through
the bridge, and writes back through it.

    from zebridge import ZeBridge

    with ZeBridge(bridge_url="https://zb.example.com", invite=code, tables=["orders"],
                  on_change=lambda r: print("changed", r["changed_tables"])) as zb:
        rows = zb.query("SELECT * FROM orders WHERE status = ?", "open")
        zb.mutate("orders", "UPDATE", {"id": 7}, {"status": "done"})

The options are the ones every ZeBridge client reads (CLIENTS.md), camelCase or snake_case
alike. The first run enrolls with `invite` and keeps the identity next to the replica;
later runs need only `bridge_url` (or nothing but `identity_path`); the JWT renews itself.

libzb drives a client from ONE thread; this class owns that thread. Every method may be
called from any thread and blocks until the worker has run it (at most one poll, 250 ms
by default). Between calls the worker polls, and `on_change` hears every poll that
applied rows, settled writes or brought requests — on the worker thread.
"""
from __future__ import annotations

import queue
import re
import threading
from concurrent.futures import Future
from typing import Any, Callable

from . import _native as n

__all__ = ["ZeBridge", "ZeBridgeError", "ZB_ABI", "create_user", "creds_file_text", "grammar_hash"]

#: libzb's C ABI version this package was written for; libzb/python/abi_check.py checks it.
ZB_ABI = 3


class ZeBridgeError(RuntimeError):
    """A call libzb refused, with libzb's own words."""


def _camel(key: str) -> str:
    return re.sub(r"_([a-z])", lambda m: m.group(1).upper(), key)


def _check(r: dict) -> dict:
    if "error" in r:
        detail = r.get("detail")
        raise ZeBridgeError(f"{r['error']}: {detail}" if detail else str(r["error"]))
    return r


def create_user() -> dict:
    """A fresh nkey pair for enrollment: {"publicKey", "seed"}. Keep the seed secret."""
    return _check(n.take(n.lib.zb_create_user()))


def creds_file_text(jwt: str, seed: str) -> str:
    p = n.lib.zb_creds_file_text(jwt.encode(), seed.encode())
    import ctypes, json
    text = ctypes.string_at(p).decode()
    n.lib.zb_free(p)
    if text.startswith("{"):
        _check(json.loads(text))
    return text


def grammar_hash() -> str:
    import ctypes
    p = n.lib.zb_grammar_hash()
    try:
        return ctypes.string_at(p).decode()
    finally:
        n.lib.zb_free(p)


class ZeBridge:
    """One client: the replica, its NATS connection, its outbox — and the thread they need."""

    def __init__(self, options: dict[str, Any] | None = None, *, on_change: Callable[[dict], None] | None = None,
                 on_error: Callable[[ZeBridgeError], None] | None = None, poll_ms: int = 250, **kw: Any):
        if n.lib.zb_abi_version() != ZB_ABI:
            raise ZeBridgeError(f"libzb speaks ABI {n.lib.zb_abi_version()}, this package {ZB_ABI}: update one of them")
        opts = {_camel(k): v for k, v in {**(options or {}), **kw}.items() if v is not None}
        self._on_change, self._on_error, self._poll_ms = on_change, on_error, poll_ms
        self._tasks: queue.SimpleQueue = queue.SimpleQueue()
        self._running = True
        self._handle = 0
        ready: Future = Future()
        self._worker = threading.Thread(target=self._run, args=(opts, ready), name=f"zebridge-{opts.get('clientId', 'client')}", daemon=True)
        self._worker.start()
        synced = ready.result()  # raises the connect error, with libzb's words
        self.tenants: list[str] = synced.get("tenants") or []

    # ─── the worker ─────────────────────────────────────────────────────────────

    def _run(self, opts: dict, ready: Future) -> None:
        # Connect AND sync here: the handle belongs to this thread from the start, and
        # zb_last_error, being per thread, is read where the failure happened.
        import json
        h = n.lib.zb_client_connect(json.dumps(opts).encode())
        if not h:
            ready.set_exception(ZeBridgeError(n.last_error() or "zb_client_connect failed"))
            return
        try:
            synced = _check(n.take(n.lib.zb_client_sync(h)))
        except ZeBridgeError as e:
            n.lib.zb_client_close(h)
            ready.set_exception(e)
            return
        self._handle = h
        ready.set_result(synced)
        # Two paces. Idle: one poll of `poll_ms` at a time, as cheap as it gets (each poll
        # is a pull request per stream). Busy — a call served in the last BUSY_S: the
        # thread waits on the call queue instead, so a caller in a loop is served at once
        # rather than after the poll in flight (a writer was capped near 4 calls/s at
        # 250 ms); it still polls every `poll_ms`, briefly, so CDC and verdicts flow.
        import time
        busy_s, busy_poll_ms = 0.05, 5
        last_call = last_poll = 0.0
        pending: list = []
        while self._running:
            while True:
                if pending:
                    fn, fut = pending.pop()
                else:
                    try:
                        fn, fut = self._tasks.get_nowait()
                    except queue.Empty:
                        break
                last_call = time.monotonic()
                try:
                    fut.set_result(fn())
                except BaseException as e:  # handed to the caller's thread
                    fut.set_exception(e)
            if not self._running:
                break
            now = time.monotonic()
            busy = now - last_call < busy_s
            poll_due = now - last_poll >= self._poll_ms / 1000
            if busy and not poll_due:
                try:
                    pending.append(self._tasks.get(timeout=min(busy_s, self._poll_ms / 1000 - (now - last_poll))))
                except queue.Empty:
                    pass
                continue
            try:
                last_poll = time.monotonic()
                r = _check(n.take(n.lib.zb_client_poll(h, busy_poll_ms if busy else self._poll_ms)))
                if (r.get("applied") or r.get("settled") or r.get("requests")) and self._on_change:
                    self._on_change(r)
            except ZeBridgeError as e:
                if self._on_error:
                    self._on_error(e)
                if n.lib.zb_client_revoked(h) == 1:
                    self._running = False  # for good: every later call answers Revoked
                else:
                    threading.Event().wait(1.0)

    def _call(self, fn: Callable[[], Any]) -> Any:
        if threading.current_thread() is self._worker:
            return fn()  # an on_change callback calling back in
        if not self._worker.is_alive():
            raise ZeBridgeError("the client is closed")
        fut: Future = Future()
        self._tasks.put((fn, fut))
        return fut.result()

    # ─── the client ─────────────────────────────────────────────────────────────

    def query(self, sql: str, *params: Any) -> list[dict[str, Any]]:
        """Read the replica: SQL with `?` placeholders; rows as dicts. Writes are refused."""
        def go():
            r = _check(n.take(n.lib.zb_client_query(self._handle, sql.encode(), n.enc(list(params)))))
            cols = r["columns"]
            return [dict(zip(cols, row)) for row in r["rows"]]
        return self._call(go)

    def mutate(self, table: str, op: str, key: dict, values: dict | None = None, version: str | None = None) -> dict:
        """Write a row (op: INSERT, UPDATE or DELETE, any case): applied locally at once,
        sent to PostgreSQL, settled by its verdict (`on_change` sees `settled`)."""
        def go():
            if version is None:
                return _check(n.take(n.lib.zb_client_mutate(self._handle, table.encode(), op.encode(), n.enc(key), n.enc(values))))
            return _check(n.take(n.lib.zb_client_mutate_at(self._handle, table.encode(), op.encode(), n.enc(key), n.enc(values), version.encode())))
        return self._call(go)

    def flush(self, wait_ms: int = 0) -> dict:
        """Send queued writes now, waiting up to `wait_ms` for their verdicts."""
        return self._call(lambda: _check(n.take(n.lib.zb_client_flush_outbox(self._handle, wait_ms))))

    def join(self, tenant: str) -> dict:
        return self._call(lambda: _check(n.take(n.lib.zb_client_join(self._handle, tenant.encode()))))

    def leave(self, tenant: str) -> dict:
        return self._call(lambda: _check(n.take(n.lib.zb_client_leave(self._handle, tenant.encode()))))

    def request(self, subject: str, payload: dict | None = None, timeout_ms: int = 5000) -> dict:
        """Ask a tenant's service (`query.<tenant>.<name>`)."""
        return self._call(lambda: _check(n.take(n.lib.zb_client_request(self._handle, subject.encode(), n.enc(payload or {}), timeout_ms))))

    def ingest(self, table: str, answer: dict, scope: dict | None = None) -> dict:
        """Store a request's answer rows in an on-demand table."""
        return self._call(lambda: _check(n.take(n.lib.zb_client_ingest(self._handle, table.encode(), n.enc(answer), n.enc(scope)))))

    def serve(self, options: dict) -> dict:
        """Answer requests as a service; they arrive in `on_change`'s `requests`."""
        return self._call(lambda: _check(n.take(n.lib.zb_client_serve(self._handle, n.enc(options)))))

    def reply(self, request_id: int, answer: dict) -> dict:
        return self._call(lambda: _check(n.take(n.lib.zb_client_reply(self._handle, request_id, n.enc(answer)))))

    @property
    def revoked(self) -> bool:
        """True once the operator revoked this principal. The rows stay; `wipe()` removes them."""
        return n.lib.zb_client_revoked(self._handle) == 1

    def wipe(self) -> None:
        """Stop, close, and delete the replica's files — the application's explicit act."""
        self._stop(n.lib.zb_client_wipe)

    def close(self) -> None:
        """Stop the loop and close the replica. Safe to call twice."""
        self._stop(n.lib.zb_client_close)

    def _stop(self, end: Callable[[int], int]) -> None:
        if not self._worker.is_alive():
            return
        def go():
            self._running = False
            end(self._handle)
        self._call(go)
        self._worker.join()

    def __enter__(self) -> "ZeBridge":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()
