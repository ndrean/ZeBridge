#!/usr/bin/env python3
"""zb-python's `serve`: a service is handlers, the library does the rest.

`zb.serve(tenants, handlers, queue)` subscribes `query.<tenant>.<name>` for each handler
name, calls the handler on libzb's thread with the asker's payload and the tenant the
question arrived on, and replies with what it returns. The same verb as zb-client-ts.

  A  a handler answers from the replica, and the answer equals PostgreSQL;
  B  the handler gets the tenant from the subject, not from the payload;
  C  a handler that raises answers {"error": "<Type>: <message>"}, not a timeout;
  D  a handler may return any JSON (a list here), not only an object;
  E  on_change still hears the poll that applies a new row, and never sees an answered
     request; the new row is in the next answer.

Usage:  scripts/scenarios/.venv/bin/python scripts/scenarios/py_serve.py
"""
import asyncio, json, pathlib, sys, tempfile

import nats
import zb

REPO = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "zb-python" / "src"))
from zebridge import ZeBridge  # noqa: E402

TABLE = "memo"
QUEUE = "pyserveprobe"
TENANT = "_default"
PROBE = "py_serve probe"


async def ask(nc, name: str, payload: dict, timeout=5.0):
    r = await nc.request(f"query.{TENANT}.{name}", json.dumps(payload).encode(), timeout=timeout)
    return json.loads(r.data)


def count(zbc):
    return lambda q, tenant: {"count": zbc[0].query(f"SELECT count(*) AS n FROM {TABLE}")[0]["n"]}


async def main() -> int:
    zb.psql(f"DELETE FROM public.{TABLE} WHERE txt = '{PROBE}'", quiet=True)  # a run cut short
    live = int(zb.psql(f"SELECT count(*) FROM public.{TABLE}", quiet=True).strip() or "0")
    polls: list = []
    zbc: list = []

    def boom(q, tenant):
        raise ValueError(f"no such thing: {q.get('what')}")

    handlers = {
        "pycount": count(zbc),
        "pytenant": lambda q, tenant: {"tenant": tenant},
        "pyboom": boom,
        "pylist": lambda q, tenant: [1, 2, q.get("n")],
    }
    ok = True
    with tempfile.TemporaryDirectory() as d, \
            ZeBridge(nats_url=zb.nats_server(), **zb.tls_opts(), creds_path=zb.creds_for("pois"), principal="pois",
                     db_path=f"{d}/r.sqlite3", tables=[TABLE], client_id="py-serve", heartbeat_ms=0,
                     on_change=polls.append) as z:
        zbc.append(z)
        n = z.serve([TENANT], handlers, queue=QUEUE)
        print(f"serving {n} subject(s)")
        nc = await nats.connect(zb.nats_server(), user_credentials=zb.creds_for("omar"), inbox_prefix=b"_INBOX.omar")
        try:
            a = await ask(nc, "pycount", {})
            good = a.get("count") == live
            ok &= good
            print(f"{'✓' if good else '✗'} A answer {a} = PostgreSQL {live}")

            a = await ask(nc, "pytenant", {"tenant": "someone-else"})
            good = a == {"tenant": TENANT}
            ok &= good
            print(f"{'✓' if good else '✗'} B tenant from the subject: {a}")

            a = await ask(nc, "pyboom", {"what": "x"})
            good = a == {"error": "ValueError: no such thing: x"}
            ok &= good
            print(f"{'✓' if good else '✗'} C a raising handler answers an error: {a}")

            a = await ask(nc, "pylist", {"n": 3})
            good = a == [1, 2, 3]
            ok &= good
            print(f"{'✓' if good else '✗'} D any JSON: {a}")

            zb.psql(f"INSERT INTO public.{TABLE} (txt) VALUES ('{PROBE}')", quiet=True)
            try:
                for _ in range(50):
                    if any(p.get("applied") for p in polls):
                        break
                    await asyncio.sleep(0.1)
                a = await ask(nc, "pycount", {})
                applied = sum(p.get("applied") or 0 for p in polls)
                leaked = [p for p in polls if p.get("requests")]
                good = applied >= 1 and not leaked and a.get("count") == live + 1
                ok &= good
                print(f"{'✓' if good else '✗'} E on_change: {applied} row(s) applied over {len(polls)} poll(s), "
                      f"{len(leaked)} with requests; the next answer counts {a.get('count')} (PostgreSQL {live + 1})")
            finally:
                zb.psql(f"DELETE FROM public.{TABLE} WHERE txt = '{PROBE}'", quiet=True)
        finally:
            await nc.close()

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
