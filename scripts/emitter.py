#!/usr/bin/env python3
"""A visible heartbeat for the demo: +1 row per second on the read-only table.

A DEMO, not a test: nothing is asserted, the exit code is always 0, and the rows are
left behind on purpose. It lives beside the scenarios only because it shares `zb.py`.

Writes **directly to PostgreSQL**, not through the bridge — which is the point. `users` is
outbound-only (no `bridge_writer` grant), so this is the application writing to its own
database exactly as it always did, while every connected consumer watches the change arrive
without having asked for anything.

Each cycle is four events, chosen so the browser shows all three verbs and the count still
climbs by one:

    INSERT, INSERT   → two green flashes
    UPDATE the last  → one orange
    DELETE one       → one red, net +1 row

⚠️ `users` declares no `tombstone_col` in `zebridge_catalogue`, so its deletes are **physical** and
arrive as `cdc.users.delete`. On a tombstoned table the same DELETE would arrive as an
UPDATE setting the tombstone (§7.5) — which is why the web client reads the tombstone to
decide the verb, rather than trusting the operation.

Usage:  python scripts/emitter.py [seconds]

  python scripts/emitter.py          # 10 seconds
  python scripts/emitter.py 60       # a minute
"""

import sys
import time
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent / "scenarios"))
import zb  # noqa: E402

SECONDS = int(sys.argv[1]) if len(sys.argv) > 1 else 10


def main() -> int:
    before = zb.psql("SELECT count(*) FROM public.users").strip()
    print(f"users has {before} rows; emitting for {SECONDS}s (net +1/second)\n")

    for i in range(SECONDS):
        stamp = f"emit-{i}-{int(time.time())}"
        # Two inserts…
        zb.psql(
            "INSERT INTO public.users (name, inserted_at, updated_at) VALUES "
            f"('{stamp}-a', now(), now()), ('{stamp}-b', now(), now())",
            quiet=True,
        )
        # …an update of the most recent…
        zb.psql(
            "UPDATE public.users SET name = name || ' (touched)', updated_at = now() "
            "WHERE id = (SELECT max(id) FROM public.users)",
            quiet=True,
        )
        # …and a delete, leaving one behind.
        zb.psql(
            "DELETE FROM public.users WHERE id = (SELECT max(id) FROM public.users)",
            quiet=True,
        )
        now = zb.psql("SELECT count(*) FROM public.users").strip()
        print(f"  {i + 1:>3}/{SECONDS}  INS INS UP DEL   users={now}")
        time.sleep(1)

    after = zb.psql("SELECT count(*) FROM public.users").strip()
    print(f"\ndone: {before} → {after} rows")
    print("⚠️  these rows are left behind on purpose — the demo is more legible when the")
    print("   count does not reset. Clean up with:")
    print("     DELETE FROM public.users WHERE name LIKE 'emit-%';")
    return 0


sys.exit(main())
