#!/usr/bin/env python3
"""TEST_SCENARIOS I — burst throughput, and the failure signature to watch for.

**Not a trickle.** Every load test before 2026-08-15 paced small transactions, and every
one passed while a quadratic bug in the WAL reader capped bursts at ~1.2k msg/s. The bug
was invisible at a trickle by construction: its cost was proportional to how many unread
bytes sat in libpq's buffer, and a bridge that keeps up never accumulates any.

Watch the bridge's LOOP line while this drains. Healthy is `idle` in the thousands with
`recv_ms` in the tens. The failure signature is its exact inverse:

    recv_ms approaching the 15000 ms interval, idle=0, cpu≈100%

— a reader that cannot drain its socket. Every *other* signal looks fine in that state:
cdc_events still climbs, queue_usage_percent still reads 0% (it watches the flush side,
which was never the problem), and nothing errors.

Usage:  python scripts/scenarios/burst.py [table] [statements] [rows_per_statement]
"""

import subprocess
import sys
import time

import zb


def main():
    table = sys.argv[1] if len(sys.argv) > 1 else "users"
    # ⚠️ This benchmark LEAVES its 2M rows in `users` on purpose (the measurement is
    # the ingest, and a CDC-visible delete storm would poison the next measurement).
    # NEVER run it as part of a shared-environment battery: clean up afterwards with
    #   DELETE FROM users WHERE name LIKE 'User-%';
    #   SELECT pg_replication_slot_advance('<slot>', pg_current_wal_lsn());  -- skip the deletes
    # then purge CDC_PUBLIC and the users generation chain (measured 2026-08-27:
    # the leftover rows turned every fresh seed into a 2M-row chain apply).
    statements = int(sys.argv[2]) if len(sys.argv) > 2 else 2000
    per = int(sys.argv[3]) if len(sys.argv) > 3 else 1000

    cols = zb.psql(
        f"SELECT attname FROM pg_attribute WHERE attrelid='public.{table}'::regclass "
        "AND attnum>0 AND NOT attisdropped ORDER BY attnum"
    ).splitlines()
    if "name" not in cols or "email" not in cols:
        sys.exit(f"burst.py generates rows for a `users`-shaped table; '{table}' has {cols}")

    # Separate statements, each its own transaction — the shape that builds a backlog.
    # One giant transaction would not: the WAL only becomes visible at COMMIT.
    sql = "\n".join(
        f"INSERT INTO public.{table} (name,email,inserted_at,updated_at) "
        f"SELECT 'User-{i}-'||i2, 'u{i}-'||i2||'@example.com', now(), now() "
        f"FROM generate_series(1,{per}) i2;"
        for i in range(statements)
    )

    total = statements * per
    print(f"\ninserting {total:,} rows as {statements} transactions of {per}")
    started = time.time()
    res = subprocess.run(zb.PSQL.split() + ["-q"], input=sql, capture_output=True, text=True)
    if res.returncode != 0:
        sys.exit(res.stderr.strip())
    print(f"  PostgreSQL absorbed them in {time.time() - started:.1f}s")

    print(
        "\nNow watch the bridge (it logs every 15s):\n"
        "  LOOP  iters=… idle=… recv_ms=… proc_ms=… cpu=…\n"
        "  METRICS … cdc_events=…\n"
        f"\n  healthy : cdc_events reaches {total:,}; idle stays high once drained\n"
        "  broken  : recv_ms → 15000, idle=0, cpu≈100%\n"
        "\nAnd the backlog — the number that answers 'is the bridge behind?':\n"
        "  curl -s localhost:9090/metrics | grep bridge_wal_confirmed_lag_bytes"
    )
    return 0


sys.exit(main())
