# Observability — what each metric means, and what proves it moves

Two instruments were caught reporting calm during the exact failures they exist to expose (`/health` answering 200 on a bridge whose replication was dead, NOTES §10bu; `bridge_queue_usage_percent` frozen at 0% while the ring held hundreds of events behind a dead broker, §10cd).
The lesson is general: **a metric only written on the happy path goes silent during the failure it is for.** This table is the antidote — every metric the bridge exposes, what it claims, which failure moves it, and the scenario that PROVES it moves. A metric with no proving scenario is a candidate liar: treat its calm with suspicion until a scenario pins it.

Scrape `/metrics` (Prometheus exposition) on `BRIDGE_PORT`; `/status` carries the same numbers as JSON. Both are served by a thread that keeps answering through every outage below — that is deliberate, and `pg_restart.py` asserts it.

| metric | it claims | moved by | proven by |
| --- | --- | --- | --- |
| `bridge_connected` | the WAL stream is attached | any PostgreSQL outage (0 while down, 1 after self-reconnect) | `pg_restart.py` |
| `bridge_pg_reconnects_total` | PostgreSQL sessions re-established | backend kills, cluster restarts | `pg_restart.py`, `chaos.py` |
| `bridge_nats_reconnects_total` | broker SESSIONS re-established, at the transport — nats.zig's `reconnected_cb` counts the library's silent self-heals, the publisher's fresh-connection fallback adds its disjoint share (§10cn). NOT a broker-restart count: adjacent bounces merge into one down period, honestly | broker kill + return, even idle bounces | `churn.py` (metric == log ground truth), `nats_outage.py` |
| `bridge_queue_usage_percent` | events held in the ring, right now | broker gone under load: climbs as the ring fills, 0 again after the drain. Three writers: post-flush, the halt loop, the periodic tick (§10cd) | `cascade.py` |
| `bridge_wal_confirmed_lag_bytes` | WAL PostgreSQL retains that this bridge has not confirmed — THE backlog number | any outage that stops acking; collapses on recovery. Samples on the monitor's 30 s cadence | `cascade.py` |
| `bridge_wal_lag_bytes` | WAL retained from `restart_lsn` — a disk-pressure number that only moves at checkpoints. NOT a backlog gauge; a healthy bridge plateaus at a few MB | slot pressure | (definitional; see the field's comment) |
| `bridge_slot_active` | PostgreSQL shows our slot streaming | bridge down or stepped aside → 0 | `downtime.py` |
| `bridge_cdc_events_published_total` | ROW events acked by JetStream — trusted arithmetic, equals rows delivered | any write reaching CDC | `slot_contest.py` (flow-through), README burst method |
| `bridge_schema_events_published_total` | KV/schema traffic, kept OUT of the row counter | DDL, suspensions, drops | `livebirth.py`, `legacybait.py` |
| `bridge_refused_tables` | tables currently suspended or refused — the COUNT | `row_too_large` and structural refusals; falls on the live lift | `suspension_lift.py`, `legacybait.py` |
| `bridge_refused_table{table,reason}` | the NAMED refusal series (§10cg): 1 while refused, an explicit 0 after a lift in this process; the family vanishes on restart — which also cleared every ban, so absence and truth agree. psql twin: `SELECT tbl, suspended_reason FROM zebridge_catalogue WHERE suspended;` (§10cf — a bridge dump, live while the bridge lives, pruned at boot) | every refusal transition | `suspension_lift.py` |
| `bridge_refused_events_dropped_total` | events dropped for refused tables | writes hitting a suspended table | `suspension_lift.py` |
| `bridge_nats_publish_ack_seconds_total` / `bridge_nats_publishes_total` | summed publish→PubAck wall time / count (mean = quotient) | load; the ack changes of §10cb ride on these being honest | README burst method |
| `bridge_gc_total_reaped_total` / `bridge_gc_last_sweep_timestamp_seconds` | sweeper activity, read off the watermark row's own CDC event — the sweeper stays a pure PG client | sweeper passes; survives a PostgreSQL restart under the daemon | `sweeper_restart.py`, `reaps.py` |
| `bridge_last_ack_lsn` | the position THIS bridge confirmed — never the server's WAL head (the conflation that once silently skipped a downtime's changes) | every ack; jumps with the §10cb drained fast-ack | `txn_kill.py`, `pg_restart.py` |
| `bridge_uptime_seconds`, `bridge_cpu_seconds_total`, `bridge_max_rss_bytes` | process vitals | always | (trivially live) |
| `/health` → 200 | the PROCESS is up **and will exit when replication dies** — a FATAL sets the global stop since §10bu, so a lying 200 cannot outlive the failure | fatal paths | `stream_full.py` |

Grafana notes, learned the hard way:
- **Alert on `bridge_connected == 0` and on `bridge_wal_confirmed_lag_bytes` growth**, not on `/health` alone — health says "process alive", and a process can be alive and parked (that is its correct behaviour during a broker outage).
- `bridge_queue_usage_percent` and `bridge_wal_confirmed_lag_bytes` tick on their own cadences (periodic tick; 30 s WAL monitor). A panel averaging over <1 min windows will alias; 1–2 min windows read true.
- The pair to watch during a broker outage is queue% (the dam filling) THEN confirmed lag (PostgreSQL taking the overflow): §10cd's cascade in two panels.
