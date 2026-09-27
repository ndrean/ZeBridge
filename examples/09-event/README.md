# 09 — sensor events: a time series on a DuckDB replica

Simulated sensors write a reading every 20 ms. The readings go into PostgreSQL through
the bridge, come back out as CDC into a DuckDB replica, and a service answers time-series
questions from that replica over NATS: freshness, moving averages, per-minute
aggregates, alarms. PostgreSQL stores the rows and never runs a query.

It follows TimescaleDB's
[events-uuidv7](https://github.com/timescale/timescaledb/tree/main/docs/getting-started/events-uuidv7)
walkthrough: the key is a UUIDv7, so the key is also the time.

```txt
sensors.py ──mutation.bob.sensor_events.insert──▶ NATS ──▶ bridge ──▶ PostgreSQL
                                                                         │ CDC
ask.py ──query.globex.<name>──▶ NATS ──▶ event_service.py ◀── NATS ◀── bridge
                                          (libzb, DuckDB replica)
```

## The table

```sql
CREATE TABLE public.sensor_events (
    event_id   uuid PRIMARY KEY DEFAULT uuidv7(),  -- the reading's time, to the millisecond
    tenant_id  text NOT NULL,
    sensor_id  integer NOT NULL,
    kind       text NOT NULL,                      -- temperature | humidity | pressure
    value      double precision NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(), -- the version column
    deleted_at timestamptz                         -- the tombstone
);
```

A UUIDv7 starts with its millisecond. Both PostgreSQL 18 and DuckDB read it back with
`uuid_extract_timestamp(event_id)`, so there is no timestamp column. Keys arrive in time
order, which is the cheap case for every index on the way: each insert lands at the
right edge.

The table is writable from the edge, scoped to its tenant. The tombstone is there for
retention: a job that deletes old readings sets `deleted_at`, the delete travels as an
ordinary delta, and the sweeper reaps it later.

## Run it

The dev stack running (PostgreSQL, nats-server, the bridge), libzb built with DuckDB,
and the `events` credential (`scripts/native/jwt-bootstrap.sh` mints it):

```sh
cd libzb && zig build -Doptimize=ReleaseFast -Dduckdb=true && cd ..
set -a && . ./.env.admin && set +a
PY=scripts/scenarios/.venv/bin/python

$PY examples/09-event/provision.py           # create + enable; waits for the first chain
$PY examples/09-event/event_service.py &     # the replica and the answers
$PY examples/09-event/sensors.py &           # 20 sensors × 50/s, 60 s
$PY examples/09-event/ask.py watch           # freshness and a moving average, every second
```

One question at a time:

```sh
$PY examples/09-event/ask.py moving_avg '{"kind": "humidity", "window_s": 30}'
$PY examples/09-event/ask.py per_minute '{"kind": "pressure", "minutes": 5}'
$PY examples/09-event/ask.py alarms '{"kind": "temperature", "above": 30}'
```

`provision.py teardown` drops the table; the bridge prunes its chain.

## The pieces

**sensors.py** publishes mutations as `bob`, the path a device takes. Each sensor keeps
its own clock and mints the reading's UUIDv7 itself. The bridge answers every write on
`mutation_ack.bob.<msg_id>`, and the script reports the verdicts and their latency.
`--sensors` and `--period-ms` set the rate.
Each sensor reads a cosine of period 2 s (`--wave-s`) around its kind's base, plus a
slow random walk about 10% of the wave's size. A 1 s
bucket holds half a wave, so the 1 s average swings each second, and a 10 s moving
average holds five whole waves: the wave cancels out and what is left is the walk, the
slow part a moving average is there to show.

**event_service.py** is one libzb client with the DuckDB engine. It follows
`sensor_events` and answers `query.globex.*` in the queue group `events`, on the same
connection and from the same file. DuckDB allows one writer per file, so the process
that writes the replica is the one that reads it. A second instance keeps its own file
and joins the queue group:

```sh
ZB_DB=/tmp/events-2.duckdb $PY examples/09-event/event_service.py --label second
```

Its credential, `events`, is a responder: it reads globex like a client and may answer
its queries, but cannot write.

**ask.py** is `bob` again, as an asker. It holds no copy of the table and asks through
libzb's `zb_client_request`, which unpacks a large answer by itself.

## The queries

| name | parameters | answer |
| --- | --- | --- |
| `freshness` | none | rows, `sum(value)`, the newest reading's time, and its age when answered |
| `moving_avg` | `kind`, `window_s` 10, `since_s` 60, `sensor_id`, `series` | per sensor, 1 s buckets and their moving average; the latest point, or one sensor's series |
| `per_minute` | `kind`, `minutes` 10 | per minute: readings, sensors, avg, min, max, stddev |
| `alarms` | `kind`, `above` 30, `since_s` 60 | the sensors above a threshold: how many readings, the highest, the last |

The newest reading's age is the lag of the whole path: sensor, NATS, bridge, PostgreSQL,
CDC, replica. The sensors write every 20 ms, so the age also includes up to one period.

## Measured (2026-09-27)

One Mac: PostgreSQL 18, nats-server, the bridge, the service and the sensors. Two
60-second runs in a row on the same replica. "Newest age" is how old the newest reading
was when the service answered, sampled every second while the sensors wrote.

| sensors | readings/s | verdicts | ack p50 / p99 | newest age p50 / p90 | question round trip p50 |
| --- | --- | --- | --- | --- | --- |
| 20 | 1,000 | 60,000 accepted | 1.8 / 3.0 ms | 9 ms | 4.4 ms |
| 100 | 5,000 | 299,996 accepted | 3.5 / 14.1 ms | 11 / 13 ms | 6.7 ms |

After both runs PostgreSQL and the replica agreed exactly: 359,996 rows, the same
`sum(value)`. The 1 s average of one sensor swung by about a degree each second, as the
2 s wave predicts, while its 10 s moving average stayed within 0.3 of the base.

## Compared with TimescaleDB

| TimescaleDB | here |
| --- | --- |
| hypertable, chunks by time | one table; the UUIDv7 key orders it by time |
| `time_bucket` | DuckDB's `time_bucket` |
| continuous aggregate | `per_minute`, recomputed on each question |
| columnstore compression | DuckDB's own columnar storage |
| retention policy | a job that sets `deleted_at`; the sweeper reaps |
| queries on the primary | queries on a replica; PostgreSQL only stores |
