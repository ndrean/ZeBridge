# 09 — sensor events: a time series on a DuckDB replica

Simulated sensors send a reading every 20 ms. The readings are stored in PostgreSQL, copied to a DuckDB file, and a small service answers questions about them: how fresh is the data, what is the moving average, what happened each minute. PostgreSQL stores the rows and never runs one of those queries.

The example has four parts, and this page walks through them in order:

1. **The emitter** (`sensors.py`): a plain NATS connection that publishes each reading as a *mutation*, a request to write a row.
2. **The backend** (PostgreSQL + the bridge + NATS): writes the row, answers the sender with a verdict, and publishes the row's change (CDC) to a stream.
3. **The event service** (`event_service.py`): Python driving `libzbcore.dylib`, the libzb library. It keeps a DuckDB copy of the table and answers questions, all in one thread.
4. **MQTT devices** (`mqtt_sensors.py`, `mqtt_gateway.py`): the same readings from plain MQTT clients. nats-server speaks MQTT itself, and a small gateway turns each reading into a mutation.

`ask.py` asks the questions, and [`web/`](web/README.md) draws them in a browser.

```mermaid
flowchart TD
    BE(Backend) --> |"<br>cdc.{tenant}.{table}.insert<br>"| ES("event_service.py<br>Python in-process Duckdb<br>hardcoded query_names")
    S(sensors.py<br>emitters) --> |"<br>mutation.{principal}.{table}.insert"<br> | BE
    BE -->|" <br>mutation_ack.{principal}.msg_id<br>"| S
    ask.py --> |"query.{tenant}.{query_name}"| ES
    ES --> |answer<br>inbox| ask.py
    M(mqtt_sensors.py<br>MQTT devices) --> |"<br>MQTT :1883<br>sensors/{tenant}/{sensor}/{kind}<br>"| GW(mqtt_gateway.py<br>readings to mutations)
    GW --> |"<br>mutation.mqttgw.{table}.insert<br>"| BE

```

The data model follows TimescaleDB's [events-uuidv7](https://github.com/timescale/timescaledb/tree/main/docs/getting-started/events-uuidv7) walkthrough: each row's key is a UUIDv7, and a UUIDv7 carries the time it was made.

## Run it

You need the dev stack running (PostgreSQL, nats-server, the bridge) and libzb built with DuckDB. The `events` credential comes from `scripts/native/jwt-bootstrap.sh`.

```sh
cd libzb && zig build -Doptimize=ReleaseFast && cd ..   # the DuckDB engine needs DuckDB installed
set -a && . ./.env.admin && set +a
PY=scripts/scenarios/.venv/bin/python

$PY examples/09-event/provision.py           # create the table; waits until the bridge has it
$PY examples/09-event/event_service.py &     # part 3: the DuckDB copy and the answers
$PY examples/09-event/sensors.py &           # part 1: 20 sensors × 50 readings/s, for 60 s
$PY examples/09-event/ask.py watch           # ask every second; prints the lag and a moving average
```

The same readings from MQTT devices instead of `sensors.py` (part 4; the dev nats-server listens for MQTT on port 1883):

```sh
$PY examples/09-event/mqtt_gateway.py &      # MQTT readings → mutations, as the principal mqttgw
$PY examples/09-event/mqtt_sensors.py        # 20 MQTT devices × 50 readings/s, for 60 s
```

One question at a time:

```sh
$PY examples/09-event/ask.py moving_avg '{"kind": "humidity", "window_s": 30}'
$PY examples/09-event/ask.py per_minute '{"kind": "pressure", "minutes": 5}'
$PY examples/09-event/ask.py alarms '{"kind": "temperature", "above": 30}'
```

`provision.py teardown` drops the table. It keeps the service's and the gateway's tenant mappings: deleting a principal's last mapping revokes it, and the bridge's retained ban would lock out the next run's service.

To see the curves, [`web/`](web/README.md) draws one sensor live in a browser: the 2 s wave in 100 ms buckets and its moving average.

## Part 1 — the emitter: publish a mutation

A real sensor would sit behind MQTT or an HTTP gateway. To keep the example simple, `sensors.py` opens its own NATS connection with `nats-py` and publishes directly. It logs in as `bob`, a client of the tenant `globex`.

A mutation is a MessagePack message on a subject that says **who** writes, **which table** and **which operation**:

```txt
mutation.<principal>.<table>.<operation>      →   mutation.bob.sensor_events.insert
```

NATS checks the subject against bob's credential before accepting it, so bob cannot write as anyone else. The body says **which row** and **what values**:

```python
eid = uuid.uuid7()                      # the reading's id AND its time (Python 3.14)
row = {"event_id": str(eid), "tenant_id": "globex", "sensor_id": 7,
       "kind": "temperature", "value": 21.43, "updated_at": "2026-09-27T09:40:01.123456Z"}

await js.publish(
    "mutation.bob.sensor_events.insert",
    msgpack.packb({"key": {"event_id": row["event_id"]},   # the primary key
                   "data": row,                            # the full row
                   "version": row["updated_at"],           # the version column's value
                   "client_id": "sensor-7"}),              # breaks ties between writers
    headers={"Nats-Msg-Id": eid.hex},                      # JetStream drops duplicates
)
```

`js.publish` is a JetStream publish. It returns once the message is safely stored in the `MUTATIONS` stream, before PostgreSQL has seen it. The bridge's verdict comes later, on a subject the sensor listens to:

```python
await nc.subscribe("mutation_ack.bob.*", cb=on_verdict)   # {"status": "accepted", ...}
```

Each simulated sensor keeps its own clock and sends every 20 ms. Its value is a cosine with a 2 s period around a base (21 °C, 45 %, 1013 hPa), plus a slow random walk about 10% of the wave's size, a little noise, and a rare spike. The shape makes the queries easy to check by eye: a 1 s average holds half a wave and swings each second, while a 10 s moving average holds five whole waves and shows only the slow walk.

## Part 2 — the backend: PostgreSQL, the bridge and NATS

This part runs on its own; the example only creates the table. For each mutation:

1. The bridge takes the message from the `MUTATIONS` stream.
2. It reads the principal from the subject and checks the table and columns against its catalogue, the list of tables it serves.
3. It writes the row in PostgreSQL with `INSERT … ON CONFLICT`, using its own writer role but on bob's behalf: it passes bob's name to the transaction, and row-level security limits the write to `globex` rows.
4. It answers on `mutation_ack.bob.<msg_id>`: `accepted`, `stale`, `rejected`, and so on.
5. PostgreSQL's write-ahead log (WAL) records the new row. The bridge reads it through a replication slot and publishes the change on `cdc.globex.sensor_events.insert` in the stream `CDC_globex`. Several rows can share one message (the `.batch` subject).

**tenant: globex**

```mermaid
flowchart LR
    N[(NATS)] -->|"MUTATIONS stream<br>mutation.{principal}.{table}.insert"| B[bridge]
    B -->|"INSERT … ON CONFLICT<br>RLS: tenant_id = f(principal)"| P[("Postgres<br>tbl: sensor_events<br>pub: my_pub")]
    P -.-> WAL("WAL<br>slot: my_slot")
    WAL -->|CDC| B
    B -->|"MUTATIONS stream<br>mutation_ack.{principal}.{msg_id}"| N
    B -->|"CDC_{tenant} stream<br>cdc.{tenant}.{table}.{op}"| N
```

The bridge also cuts a **chain** every few minutes: compressed snapshots and deltas of the table in a NATS object store. A new replica loads the chain first, then follows the stream from where the chain ends.

The table, created by `provision.py`:

```sql
CREATE TABLE public.sensor_events (
    event_id   uuid PRIMARY KEY DEFAULT uuidv7(),  -- the reading's time, to the millisecond
    tenant_id  text NOT NULL,                      -- which tenant owns the row
    sensor_id  integer NOT NULL,
    kind       text NOT NULL,                      -- temperature | humidity | pressure
    value      double precision NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(), -- the version: the newer write wins
    deleted_at timestamptz                         -- set instead of deleting the row
);
SELECT zebridge_enable('public.sensor_events',
    tenant_col => 'tenant_id',
    writable => true,
    version_col => 'updated_at',
    tombstone_col => 'deleted_at',
    publication => 'my_pub',
    dry_run => false
);
```

`zebridge_enable` does the setup: grants, row-level security, the index the bridge needs, the catalogue row, and adding the table to the publication. `provision.py` also maps the service's principal `events` to `globex`, so libzb knows which tenant it follows.

There is no timestamp column. PostgreSQL 18 and DuckDB both read the time back with `uuid_extract_timestamp(event_id)`. Keys arrive in time order, so every insert lands at the end of each index, the cheapest place.

`deleted_at` is the tombstone. A retention job would set it on old readings. The delete then travels like any other change, and the bridge's sweeper removes the row later.

## Part 3 — the event service: libzb from Python

```mermaid
flowchart LR
    subgraph ES [Python event service, one thread]
        PY[Python loop] -->|"sync, serve, poll,<br>query, reply"| libzb
        libzb -->|"sync: seed from the chain<br>poll: apply a CDC batch"| DDB[(DuckDB)]
        DDB -->|query: rows| libzb
    end
    NATS -->|"chain, CDC batches,<br>questions"| libzb
    libzb -->|"connect, serve,<br>reply to the asker's inbox"| NATS
```

`event_service.py` loads `libzbcore.dylib` with `ctypes`.
Every libzb call takes a handle and JSON text, and returns JSON text that the caller frees with `zb_free`:

```python
lib = ctypes.CDLL("libzb/zig-out/lib/libzbcore.dylib")

def take(ptr):                       # every libzb answer: read the JSON, then free it
    try:
        return json.loads(ctypes.string_at(ptr).decode())
    finally:
        lib.zb_free(ptr)
```

The service uses six calls:

| call | what it does |
| --- | --- |
| `zb_client_connect(opts)` | opens the local database and the NATS connection; returns a handle |
| `zb_client_sync(h)` | loads the chain into DuckDB, then catches up on the stream |
| `zb_client_serve(h, opts)` | subscribes to `query.globex.<name>` for each query it answers |
| `zb_client_poll(h, wait_ms)` | applies the next batch of changes; returns any questions that arrived |
| `zb_client_query(h, sql, params)` | reads the DuckDB copy with SQL |
| `zb_client_reply(h, id, answer)` | sends one answer back to whoever asked |

Here is the service with its bookkeeping removed. It is the same code as in `event_service.py`, where `card` wraps the calls above:

```python
# 1. Open: a DuckDB file that follows one table, as the principal `events`.
card = Card(lib, {"natsUrl": "nats://127.0.0.1:4222",
                  "credsPath": "scripts/native/creds/events.creds", "principal": "events",
                  "dbPath": "/tmp/events-service.duckdb", "engine": "duckdb",
                  "tables": ["sensor_events"], "seedStreaming": True})

# 2. Catch up: load the chain, then the stream, until the copy is current.
card.sync()

# 3. Announce the questions this service answers, in the queue group "events".
card.serve(["globex", "acme", "tango", "kilo"], ["alarms", "freshness", "moving_avg", "per_minute"], "events")

# 4. The loop. One thread does everything, one step at a time.
while True:
    report = card.poll(50)          # apply the next CDC batch to DuckDB (or wait ≤ 50 ms)
    for q in report.get("requests", []):                   # questions that arrived meanwhile
        answer = QUERIES[q["name"]](card, q["tenant"], q["payload"])  # the tenant: from the subject
        card.reply(q["id"], answer)                        # back to the asker
```

And one of the queries, a 10-second moving average per sensor:

```python
def v7_boundary(t):                  # the smallest UUIDv7 of a moment (epoch seconds)
    h = f"{int(t * 1000):012x}"
    return f"{h[:8]}-{h[8:12]}-7000-8000-000000000000"

def moving_avg(card, tenant, q):
    return card.query("""
        WITH b AS (SELECT sensor_id,
                          time_bucket(INTERVAL 1 SECOND, uuid_extract_timestamp(event_id)) AS t,
                          avg(value) AS v
                   FROM sensor_events
                   WHERE tenant_id = ? AND kind = ? AND event_id >= ?::UUID
                   GROUP BY ALL)
        SELECT sensor_id, t, v,
               avg(v) OVER (PARTITION BY sensor_id ORDER BY t
                            RANGE BETWEEN INTERVAL 9 SECOND PRECEDING AND CURRENT ROW) AS moving_avg
        FROM b""", [tenant, q.get("kind", "temperature"), v7_boundary(time.time() - 60)])
```

The window holds 10 one-second buckets: the current one and the 9 before it. A `RANGE` frame includes both ends, so `10 SECOND PRECEDING` would reach an 11th bucket; the service writes `window_s - 1`.

The first filter is the tenant. One DuckDB file holds every tenant the service follows, and the tenant comes from the subject the question arrived on, `query.<tenant>.<name>`, which NATS only lets an asker publish for its own tenant. Never from the payload: that is whatever the asker wrote.

The time filter compares the key itself with the smallest UUIDv7 of "60 seconds ago". It could extract the time from every key instead, `uuid_extract_timestamp(event_id) >= …`, and get the same rows, but slower: see [the boundary](#the-uuidv7-boundary) below.

**Why one thread.** `poll` does two jobs: it writes the next batch of changes into DuckDB, and it hands over the questions that arrived. The loop then answers them before the next `poll`. Writes and reads never overlap, so no lock is needed. This also fits DuckDB, which **allows only one process to write a file**: the process that writes the copy is the one that reads it.

**Where the answers go.** A question is a NATS request: the asker listens on a private inbox, and `reply` publishes the answer there as a plain message, not into a stream. libzb compresses the answer first. An answer over 256 KiB is stored as an object instead, and the reply carries its name; the asker's libzb fetches it without the asker noticing.

**Several instances.** Start a second service with its own file. Both join the queue group `events`, and NATS gives each question to one of them:

```sh
ZB_DB=/tmp/events-2.duckdb $PY examples/09-event/event_service.py --label second
```

**The credential.** `events` is a *responder*: it can read its tenants like a client and answer their questions, but it cannot write. Its NATS credential carries one tag per tenant (globex, acme, tango, kilo), and `provision.py` maps it to the same tenants in PostgreSQL, which is how libzb knows what to follow.

### Asking

`ask.py` logs in as `bob` again. It keeps no copy of the table: it declares the table *on demand*, so nothing is loaded, and asks with one call:

```python
answer = take(lib.zb_client_request(h, b"query.globex.moving_avg", b'{"kind": "humidity"}', 5000))
```

## Part 4 — MQTT devices

Many sensors speak MQTT, not NATS. nats-server speaks MQTT 3.1.1 itself, so a device can publish to the same server with a stock MQTT client. Three pieces make that work.

**The listener.** One block in the server's configuration (`scripts/native/nats-server-jwt.conf`); MQTT needs JetStream, which is already on:

```text
mqtt {
  port: 1883
}
```

**The device's credential.** In operator mode a NATS client proves who it is by signing a challenge from the server, and MQTT has no way to do that. So an MQTT device sends its JWT as the MQTT password, with any non-empty user name, and the JWT must be marked *bearer*. `jwt-bootstrap.sh` mints one per tenant; `mqtt_globex` may connect over MQTT only, and may only publish its tenant's readings:

```sh
nsc add user --account ZEBRIDGE --name mqtt_globex --bearer --allow-pub "sensors.globex.>"
nsc edit user --account ZEBRIDGE --name mqtt_globex --conn-type MQTT
```

**The gateway.** The MQTT topic `sensors/globex/7/temperature` arrives in NATS as the subject `sensors.globex.7.temperature`, so any NATS subscriber sees it. It cannot be a mutation as it stands: MQTT carries no headers, and a mutation needs `Nats-Msg-Id` and a full row. `mqtt_gateway.py` listens on `sensors.globex.>` in a queue group and writes each reading as its own principal, `mqttgw`, which `provision.py` maps to globex:

```python
async def on_reading(m):                     # sensors.<tenant>.<sensor_id>.<kind>
    _, tenant, sensor_id, kind = m.subject.split(".")
    value, ms = reading(m.data, now_ms)      # a bare number, or {"value": v, "ts_ms": t}
    eid = uuid7_at(ms)                       # the device's time becomes the row's key
    await js.publish("mutation.mqttgw.sensor_events.insert",
                     msgpack.packb({"key": {"event_id": str(eid)}, "data": row, "version": stamp}),
                     headers={"Nats-Msg-Id": eid.hex})
```

The device sends either a bare number, stamped by the gateway on arrival, or JSON with its own time in milliseconds, which becomes the row's UUIDv7. From there the row takes the same path as the others: bridge, PostgreSQL, CDC, the DuckDB copy.

```sh
$PY examples/09-event/mqtt_gateway.py &                 # MQTT readings → mutations
$PY examples/09-event/mqtt_sensors.py                   # 20 MQTT devices × 50/s, JSON with the device time
$PY examples/09-event/mqtt_sensors.py --qos 1 --bare    # acknowledged delivery, bare numbers
```

Two layers keep tenants apart. A device that publishes another tenant's topic is refused by the server, which logs `Publish Violation - Subject "sensors.acme.7.temperature"` against the device's MQTT client id. The gateway may only subscribe to its own tenant's readings, and PostgreSQL's row-level security checks every row it writes.

Measured on the same Mac, 20 MQTT devices at 1,000 readings/s for 60 s, each device on its own MQTT connection:

| | direct NATS sensors | MQTT devices through the gateway |
| --- | --- | --- |
| readings written | 60,000 of 60,000 | 60,000 of 60,000 |
| newest reading's age, p50 / p90 | 9 / 11 ms | 8 / 11 ms |
| question round trip, p50 | 4.4 ms | 6.0 ms |

The age is measured from the device's own timestamp, so the MQTT hop and the gateway together add nothing visible. The copy matched PostgreSQL exactly after both runs and a QoS 1 run with bare numbers: 140,000 rows, the same `sum(value)`.

## The queries

| name | parameters | answer |
| --- | --- | --- |
| `freshness` | none | rows, `sum(value)`, the newest reading's time, and its age when answered |
| `moving_avg` | `kind`, `window_s` 10, `since_s` 60, `sensor_id`, `series`, `bucket_ms` 1000 | per sensor, buckets of `bucket_ms` (100 ms to 60 s) and their moving average over `window_s`; the latest point, or one sensor's series |
| `per_minute` | `kind`, `minutes` 10 | per minute: readings, sensors, avg, min, max, stddev |
| `alarms` | `kind`, `above` 30, `since_s` 60 | the sensors above a threshold: how many readings, the highest, the last |

The newest reading's age measures the whole path: sensor, NATS, bridge, PostgreSQL, CDC, DuckDB. The newest reading comes from whichever sensor sent last, so the age adds at most the gap between two readings: about 1 ms with 20 sensors, 0.2 ms with 100.

## First measurements

**Test.** Two 60-second runs of `sensors.py`, one after the other: 20 sensors, then 100. Each sensor sends a reading every 20 ms, from its own random starting point within the period. On average that is one reading every 1 ms, then one every 200 µs. The second run did not reset anything: it started with the first run's 60,000 rows in PostgreSQL and in the DuckDB file.

In the other test, we ramp up and use the parallelised threads.

**Conditions.** Everything on one Mac: PostgreSQL 18, nats-server, the bridge, the service and the sensors.

**How each number is taken:**

* **Ack**: in `sensors.py`, for every write, the time from just before the publish to the bridge's verdict on `mutation_ack`. p50 is the median, p99 the time 99% of writes stayed under.
* **Newest age**: in the service, when it answers `freshness`: its clock minus the millisecond in the newest key. `ask.py watch` asks once a second, about 60 samples per run, counting only samples where a new reading had arrived.
* **Question round trip**: in `ask.py`, around each of those `freshness` requests: the question out, the service's work, the answer back.

| sensors | readings/s | verdicts | ack p50 / p99 | newest age ("freshness") p50 / p90 | question round trip p50 |
| --- | --- | --- | --- | --- | --- |
| 20 | 1,000 | 60,000 accepted | 1.8 / 3.0 ms | 9 / 11 ms | 4.4 ms |
| 100 | 5,000 | 299,996 accepted | 3.5 / 14.1 ms | 11 / 13 ms | 6.7 ms |

After both runs PostgreSQL and the DuckDB copy agreed exactly: 359,996 rows, the same `sum(value)`. One sensor's 1 s average swung by about a degree each second, as the 2 s wave predicts, while its moving average stayed within 0.3 of the base. That average held 11 buckets at the time, the off-by-one since fixed; 10 buckets hold exactly five waves, so the wave cancels fully.

## The ramp: how far one Mac goes

Restart the bridge with  `ZB_INGRESS_LANES=1..8`.

The bridge does not write one row per transaction. Each ingress **lane** pulls up to 64 mutations at once and applies them in one pipelined transaction, with one WAL flush; every mutation still sets its own principal, so row-level security holds per row. `ZB_INGRESS_LANES` (1 to 8, default 1) runs several lanes on the same stream, each with its own PostgreSQL and NATS connections.

`ramp.sh N [seconds]` runs one step: N sensors, one `sensors.py` process per 100 sensors, plus `ask.py watch`. It merges the ack latencies of every write and samples the `MUTATIONS` backlog every 2 s:

```sh
examples/09-event/ramp.sh 400 30      # 400 sensors × 50/s = 20,000 readings/s for 30 s
```

Measured 2026-09-27, 30 s per step, the bridge built ReleaseFast and restarted with each lane count. Everything on one Mac (10 cores, 16 GB), the sensor processes included:

| lanes | readings/s asked | accepted/s | refused | ack p50 / p99 | newest age p50 |
| --- | --- | --- | --- | --- | --- |
| 1 | 10,000 | 8,535 | 15% | 592 / 620 ms | 595 ms |
| 2 | 10,000 | 10,000 | 0 | 4.9 / 14.8 ms | 10 ms |
| 2 | 20,000 | 14,149 | 29% | 355 / 410 ms | 356 ms |
| 4 | 20,000 | 20,000 | 0 | 11.4 / 59.1 ms | 17 ms |
| 4 | 30,000 | 19,527 | 35% | 250 / 393 ms | 262 ms |
| 8 | 30,000 | 22,681 | 24% | 216 / 331 ms | 212 ms |

Reading it:

* **Below a lane count's ceiling, nothing queues.** The backlog stays near zero and the newest reading is 10 to 17 ms old.
* **Above it, the queue fills and then refuses.** The `MUTATIONS` stream keeps at most 5,000 waiting writes per subject (`MUTATION_BACKLOG_PER_PRINCIPAL`), and all the sensors write as `bob` on one subject. Once full, a publish is refused with code 503 (`maximum messages per subject exceeded`), and every accepted write waits behind a full queue: the ack and the newest age jump to hundreds of milliseconds together. The cap is per principal on purpose: one writer cannot flood the ingress for everyone else.
* **One lane takes about 8,500 writes/s, two about 14,000, four about 19,500.** Eight reached 22,700 with the CPU at 0% idle: past four lanes the limit was the Mac, running PostgreSQL, NATS, the bridge, the service and six Python sensor processes at once.
* **The DuckDB copy kept up at every step.** The service applied up to 22,900 changes/s, and after the whole ramp PostgreSQL and the copy agreed exactly: 3,515,172 rows, the same `sum(value)`.

So 400 sensors at 50 readings a second, or 20,000 sensors at one a second, fit with four lanes on this machine.

### Several tenants

The same ramp with the writers spread over tenants: `PRINCIPALS` lists principal:tenant pairs, one sensor process each, and the service answers every tenant from one DuckDB file. Each question is filtered on the tenant in its subject, and NATS refuses a question about another tenant: alice asking `query.globex.freshness` gets a permissions violation before anything reaches the service.

```sh
PRINCIPALS=bob:globex,alice:acme,nina:tango,omar:kilo examples/09-event/ramp.sh 400
```

Four lanes, 30 s per step, each run ending with a per-tenant check of the copy against PostgreSQL:

| writers | readings/s asked | accepted/s | refused | ack p50 / p99 | newest age p50 | largest queue |
| --- | --- | --- | --- | --- | --- | --- |
| 4 principals, 4 tenants | 20,000 | 20,000 | 0 | 12.6 / 62.7 ms | 40 ms | 459 |
| 5 principals, 4 tenants | 25,000 | 21,605 | 14% | 1,173 / 1,279 ms | 1,208 ms | 24,744 |

* **Below the ceiling, tenants cost a little lag, not throughput.** 20,000 writes/s were absorbed as with one tenant, but the newest reading was 40 ms old instead of 17: the service drains four CDC streams in its one loop.
* **Above it, principals trade refusals for delay.** Each principal has its own 5,000-write queue, so five of them hold up to 25,000 waiting writes. One principal at 30,000/s had 35% refused and waited about 250 ms; five principals at 25,000/s had 14% refused but waited about 1.2 s. The queue is the deployment's choice: `MUTATION_BACKLOG_PER_PRINCIPAL` sets its depth.
* **Every tenant ended exact**, at both steps.

## Compared with TimescaleDB

| TimescaleDB | here |
| --- | --- |
| hypertable, chunks by time | one table; the UUIDv7 key orders it by time |
| `time_bucket` | DuckDB's `time_bucket` |
| continuous aggregate | `per_minute`, recomputed on each question |
| columnstore compression | DuckDB's own columnar storage |
| `to_uuidv7_boundary` | none built in; a small function does it (below) |
| retention policy | a job that sets `deleted_at`; the sweeper removes the rows |
| queries on the primary | queries on a copy; PostgreSQL only stores |

### The UUIDv7 boundary

TimescaleDB's `to_uuidv7_boundary(t)` gives the smallest UUIDv7 of a moment: its millisecond, then version 7, variant 10 and zeros. DuckDB has none, and the service cannot create a macro: `zb_client_query` runs reads only. So `event_service.py` builds the boundary in Python, `v7_boundary(t)` above, and binds it as a parameter. Building it from an epoch number also keeps time zones out of the comparison.

Every time filter in the service is written `event_id >= ?::UUID`. DuckDB keeps a min and max key for each block of rows, so it can skip a whole block from those two numbers. A function of the key, such as `uuid_extract_timestamp(event_id) >= t`, hides them, and every row is read. The newest reading is found the same way, as `max(event_id)`, with the time extracted once.

Measured on 20M synthetic readings, keeping the last 200k, both forms returning the same rows:

| query | extracting the time | the boundary |
| --- | --- | --- |
| count and average | 19.3 ms | 1.3 ms |
| 10 s moving average | 31.3 ms | 13.7 ms |

The moving average gains less because its window still runs over every row that passes the filter. The gain relies on rows sitting in the file roughly in key order, which holds here: sensors mint their keys as they read, and the copy appends them as they arrive.

Outside the service, in a DuckDB shell that can create objects, the same boundary is a macro:

```sql
CREATE MACRO to_uuidv7_boundary(ts) AS (
  WITH h AS (SELECT lpad(printf('%x', epoch_ms(ts)), 12, '0') AS x)
  SELECT (x[1:8] || '-' || x[9:12] || '-7000-8000-000000000000')::UUID FROM h);
```
