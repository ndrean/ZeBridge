# An example of a measured throughput

Same machine (Apple M2 Pro, 10 cores), same load (2,000,000-row burst, detailed below), two environments:

| environment | end-to-end rate |
| --- | --- |
| PostgreSQL + NATS in Docker on the same host | **~100k events/s** |
| PostgreSQL + NATS native on the host (no container/VM virtualization) | **200k+ events/s** |

The gap is the virtualization layer, not the bridge: the same binary, same code, same load — only Docker's I/O virtualization differs. If you're chasing a specific throughput number on your own hardware, measure both before assuming the bridge itself is the ceiling.

⚠️ Treat either figure as a reference point, not a spec — absolute throughput moves with machine, build mode, PostgreSQL version, and host load (a background process competing for CPU cores measurably drops it). A rerun that differs is not automatically a regression; see below for the number that _is_ comparable across machines.

Full method, raw output, and how to read the <code>LOOP</code> line

**Method** (Docker environment):

| | |
| --- | --- |
| machine | Apple M2 Pro, 10 cores, macOS |
| build | `zig build -Doptimize=ReleaseFast` — a Debug build is several times slower |
| PostgreSQL | 18.4 in Docker on the same machine |
| NATS | JetStream, file storage, in Docker on the same machine, **no consumers attached** |
| bridge | one instance, `BASE_BUF=14` (16 KB/event), `RING_BUFFER_COUNT=32768`, MessagePack |
| table | `users` — 4 small columns, single-column PK, `REPLICA IDENTITY DEFAULT` |
| load | 2000 statements × a 1000-row `INSERT … SELECT … generate_series`, each its own transaction = **2,000,000 rows** |

```bash
python3 -c "
for i in range(2000):
    print(\"INSERT INTO public.users (name,email,inserted_at,updated_at) \"
          \"SELECT 'User-%d-'||i, 'u%d-'||i||'@e.com', now(), now() \"
          \"FROM generate_series(1,1000) i;\" % (i,i))
" > load.sql
docker exec -i postgres-primary psql -U postgres -q -f - < load.sql
```

`generate_series(1,1000)` is PostgreSQL's set-returning function: it produces a thousand
rows, and `INSERT … SELECT … FROM generate_series` inserts one row per value — so each
statement writes 1000 rows in **one** transaction rather than 1000 round trips. That is
deliberate: the point is to saturate the WAL, and a client sending 2,000,000 separate
`INSERT`s would be measuring the client, not the bridge. 2000 statements × 1000 rows is
also 2000 _transactions_, so the WAL carries 2000 BEGIN/COMMIT pairs — visible as the gap
between `wal_messages` and `cdc_events` below.

**How to measure it.** The `METRICS` line is a 15 s sampler with no timestamp, so it
cannot answer "how long did this take" — poll the counter instead, which updates the
moment the batch publisher acks:

```bash
# in another shell, before starting the load
while :; do
  printf '%s %s\n' "$(date +%s.%N)" \
    "$(curl -s localhost:9090/metrics | awk '/^bridge_cdc_events_published_total /{print $2}')"
  sleep 0.5
done | tee drain.log
```

End-to-end rate is `2000000 / (t_at_2M − t_at_start)`.
PostgreSQL's own write time is the wall clock of the `psql` command above (`time docker exec …`).
CPU is `bridge_cpu_seconds_total` sampled the same way — subtract the endpoints rather than reading the `cpu=%` field, which is a per-interval average.

⚠️ **Detach every CDC consumer first.** The figures below were taken with none attached, and a browser client replaying 2M events into OPFS changes both the number and, usually, the browser. Check with `nats consumer ls CDC`.

⚠️ **Host load matters more than you'd expect.** A CPU-bound process (the bridge) loses far more wall-clock time to a busy host than an I/O-bound one (PostgreSQL) — PostgreSQL mostly waits on disk either way, so background CPU contention barely touches its write time, while the bridge needs a core continuously and pays for every scheduling delay. A single unrelated process pinning even a few cores can cut measured throughput by half or more. Close anything CPU-heavy before trusting a number.

**Result:**

```txt
LOOP iters=1407805 idle=10274 recv_ms=139 proc_ms=1494 cpu=31%
METRICS uptime=15 wal_messages=1443766 cdc_events=1440690 …
LOOP iters=618169  idle=11431 recv_ms=90  proc_ms=616  cpu=16%
METRICS uptime=30 wal_messages=2004279 cdc_events=2000000 …
```

| measure | value |
| --- | --- |
| PostgreSQL writing 2M rows | ~6 s |
| all 2M events in JetStream | under 20 s from bridge start |
| end-to-end, producer included | **~100k events/s** |
| WAL loop busy time (`15 − idle`) | ~7.6 s → **~260k events/s** while draining |
| time inside libpq (`recv_ms`) | 0.9% of the interval |
| time decoding + packing (`proc_ms`) | 7.9% |
| CPU while draining | **31% of one core** (`bridge_cpu_seconds_total` rose 7.6 s in total) |

The number that _is_ comparable across machines is `iters` for a fixed event count. The WAL loop runs roughly one iteration per WAL message, so 2M events should cost ~2M iterations wherever it runs; a rerun within a few percent means the hot path is unchanged even when the wall-clock figures differ.

**This benchmark is producer-bound in Docker**: PostgreSQL needs 6 s to write what the bridge drains in 7.6 s of loop time, and the loop is idle ~72% of the first interval — the ceiling is higher than 260k. Running the same load natively removes Docker's I/O virtualization and reaches 200k+ events/s end-to-end; the CPU-bound-vs-I/O-bound asymmetry above is why that gap exists.

**What it does not measure**: wide rows, `jsonb`/array-heavy tables, `REPLICA IDENTITY FULL` (which doubles tuple volume), a remote PostgreSQL or NATS, consumers reading concurrently, or a NATS server under back-pressure. Each of those moves the number.

**Reading the `LOOP` line:**

| field | meaning |
| --- | --- |
| `iters` | WAL loop iterations in the interval |
| `idle` | iterations that found nothing and slept 1 ms — high `idle` means the bridge is waiting for PostgreSQL, not struggling |
| `recv_ms` | milliseconds inside `receiveMessage` (libpq + framing) |
| `proc_ms` | milliseconds decoding tuples and packing them into the ring buffer |
| `cpu` | process CPU over the interval, all threads — `100%` is one core saturated |

`recv_ms` approaching the interval length with `idle=0` is the signature of a reader that cannot drain its socket — the exact shape of a bug this project has hit once already, in `wal_stream.zig`.

## TLS or plain TCP between the bridge and NATS (2026-09-15)

Four measurements answer one question: does the bridge lose anything when its link to
NATS is TLS instead of plain TCP? The link matters because a nats-server client port with
TLS refuses plaintext from every client, and the port phones use must be TLS.

**Where they ran, and what that means for a VPS.** All four ran on the same Apple M2 Pro
(10 cores), PostgreSQL 18, nats-server 2.14.6 and the ReleaseFast bridge native on the
host, everything on localhost. A VPS has fewer cores, often shared with neighbours, so
expect lower absolute numbers and less headroom; the comparisons between TLS and plain are
the meaningful part. Each script runs as it is on a VPS, and should be rerun there before
publishing a figure for it. The TLS certificates are nats.zig's test certificates
(`nats.zig/tests/configs/certs`), verified with their CA.

Every run below was isolated: a scratch database rendered from the templates, its own
publication and slot, scratch nats-servers with the buckets and MUTATIONS stream that
`up.sh` creates, a bridge on its own port. ⚠️ Isolation does not cover WAL: a benchmark
writing millions of rows produces gigabytes of WAL for the whole cluster, and any INACTIVE
logical slot retains it until `max_slot_wal_keep_size` invalidates the slot. Run these on a
PostgreSQL of their own, or with every other bridge of the cluster running.

### 1. One acknowledged publish, by message size — `scripts/scenarios/tls_cost.py`

A probe on the vendored nats.zig publishes to a file-backed JetStream stream and waits for
each acknowledgement, as the bridge does, against two nats-servers identical but for TLS.
Median of three runs.

| message | plain | TLS | TLS / plain (time, probe CPU, server CPU) |
| --- | --- | --- | --- |
| 400 B, one event, 60,000 messages | 18,432 msg/s, 7.4 MB/s | 17,793 msg/s, 7.1 MB/s | ×1.04, ×1.03, ×1.05 |
| 16 KB, a batch, 12,000 messages | 15,284 msg/s, 245 MB/s | 13,139 msg/s, 210 MB/s | ×1.16, ×1.21, ×1.12 |
| 256 KB, 1,500 messages | 4,814 msg/s, 1,233 MB/s | 3,152 msg/s, 807 MB/s | ×1.53, ×3.50, ×1.66 |

Small messages are bound by the acknowledgement round trip, large ones by encryption.

### 2. The 2M-row burst, PostgreSQL → NATS events — `scripts/scenarios/burst_tls.py`

The load of this document's first section: 2,000 transactions of 1,000 small rows each into
a `users`-shaped table. The bridge with generations on (cadence 20 s), no consumers
attached. The rate is 2,000,000 over the time from the first to the last published event,
read from `bridge_cdc_events_published_total` every 250 ms. The full chain column is the
producer's full of all 2M rows cut after the burst.

| run | link | events/s | bridge CPU | NATS CPU | full chain (build) | upload of the 57.8 MB object |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | plain | 201,765 | 8.2 s | 1.4 s | 2,601 ms | 188 ms |
| 1 | TLS | 207,493 | 8.3 s | 2.0 s | 2,438 ms | 239 ms |
| 2 | plain | 219,093 | 7.8 s | 1.6 s | 2,588 ms | 183 ms |
| 2 | TLS | 207,705 | 8.4 s | 2.0 s | 2,435 ms | 238 ms |

TLS moves the event rate by less than plain moves between its own two runs (9 %). It costs
the bridge up to 8 % more CPU and nats-server 25–41 % more; the chain upload takes about
30 % longer, a quarter of a second for 57.8 MB, a tenth of the chain's build.

### 3. The chain under a firehose, PostgreSQL → NATS objects — `scripts/scenarios/firehose_tls.py`

The risk TLS could add: the producer uploads its chains over the same link, and if a cut is
late while the CDC stream prunes, the chain's cutoff falls below the stream's oldest
message and a client seeding from it cannot splice. The load: every second one INSERT of
10,000 rows shaped like `test_types`, then an UPDATE of the previous second's rows, for
180 s, so about 20,000 events a second, about 9 MB/s on the stream. Cadence 60 s. The
stream's byte cap (`CDC_MAX_BYTES`) set small so it prunes all along. Once a second the
harness samples margin = the newest manifest's cutoff + 1 − the stream's oldest sequence,
in messages (a message is a batch of events); negative is a hole.

```
python3 scripts/scenarios/firehose_tls.py --seconds 180 --rate 10000 --cap-mib 256   # also 64, 32
```

| cap | link | cuts (early) | fell off (log) | build ms median / max | upload ms median / max | margin min, negative samples |
| --- | --- | --- | --- | --- | --- | --- |
| 256 MiB | plain | 11 (7) | 0 | 432 / 1,911 | 12 / 77 | 639, 0 of 136 |
| 256 MiB | TLS | 11 (7) | 0 | 484 / 1,939 | 16 / 89 | 636, 0 of 136 |
| 64 MiB | plain | 33 (30) | 0 | 262 / 2,666 | 6 / 110 | 65, 0 of 136 |
| 64 MiB | TLS | 34 (31) | 0 | 254 / 2,495 | 7 / 122 | 40, 0 of 135 |
| 32 MiB | plain | 58 (55) | 2 (another table) | 224 / 2,645 | 5 / 103 | −14, 3 of 134 |
| 32 MiB | TLS | 58 (55) | 2 | 214 / 2,569 | 6 / 123 | −15, 8 of 136 |

The upload is a few milliseconds of a cut that takes a quarter of a second; TLS leaves the
producer's behaviour unchanged. The edge is the cap measured in seconds of burst: at 32 MiB
the stream held about 172 messages while pruning 35–52 a second, 3.5 to 5 s of history,
against builds of up to 2.6 s, and both links met holes (TLS in more samples, one run each).
Size `CDC_MAX_BYTES` for ten seconds or more of the worst burst; the default 1 GiB holds
about two minutes at this rate.

### 4. Two edge topologies, with consumers attached — `scripts/scenarios/firehose_topology.py`

With phones connected, encrypting every delivery is the larger TLS work. Topology 2: TLS on
nats-server's client port, the bridge on `tls://`, consumers to nats-server over TLS.
Topology 1: nats-server plain on localhost, the bridge on `nats://`, HAProxy terminates the
consumers' TLS in TCP mode with the TLS-first handshake. Load as in 3, cap 64 MiB,
20 consumers (`nats bench js ordered`, each tailing the whole CDC stream). One run each.

```
python3 scripts/scenarios/firehose_topology.py --clients 20 --seconds 180 --rate 10000 --cap-mib 64
```

| | nats-server CPU | HAProxy CPU | server total | bridge CPU | consumers CPU | cuts (early) | margin min, holes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| topology 2 | 42.4 s | — | 42.4 s | 36.4 s | 38.6 s | 35 (33) | 65, 0 of 139 |
| topology 1 | 20.6 s | 44.4 s | 65.0 s | 36.0 s | 46.8 s | 34 (32) | 38, 0 of 139 |

Terminating in HAProxy halves nats-server's CPU and costs HAProxy twice that: about 50 %
more CPU on the server side, and the chain no safer on one machine. Topology 1 needs
nats-server's TLS block with `allow_non_tls: true`, or clients that asked for TLS refuse
the server's plain INFO ("secure connection not available").

**What the four say together, for a VPS demo:** TLS on the bridge's link and on every
client's link is the sound default. Its cost is CPU on nats-server, which grows with the
number of connected clients, so that is the margin to watch on a VPS; the event rate and
the chain's safety do not depend on the choice of link, the chain's safety depends on the
stream cap.
