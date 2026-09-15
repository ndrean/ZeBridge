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

## The version index: what it costs the writes, what it saves the chain (2026-09-15)

`zebridge_enable` builds `<table>_zb_version`, a B-tree on the version column, so the
generation producer reads only the rows changed since its last cut. The index is not
sent to clients. Two benchmarks, both on the Mac (10 cores, 16 GB), both manual.

### 1. PostgreSQL alone — `scripts/scenarios/version_index.py`

A scratch PostgreSQL 18 cluster of its own (wal_level logical, shared_buffers 128 MB),
so its WAL cannot touch another cluster's slots. A table of 2,000,000 rows with a primary
key and `(tenant_id, id)`, the read policy enable installs, and three variants: no version
index, a B-tree, a BRIN. Three write loads, each 2,000,000 updates in statements of 10,000.
Two runs per variant, interleaved; the two runs agree.

```sh
scripts/scenarios/.venv/bin/python scripts/scenarios/version_index.py --rows 2000000 --runs 2
```

| write load | no index | B-tree | BRIN |
| --- | --- | --- | --- |
| firehose (insert 10k, update the previous 10k), seconds | 14.1–14.7 | 16.0 | 14.2–14.4 |
| firehose, WAL | 1,479 MB | 1,730–1,736 MB | 1,481–1,483 MB |
| scattered (each row once), seconds | 12.8–18.3 | 13.1–15.0 | 12.1–13.0 |
| scattered, WAL | 1,435 MB | 1,560 MB | 1,425–1,435 MB |
| repeat (same 10k rows 200 times), seconds | 6.9–7.0 | 8.7–9.5 | 6.8–6.9 |
| repeat, WAL | 765 MB | 926 MB | 765 MB |
| repeat, updates kept HOT | 10 % | 0 % | 10 % |
| indexes on disk after | 362 MB | 425 MB | 362 MB |

| read, as the producer runs it | no index | B-tree | BRIN |
| --- | --- | --- | --- |
| "did anything change", nothing did | 329–332 ms | 0.013 ms | 302–308 ms |
| delta of 10,000 rows | 139–142 ms | 4.2–4.3 ms | 288–290 ms |
| delta of 100,000 rows | 143–145 ms | 23–24 ms | 180 ms |

### 2. The chain over TLS — `scripts/scenarios/firehose_tls.py --runs`

The bridge speaks TLS to its own nats-server. CDC stream cap 64 MiB, cadence 60 s, 180 s
of load, the index dropped or kept before the bridge boots. The firehose ran four times,
in the order with, without, without, with.

```sh
python scripts/scenarios/firehose_tls.py --seconds 180 --rate 10000 --cap-mib 64 --runs tls:on,tls:off,tls:off,tls:on
python scripts/scenarios/firehose_tls.py --seconds 180 --rate 10000 --cap-mib 64 --preload 2000000 --runs tls:off,tls:on
python scripts/scenarios/firehose_tls.py --seconds 180 --rate 500   --cap-mib 64 --preload 2000000 --runs tls:off,tls:on
```

| load | index | delta query ms, median | delta build ms, median | full-carrying build ms, median | cuts (early) | smallest margin, holes |
| --- | --- | --- | --- | --- | --- | --- |
| firehose 10k/s | no | 131, 128 | 240, 237 | 1,771, 1,886 | 35 (32), 34 (31) | 64, 0 |
| firehose 10k/s | yes | 102, 99 | 208, 195 | 1,957, 1,963 | 34 (31), 34 (31) | 64, 0 |
| 2M static rows + firehose | no | 192 | 362 | 4,811 | 31 (28) | −76, 7 samples |
| 2M static rows + firehose | yes | 99 | 253 | 4,637 | 31 (28) | −30, 3 samples |
| 2M static rows + 500/s | no | 91 | 170 | 2,859 (the first full) | 4 (0) | 360, 0 |
| 2M static rows + 500/s | yes | 35 | 87 | 2,788 (the first full) | 4 (0) | 360, 0 |

**What it says.** The index makes every delta cheaper, by about a quarter when each delta
carries 100,000 rows and by half or more when the table is large next to its changes. It
makes the idle check almost free: without it, every tick scans the whole table once per
tenant. It does not make a full cheaper, and fulls set the edge: in the firehose the cuts,
the margins and the holes were the same with and without it. With 2M static rows both runs
met holes at 64 MiB; one pair, so "fewer holes" is not proven. The price is on the writes:
up to a fifth more WAL and a third more time when the same rows are updated again and
again, and no HOT update on the table. BRIN costs the writes nothing but reads worse than
no index here, because updated rows lose the link between position and time.

**Harness notes.** Two early runs had a load that ended far before 180 s (98 s and 15 s)
and did not reproduce; they are left out. The harness now prints each run's result as
soon as it ends, and the load's error lines.

## The burst with the version index, over TLS (2026-09-15)

`burst_tls.py --runs=tls:on,tls:off,tls:off,tls:on`: the 2M-row burst, the bridge on TLS,
`bench_users` with and without `bench_users_zb_version`, in that order.

| run | events/s | PostgreSQL load | drain | WAL | full chain build / upload |
| --- | --- | --- | --- | --- | --- |
| index | 192,666 | 10.6 s | 10.4 s | 656 MB | 2,640 ms / 244 ms |
| no index | 219,564 | 9.4 s | 9.1 s | 449 MB | 2,749 ms / 241 ms |
| no index | 207,597 | 9.9 s | 9.6 s | 443 MB | 2,678 ms / 296 ms |
| index | 192,874 | 10.6 s | 10.4 s | 563 MB | 2,755 ms / 237 ms |

**What it says.** The drain equals PostgreSQL's own load time in every run, within 0.3 s:
the bridge publishes each transaction as it commits and its loop is idle more than a third
of the time. The burst rate is PostgreSQL's insert rate on this Mac, not the bridge's
ceiling. The index slows the inserts by about 8 % (27–46 % more WAL), and the event rate
follows. Publish the rate as "what the bridge kept up with", not as its limit.

## Deferring the depth rotation's full (2026-09-15)

`firehose_tls.py --preload 2000000 --runs tls:on:nodefer,tls:on:defer,...`: 2M static
rows, then 10,000 rows a second inserted and the previous second's updated, 180 s, CDC
cap 64 MiB, TLS, the version index on. `GENERATION_DEFER_FULLS` off or on.

| deferral | fulls after the boot full | fulls deferred | holes (samples) | smallest margin | bridge CPU |
| --- | --- | --- | --- | --- | --- |
| off | 5 | 0 | 6 of 133 | −76 | 42 s |
| on | 2 | 18, one built at the limit | 2 of 133 | −29 | 31 s |
| off | 6 | 0 | 8 of 134 | −122 | 47 s |
| on | 2 | 18, one built at the limit | 2 of 133 | −25 | 32 s |
| off | 6 | 0 | 7 of 134 | −77 | 46 s |
| on (log kept) | 2 | 18, one built at the limit | 4 of 133 | — | — |

**What it says.** Deferral cuts the holes by about 70 % and the bridge's CPU by about 30 %.
The kept log places every remaining hole inside a full's build: g6, the first rotation,
before the stream pruned (nothing to defer yet), and g29, the full the limit forced after
23 deferred generations, built in 5.2 s with 2.8 s of margin. A full that takes 5 s cannot
fit a stream holding about 8 s of history; no scheduling fixes that, only the cap does:
cap ≥ peak MB/s × 3 × the longest full build.

**Discarded runs.** Three runs across the day had loads that ended early (98 s, 15 s,
83 s). `pmset -g log` places a system sleep inside each: macOS froze the benchmark, the
wall clock PostgreSQL paces on kept running, and the load burst on wake. The harnesses now
hold `caffeinate -i` and mark a run INVALID when wall and monotonic time drift apart.

## The routine full in the background (2026-09-15)

`firehose_tls.py --preload 2000000 --verify --runs tls:on:defer:async,tls:on:defer:sync,tls:on:defer:sync,tls:on:defer:async`:
2M static rows, then 10,000 rows a second inserted and the previous second's updated,
180 s, CDC cap 64 MiB, TLS, version index on, deferral on. `sync` builds every full with
its delta (as before); `async` builds the routine full in the background lane.
`--verify` then seeds from the published chain in Python, the clients' plan rule, and
compares every row with PostgreSQL.

| run | fulls | full builds | holes (samples) | smallest margin | bridge CPU | chain check |
| --- | --- | --- | --- | --- | --- | --- |
| async | 2 in the background | 3,678 and 4,836 ms | 0 of 133 | 131 | 32 s | 3,800,000 rows, 0 missing, 0 extra, 0 wrong |
| sync | 2 with their delta | 3,849 and 5,074 ms | 3 of 133 | −77 | 30 s | same |
| sync | 2 with their delta | 3,769 and 5,074 ms | 3 of 133 | −77 | 31 s | same |
| async | 2 in the background | 3,527 and 4,949 ms | 0 of 133 | 132 | 31 s | same |

**What it says.** The same fulls, taking the same time, stop making holes once they no
longer hold the cut: 3 holes in each synchronous run, none in either background run, and
the smallest margin goes from 77 messages short to 131 to spare. CPU is unchanged. Every
chain, both ways, seeds to exactly PostgreSQL's rows and survives replaying all its deltas.

## 50,000 events a second for 5 minutes (2026-09-15)

```sh
python scripts/scenarios/firehose_tls.py --seconds 300 --rate 25000 --cap-mib 128 --preload 2000000 --runs tls:on:defer:async
```

2M static rows, then 25,000 rows a second inserted and the previous second's 25,000
updated; CDC cap 128 MiB (about 6 s of events at this rate), TLS, version index, full
deferral and background fulls on. One run, on the Mac.

| measure | value |
| --- | --- |
| events published during the load | 14,949,793 in 304 s, 49,208/s |
| PostgreSQL pacing | 304 s for a 300 s load: it kept up |
| WAL written | 17.3 GiB |
| slot lag (WAL not yet confirmed) | median 8 MiB, max 120 MiB |
| holes | 0 of 254 samples; smallest margin 166 messages |
| cuts | 77 deltas (73 early), median 225,000 rows, build median 595 ms, max 3,596 ms |
| background fulls | 4, of 3.9M, 5.6M, 7.5M and 9.3M rows: 5.4, 8.6, 10.7 and 13.3 s; deltas cut during each |
| bridge CPU / nats-server CPU | 137 s / 33 s over 304 s |
| bridge memory (RSS) | median 2,530 MiB, max 4,211 MiB |

**What it says.** The bridge keeps up at 50,000 events a second: the slot lag stays at a
few megabytes, so PostgreSQL holds almost none of the 17 GiB it writes. The chain holds:
no hole, with fulls up to 13 s long, because they no longer hold the cut. The limit found
is memory: a full is encoded whole before it is compressed and uploaded, about 200 bytes a
row raw (the 2M-row full was 403.6 MB raw, 65 MB compressed), so a 9.3M-row full is about
1.9 GB of rows in the bridge, and the peak reached 4.2 GB. On a VPS with 4 GB of memory
this run would have failed at its third or fourth full.
