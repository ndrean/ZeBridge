# The PowerSync comparison

The harness behind [the comparison with PowerSync](../../../COMPARISON.md): the same workload, run by ZeBridge and by PowerSync against one PostgreSQL database, from one host. The figures and how to read them are there; this page is how to run it again.

## What it measures

Four phases, the same for both systems, on a table of 10,000 rows of about 200 bytes (`storm_items`, tenant `cmp`):

| Phase | What | ZeBridge | PowerSync |
| --- | --- | --- | --- |
| `seed N` | N clients from empty replicas, then again with their replicas kept: time until each holds the 10,000 rows | `zb_cmp.py` | `client/ps_cmp.mjs` |
| `roundtrip S` | one client writes a marker each second for S seconds; another reports when it has it | same | same |
| `burst N` | the administrator inserts N rows in one statement; one client times when it holds them all | same | same |
| `writes P M` | P clients send M inserts each as fast as they can: writes per second, and each write's latency | until its verdict returns | until the backend has committed it and the SDK has completed the upload |

## Fair by construction

Each of these rules came from a run that was not fair without it:

- **The clients run on the comparison host**, next to the services. Through one SSH tunnel from a laptop, ZeBridge wrote 391 writes/s where the host itself measured 1,658.
- **One system at a time:** stop one stack before measuring the other.
- **A fresh table before each seed** (`db.sh table`): a table grown by earlier bursts makes the next seed slower.
- **The same table setup for both.** The table is enabled by `zebridge_enable` (its triggers and tenant policy), and PostgREST writes as ZeBridge's write role with `zb.principal` set per request, so PowerSync's uploads pay the same database cost. `powersync.sh up` checks that the policy applies (a mapped principal writes, an unmapped one is refused). In an earlier run whose backend role bypassed the policy, PowerSync wrote about 60% more on the larger server (other settings differed too, and worked against it).
- **PowerSync with no delay between uploads** (`crudUploadThrottleMs: 0`; the SDK's default waits 1 s, which alone made its round trip about 930 ms) and **its bucket storage on the host**, as its own deployments place it. With the storage on the remote database, its seed and burst figures were slower for a reason that has nothing to do with sync.
- **Sync Streams**, not the deprecated `bucket_definitions`. It did not change the figures, but it removes the objection.
- **Write rows more than once.** ZeBridge's write figures repeated within about 10%; PowerSync's varied by a factor of two to three between runs, so one run of a write row says little.

What it does not measure: the work of building and running the backend PowerSync's writes need (it is given PostgREST, ready), and the principal travels in a plain header that PostgREST does not verify (the policy's cost is the same as with a verified one).

## Never on a production host

The first run put 50 PowerSync Node clients on a production hub: the host ran out of memory, and the kernel killed the hub's bridge and nats-server (systemd restarted them). Use a scratch machine. Every container here has a memory cap, `powersync.sh run` kills its client when the host's available memory falls under 1.5 GB, and 10 clients is the most measured with PowerSync. On a 2 vCPU, 4 GB host, 50 clients did not fit for either system.

## What you need

- **A scratch host** reachable over SSH as a user with `sudo`: Docker, `nats-server` (2.15 or newer), `psql`, Python 3, and the release's `zebridge` and `libzb` packages (`gh release download v<V> -p '*_amd64.deb'`, then `sudo dpkg -i`), with no ZeBridge service enabled. Ports used, all on 127.0.0.1: 14222 (NATS), 27534 (the bridge), 18080 (PowerSync), 13000 (PostgREST), 15432 (PowerSync's storage).
- **A scratch PostgreSQL database** with logical replication, and its connection file in the repository root with `SB_ADMIN_URL` (or `ADMIN_DATABASE_URL`), `DATABASE_READER_URL`, `DATABASE_WRITER_URL`, `BRIDGE_CDC_PUBLICATION` and `BRIDGE_CDC_SLOT`. A Supabase project works (its direct connection is IPv6 only; the containers use the host network for that reason). These scripts drop tables, roles and slots there.
- **On your machine:** `psql`, Python 3, and the scenarios' Python venv (`scripts/scenarios/README.md`), which mints the client identities. The init SQL is rendered by the host's installed bridge, so the database gets the SQL of the version the stack runs.

Then `cp settings.example settings` and fill in the host, the connection file and the database's `system_identifier`. Every database step checks that identifier and refuses any other database.

## Running it

From this folder, in this order:

```sh
./db.sh rtt                 # the host's distance to the database: it sets both systems' write figures
./db.sh init                # ZeBridge's init SQL
./db.sh table               # the 10,000-row table

./zebridge.sh up            # the temporary stack
./zebridge.sh creds         # 50 client identities, mapped to tenant cmp
./zebridge.sh run seed 10
./zebridge.sh run roundtrip 30
./zebridge.sh run burst 100000
./zebridge.sh run writes 1 500
./zebridge.sh run writes 10 1000
./zebridge.sh down          # one system at a time

./db.sh table               # a fresh table for PowerSync's seed
./powersync.sh roles        # PowerSync's roles and publication
./powersync.sh up
./powersync.sh run seed 10
./powersync.sh run roundtrip 30
./powersync.sh run burst 100000
./powersync.sh run writes 1 500
./powersync.sh run writes 10 1000
./powersync.sh down
```

`zebridge.sh up` starts from a new NATS operator each time, so run `zebridge.sh creds` after every `up`: the previous identities are refused. It also waits for the table's first snapshot, since a client connecting before it would time the wait. Repeat the write rows (`run writes …`) at least twice more for each system. `db.sh count` shows what reached the table. PowerSync's clients may log `Sync aborted Error: illegal end of stream in BSON object` and reconnect; `run` counts the lines, and in the measured runs no write was lost.

## A crowd of 500

The phases above use real clients, which do not fit 500 to a host. The crowd measures what each server delivers to many clients and takes from them, with light clients that do the wire work and nothing else:

| Direction | ZeBridge | PowerSync |
| --- | --- | --- |
| Sync | `zebridge.sh crowd sync N MARKERS` (`zb_crowd.py`): each reader reads the chain's manifest, downloads its objects and follows CDC from the cut, as libzb does on the wire; it neither decompresses nor applies | `powersync.sh crowd sync N MARKERS` (`client/ps_crowd.mjs`): each reader opens `/sync/stream` as the SDK does (BSON preferred), decodes only the control documents and scans the data documents for markers |
| Writes | `zebridge.sh ingress N RATE SECONDS`: `scripts/bench/ingress_load.py`, N principals, one NATS connection each | `powersync.sh crowd writes N RATE SECONDS`: N apps uploading as `uploadData` does, one request at a time, up to 200 rows, to PostgREST |

Markers are written through each system's write path: a mutation from `p000` for ZeBridge, a PostgREST insert for PowerSync. The committed figure is PostgreSQL's row count, sampled every second (`count_rate.py`). Both write crowds use UUIDv7 keys.

```sh
./zebridge.sh up && ./zebridge.sh creds 500 && ./zebridge.sh tools
./zebridge.sh crowd sync 500 30           # each run on a fresh `up`: see below
./zebridge.sh ingress 500 1000 30
./zebridge.sh down
./db.sh table && ./powersync.sh roles && ./powersync.sh up
./powersync.sh crowd sync 500 30          # each run after `db.sh table` and `up`
./powersync.sh crowd writes 500 1000 30
```

What 500 clients taught, kept in the scripts:

- **ZeBridge:** each object download holds a consumer for 5 minutes, and a client cannot delete it. So 500 cold readers on a chain of a full and 12 deltas need 6,500 consumers, past the hub's cap of 5,000 per stream. The temporary stack keeps the hub's `default_max_consumers: 5000` (nats-server 2.15 defaults to 1,000). Run each sync on a fresh `up`: a new NATS makes the bridge build a single full, and no consumers are left from the last run.
- **PowerSync:** one API process serves 200 connections at most (`max_concurrent_connections`), and its documentation advises 100. `ps_service.py` runs PowerSync's production layout: one replication process (`start -r sync`) and one API process (`start -r api`) per 100 connections. `ps_crowd.mjs` spreads its readers over them, as a round-robin load balancer would.
- **PostgREST's pool** is 10 connections by default. Under 500 apps it answers `504 Timed out acquiring connection` and the apps retry. That is the default a PowerSync backend would start from, but it is not tuned.
- **One host for services and crowd:** the readers and the services share its cores, so the figures are what one host holds, not what a server alone would.

## Cleaning up

```sh
./zebridge.sh clean         # the stack, ~/zbcmp/zb and ~/zbcmp/cl on the host, the creds here
./powersync.sh clean        # the containers, their images, ~/zbcmp/ps and ~/zbcmp/psc
./db.sh wipe                # both systems' objects on the database (stop both stacks first: active slots stay)
./db.sh inventory           # what is left
rm -rf secrets
```

`secrets/` holds the generated passwords, the JWT signing key and the database's administrator URL; it and `settings` are git-ignored.
