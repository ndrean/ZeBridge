# zb-archiver (Python)

The ZeBridge readings archiver: it copies one tenant's sensor readings from their NATS stream
(`READINGS_<tenant>`) into Parquet files in S3 or Cloudflare R2, and records each file in an archive
manifest in NATS KV. Readers combine those files with the live stream; the example
[examples/16-archive](https://github.com/ndrean/zebridge/blob/main/examples/16-archive/README.md)
has a responder that does, and explains the whole pipeline.

```sh
pip install ./zb-archiver        # from a checkout; Python 3.10 or later
```

It depends on `nats-py[nkeys]`, `duckdb` and `boto3`. A native archiver with no Python (`bridge_archiver`, and `libzbarchive` for a C, Go, Rust or Swift host) writes the same archive and can replace this package: see [the example](https://github.com/ndrean/zebridge/blob/main/examples/16-archive/README.md#the-native-archiver). It also needs the native fetcher
`bridge_readings_fetch`, which is built with the bridge (`zig build readings-fetch -Doptimize=ReleaseFast`)
and ships in the `zebridge` package. It finds the fetcher through `--fetch-bin` / `fetch_bin`,
then `ZB_READINGS_FETCH`, then `PATH`. The archiver has no Python reader.

## Scope

**What it is.** It archives sensor readings. Sensors publish to NATS (or MQTT) on
`sensors.<tenant>.<sensor>.<kind>`, and their readings never pass through PostgreSQL. It is not the
replication path: PostgreSQL tables reach SQLite or DuckDB replicas through CDC and libzb, and
the archiver is not involved.

```
sensors ─▶ NATS / MQTT ─▶ READINGS_<tenant>  ─▶ archiver ─▶ Parquet in S3 ─▶ manifest (NATS KV)
                                    │                                              │
                                    └──── live tail ──▶ responder ◀── files ───────┘
```

**What it is like.** Two familiar designs, at the scale of one small process per tenant:

| Part                   | Closest equivalent                                              | Difference                                                                                                                                    |
| ---------------------- | --------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| The archiver           | A Kafka Connect S3 sink, a Kinesis Firehose delivery stream     | No Kafka or Connect cluster. The position (`covers_up_to`) and the file list commit together, in one write made only after the upload is verified |
| Archiver and responder | Thanos (sidecar and querier), Druid (hand-off to deep storage)  | JetStream is the hot tier, Parquet in S3 the cold one, the NATS KV manifest records the hand-off, and the responder queries both              |
| The manifest           | A table-format catalog (Iceberg, Delta Lake, DuckLake)          | A list of files and a position, nothing more: no snapshots, no schema evolution, no compaction                                               |

It is not a stream processor (Flink, Spark): it transforms, joins and aggregates nothing.
Aggregations happen when the data is read.

**When it fits.** You already run ZeBridge, so the NATS server, the per-tenant streams and the
credentials exist. Devices in the field push readings, tenants must stay isolated, and you want a
durable archive in open Parquet that any tool reads (DuckDB, Polars, Spark, Athena, Trino). It
stops rather than skip readings that left the stream before a file held them
([what it guarantees](#what-it-guarantees)).

**When it does not.**

- **Monitoring metrics** (servers, services, alerting): use Prometheus with Thanos or Mimir.
- **Interactive analytics over a long history** (many dimensions, months of data): use Druid,
  Pinot or ClickHouse.

**Limits today.**

- One numeric value per reading, with fixed subjects and columns.
- One writer per tenant, so throughput scales across tenants, not within one. Throughput is
  [measured in the example](https://github.com/ndrean/zebridge/blob/main/examples/16-archive/README.md#performance).
- One file per chunk and no compaction, so a slow tenant makes many small files.
- No downsampling and no retention rules for the files.
- The example's responder downloads every file the manifest names. Past a modest history, query
  the Parquet with an engine that prunes files by time range.

## Command

```sh
NATS_URL=tls://nats.example.com:4222 NATS_CREDS=archiver-acme.creds \
S3_ENDPOINT=https://ACCOUNT.r2.cloudflarestorage.com S3_BUCKET=zb-events \
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  zb-archiver --tenant acme --flush-s 60
```

`python -m zb_archiver` runs the same command. Mint the credential with
`bridge --mint-archiver --name archiver-acme --tenant acme`. It can read `READINGS_acme` and write acme's
manifest, and nothing else ([DEPLOYMENT](https://github.com/ndrean/zebridge/blob/main/DEPLOYMENT.md)).

| Option | Default | Purpose |
| --- | --- | --- |
| `--tenant` | Required | Tenant to archive |
| `--flush-s` | 60 seconds | Start a flush after readings have waited this long |
| `--batch` | 100,000 messages | Start earlier when a full batch is waiting; maximum messages per chunk |
| `--outage-s` | 3,600 seconds | S3 outage allowance used in the stream-retention check |
| `--fetch-bin` | `ZB_READINGS_FETCH` or `PATH` | Native fetcher executable |
| `--exit-when-idle` | 0 (never) | Stop after this long with nothing to flush (tests) |
| `--accept-loss` | off | Start even if the stream lost readings the archive does not hold |

Environment: `NATS_URL`, `NATS_CREDS`, `NATS_TLS_CA` (a server certificate signed by a private CA);
`S3_ENDPOINT` (default `http://127.0.0.1:8333`), `S3_BUCKET` (default `zb-events`), `S3_REGION` (default `auto`),
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`. Only a local endpoint may take its keys from the file `S3_KEYS`
(default `~/.config/zebridge/s3.json`).

| Exit | Meaning | Action |
| --- | --- | --- |
| 0 | Stopped (SIGINT, SIGTERM, `--exit-when-idle`) | — |
| 1 | The fetcher is missing or failed | Install the fetcher, or read its error |
| 2 | The stream keeps readings for less than 2 × flush + outage | Raise the stream's `max_age` |
| 3 | Another archiver wrote the manifest | Run one archiver per tenant |
| 4 | Readings left the stream before any file held them | Investigate; `--accept-loss` to go on |
| 5 | The object in S3 does not have the size that was written | Check the bucket; the manifest was not changed |
| 9 | Killed on purpose (`--crash-after-upload`, tests) | — |

A supervisor restarts on every exit code. Codes 2, 3 and 4 need a person to look.

## Library

```python
import asyncio
from zb_archiver import ArchiverConfig, ExitCode, run

stop = asyncio.Event()                       # set it to stop after the current chunk, or while waiting for the stream
code = await run(ArchiverConfig(tenant="acme", flush_s=60, nats_url="tls://…",
                                creds="archiver-acme.creds", bucket="zb-events"), stop)
assert code in ExitCode
```

`run` returns an `ExitCode` and never raises for an outcome it can name. The command installs the
signal handlers; a library caller owns `stop`. Every field of `ArchiverConfig` matches an option above.
`nats_url`, `creds`, `s3_endpoint` and `bucket` override the environment, and `say` replaces `print`
for its status lines.

For readers of the archive:

- `zb_archiver.layout` is the contract that the writer and its readers share. It holds the stream and
  subject names, the manifest key, `read_manifest` and `write_manifest` (with a revision check), the file
  key, the DuckDB `COLUMNS` and `SELECT_ROWS`, and `reading(msg)`. `reading(msg)` is the Python decoder
  for one stream message. It follows the same rules as the native one (`src/readings_decode.zig`), and
  `scripts/scenarios/readings_fetch.py` checks that both decoders agree.
- `zb_archiver.env` provides `connect` (NATS with the principal's own inbox), `s3`, `stream_when_there`
  and `fetch_files`. `fetch_files` downloads the manifest's files into a local cache, each file once.

## What it guarantees

The example's README owns this list, and the scenarios test it:
[Recovery and data integrity](https://github.com/ndrean/zebridge/blob/main/examples/16-archive/README.md#recovery-and-data-integrity).
In short: the upload comes first and the manifest second. The position is the manifest's `covers_up_to`.
A gap stops the archive. A stale manifest revision stops a second writer.

## Tests

```sh
cd zb-archiver && python -m unittest discover -s tests
```

These need only `duckdb` and `nats-py`, with no server. The scenarios that need a real stream are
`scripts/scenarios/archive.py`, `readings_fetch.py` and `readings_roles.py`.
