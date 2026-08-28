# Native (no-Docker) stack

PostgreSQL + NATS JetStream, run directly on the host — no Docker, no OrbStack/Docker
Desktop VM. Exists because that virtualization layer is not free: measured on this
project, the same 2M-row burst went from ~67k events/s at ~100%+ bridge CPU under
Docker to ~189k events/s at ~86% CPU natively — a ~3x difference attributable
entirely to the VM's I/O virtualization, not to the bridge, the schema, or anything
Docker-compose-specific in configuration. See README.md's "measured throughput"
section for the full comparison.

Configured identically to `docker-compose.full.yml`'s `postgres-primary` and the
`nats-config-gen`/`nats-init` services — same PostgreSQL flags, same
`nats-server.conf.template` rendering, same MUTATIONS/REQUESTS streams and KV
buckets from `grammar.json` (the bridge creates and reconciles the CDC/INIT stream
family itself at boot, from `zebridge_catalogue`) — so a native run and a Docker run
differ only in the virtualization layer, nothing else.

## Usage

```bash
scripts/native/up.sh      # idempotent: initializes on first run, just starts thereafter
scripts/native/down.sh    # stops both, leaves data in place (like `docker compose down`)
scripts/native/down.sh --clean   # also wipes postgres-data/ and nats-data/ (like `down -v`)
```

`up.sh` prints the `DATABASE_READER_URL`/`NATS_URL`/etc. to export once it's ready.

⚠️ **`down.sh --clean` is destructive** — it deletes `postgres-data/` and
`nats-data/` at the repo root, same as `docker compose down -v` deletes those
volumes. Use it for a genuinely clean benchmark baseline (no leftover WAL retained
by a stale replication slot, no accumulated JetStream data from a prior run) —
never against a data directory holding anything you have not deliberately decided
is disposable.

## What's committed vs. what isn't

`up.sh`, `down.sh` and `nats-server.conf` are committed. `nats-server.conf` is the
*rendered* config (real values, not `${VAR}` placeholders) — safe to commit here
because `.env.admin`/`.env.bridge`, which those values come from, are themselves
already tracked in this repo with throwaway `_changeme` dev credentials; a rendered
native config carries no more exposure than what is already there.

`postgres-data/`, `nats-data/`, and this directory's own `*.log`/`*.pid`/`seed.txt`
are gitignored — runtime state, not configuration.

## Requirements

- `postgresql@18` and `nats-server`/`nats` (Homebrew: `brew install postgresql@18 nats-server nats-io/nats-tools/nats`)
- `jq`, `envsubst` (`brew install jq gettext`)
- Nothing listening on host ports 5432 or 4222 — if Docker's stack is also up, stop
  it first (`docker compose -f docker-compose.full.yml down`) since it publishes
  NATS on the same 4222.
