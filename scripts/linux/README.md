# The scenarios' stack on Linux, without Docker

The same stack as [`scripts/dev`](../dev/README.md) (PostgreSQL with logical replication, NATS in operator mode, credentials minted by `nsc`, the bridge's init SQL), as plain processes.
For a sandbox, a CI runner or a workstation where containers are not an option.
On a Mac use `scripts/dev` (containers) or `scripts/native` (Homebrew, for speed measurements).

## Install checklist (Ubuntu or Debian)

Do these once per machine, in this order. Each line says what breaks when it is missing.

- [ ] **PostgreSQL 15 or newer** with its server binaries: `sudo apt install postgresql postgresql-contrib` (16 on current Ubuntu). The init SQL reads `pg_publication_rel.prqual`, which 14 does not have: `up.sh` refuses an older server before it touches anything.
- [ ] **Stop the system cluster the package starts**: `sudo systemctl disable --now postgresql`. It listens on 5432, where `up.sh` wants to start its own cluster.
- [ ] **Development headers for the build**: `sudo apt install libsqlite3-dev libzstd-dev libpq-dev build-essential curl openssl`. Without them the libzb build stops with "sqlite3.h not found" and the bridge build with "libpq-fe.h not found".
- [ ] **`nats-server` 2.15 or newer** (2.11 lacks `default_max_consumers`), **`nats`** and **`nsc`**, from the releases of [nats-server](https://github.com/nats-io/nats-server/releases), [natscli](https://github.com/nats-io/natscli/releases) and [nsc](https://github.com/nats-io/nsc/releases), on `PATH`. Check with `nats-server --version`.
- [ ] **The Zig toolchain (0.17)** on `PATH`.
- [ ] **The submodule**: `git submodule update --init`. An empty `nats.zig/` makes the build stop with "unable to find nats".
- [ ] **Python 3.15 for the scenario venv.** The minimum supported version is 3.14 because `serve` imports `compression.zstd`. Ubuntu's `python3` may be older, and that is fine: only the venv needs the new one. With `uv`: `uv venv --python 3.15 scripts/scenarios/.venv` (or `python3.15 -m venv scripts/scenarios/.venv` if `ensurepip` is there), then `uv pip install --python scripts/scenarios/.venv/bin/python -r scripts/scenarios/requirements.txt`. Check with `scripts/scenarios/.venv/bin/python --version`.
- [ ] **Node and `pnpm`**, then `cd zb-client-ts && pnpm install` (builds `better-sqlite3`, which needs `build-essential`; for `outbox_*`, `reconcile`, `hub_fallback`, `jwt_renew`) and `cd examples/04-node-consumer && pnpm install` (the Node worker of `migrate_both`, `column_flood`, `rekey_*` and others).
- [ ] **`zstd` command** (`sudo apt install zstd`), for `shared_gap`.
- [ ] **`.env.bridge`** in the repository root, with `DATABASE_READER_URL` and `DATABASE_WRITER_URL` pointing at `127.0.0.1:5432`.

## Setup, in this order

Everything runs from the repository root.

**1. Build:**

```sh
zig build -Doptimize=ReleaseFast
(cd libzb && zig build -Doptimize=ReleaseFast)
(cd libzb && zig build endpoint-probe)                                                  # hub_fallback
zig build -Doptimize=ReleaseSafe -Dtest-faults=true -p zig-out-faults                   # walls (case E)
(cd libzb && zig build -Doptimize=ReleaseSafe -Dtest-faults=true -p zig-out-faults)     # receipt_faults, recreate_recovery 4/13/15
```

**2. Start the stack.** The publication has no default and is not in `.env.bridge`: pass a name and use it everywhere below.

```sh
BRIDGE_CDC_PUBLICATION=zb_pub scripts/linux/up.sh
```

It initialises a PostgreSQL cluster, mints the NATS credentials, starts `nats-server`, applies the bridge's init SQL, creates the publication, and prints the exports of step 3.
Run again, it checks every step before it acts.

**3. In every terminal you use for the next steps:**

```sh
source scripts/scenarios/.venv/bin/activate
set -a && . ./.env.bridge && set +a
export BRIDGE_CDC_PUBLICATION=zb_pub
export NATS_URL=nats://127.0.0.1:4222
export ZB_PGBIN=/usr/lib/postgresql/16/bin ZB_PG_CTL=$PWD/scripts/linux/pg_ctl ZB_JWT_STATE=$PWD/scripts/linux/state
```

The last two lines are the ones `up.sh` prints with your values.
There is no `ZB_NATS_CONTAINER` here: the scenarios that restart NATS signal the `nats-server` process (its pid is in `scripts/native/nats-server.pid`), and `ZB_PG_CTL` is a wrapper that stops and starts the cluster `up.sh` initialised.

**4. Provision the baseline. Required, once per database:**

```sh
python3 scripts/reprovision.py; echo "exit $?"      # exit 0, and no line starting with ✗
```

It creates the fixture tables (including `routes`), their catalogue rows and the principals' tenants. Do it again after `down.sh --clean`.

**5. Start the bridge, after step 4**, in its own terminal with step 3 repeated:

```sh
zig-out/bin/bridge --pub $BRIDGE_CDC_PUBLICATION > /tmp/zb-bridge.log 2>&1
```

Redirect to a file, do not pipe into `tee` (Ctrl-C would kill `tee` too and turn the bridge's graceful stop abrupt).

**6. Run**, in another terminal with step 3 repeated:

```sh
python3 scripts/scenarios/run.py offline
python3 scripts/scenarios/run.py live --bridge-log /tmp/zb-bridge.log
# stop the bridge (Ctrl-C): each `owns` scenario starts its own and refuses to run beside another
python3 scripts/scenarios/run.py owns
```

**Through mutation admission** (the production write path): if you already ran step 5 **without admission**, first follow the migration procedure in [the compose README](../dev/README.md#the-write-path-through-admission); the same commands and credentials apply here. Otherwise, skip migration and run the commands below instead of step 5, in two terminals with step 3 repeated. Admission starts first and creates `MUTATIONS`, then the bridge starts. Running only the stack setup and reprovisioning does not require migration.

```sh
MUTATION_ADMISSION=1 zig-out/bin/bridge --mutation-ingress > /tmp/zb-admission.log 2>&1       # wait for "mutation admission ready"
MUTATION_ADMISSION=1 zig-out/bin/bridge --pub $BRIDGE_CDC_PUBLICATION > /tmp/zb-bridge.log 2>&1
```

## Stop and reset

```sh
scripts/linux/down.sh                     # stop; the data stays
scripts/linux/down.sh --clean             # also delete the data, the keys and the credentials: redo steps 2, 4 and 5
```

## Scenarios that fail until a piece is installed

These are not bugs of the stack. Each is fixed by the install line or the build named.

| scenario | needs | where |
| -- | -- | -- |
| `receipt_faults`, `recreate_recovery` 4/13/15, `walls` E | the fault-hook builds | step 1 |
| `hub_fallback` | the native endpoint probe (`Build the native pool probe first`) | step 1 |
| `outbox_*`, `reconcile`, `hub_fallback`, `jwt_renew` | the TypeScript client's dependencies (`ERR_MODULE_NOT_FOUND: better-sqlite3`) | install checklist |
| `migrate_both`, `column_flood`, `rekey_*`, `offline_migrate`, `cascade_held`, `tombstone_children`, `suspension_reasons`, `rebuild_kill` | the Node worker's dependencies (`node worker did not come up`) | install checklist |
| `serve` | a venv on Python 3.14 or newer | install checklist |
| `shared_gap` | the `zstd` command | install checklist |
| `standby` | `ZB_PGBIN` exported (it finds `pg_ctl` and `pg_basebackup` there; Ubuntu keeps them off `PATH`), and a user other than root | step 3 |
| `archive`, `readings_roles`, the S3 case of `readings_fetch` | SeaweedFS (S3 on :8333), which only the compose stack provides | not covered here |
| `leaf_security` | Docker and the image `zebridge-leaf-nats:2.15.0-security` | not covered here |

If a scenario outside this table fails, its log (`run.py` prints the path) is what to send.

## Where things are

`scripts/linux/state/` (untracked: the keys sign every credential) holds the `nsc` store, `resolver.conf`, the generated `nats-server.conf` (that of `scripts/dev` with its two container paths made local), the JetStream directory and the logs (`postgres.log`, `nats-server.log`, `jwt-bootstrap.log`).
`pg.env` there is what `scripts/linux/pg_ctl` reads. The `.creds` files go to `scripts/native/creds/`, where `run.py` reads them.

Run as root (a container, a sandbox), PostgreSQL starts as the `postgres` operating-system user, because it refuses to run as root, and its data lives in `/var/lib/zebridge-dev/pgdata` (that user cannot traverse most home directories).
As any other user it lives in `scripts/linux/state/pgdata`. `ZB_PGDATA` moves it.

## Differences from the container stack

- `up.sh` runs the credential bootstrap every time, like `scripts/dev/up.sh`, and reloads a `nats-server` that is already up (`kill -HUP`), for the same reason: the account JWT it loaded does not know the newest signing key.
- PostgreSQL 15 or newer. With several installed, `ZB_PGBIN` chooses; without it, a `pg_ctl` on `PATH` older than 15 gives way to the newest under `/usr/lib/postgresql/`.
- `wal_compression=on` instead of `zstd` (a server built without zstd refuses the latter).
- Not covered, as in `scripts/dev`: TLS and the leaf listener. Also not covered here: SeaweedFS.

## Status

Run on Debian with PostgreSQL 16 as root: `up.sh` from nothing, `down.sh --clean`, `scripts/reprovision.py`, and the scenarios that restart or reload the servers (`pg_restart`, `nats_outage`, `jwt_expiry`, `revoke_midseed`).
Run by the owner on Ubuntu with PostgreSQL 16 and Python 3.15: `offline` 9/9 and `live` 53/66 before the fault builds and the Node installs of the checklist were done. Re-run in the Debian sandbox on a fresh stack in admission mode: `reconcile` (fixed, a real regression), `outbox_recovery`, `outbox_backlog`, `jwt_renew`, `revoke_purge` and `shared_cut` (it needed a row of its own) pass. `standby` now finds its tools through `ZB_PGBIN` but cannot run as root, so it is not verified there. `serve` needs Python 3.14, which the sandbox lacks, so it is not verified either.
