# The scenario stack in containers

PostgreSQL, NATS (operator mode) and SeaweedFS (S3) in Docker Compose, started and stopped with one command.
It replaces [`scripts/native`](../native/README.md) for running the scenarios: no Homebrew PostgreSQL, no `nats-server`, no `nsc` on the host.
The bridge, the sweeper and the scenarios still run from the host build and connect over the published ports, so a code change needs `zig build`, not an image rebuild.
Without Docker, use [`scripts/linux`](../linux/README.md) (plain processes, with a full Ubuntu install checklist).

❗️ Containers add a virtualization layer. Do not take speed measurements here; `scripts/native` is for those.

## Install checklist

Do these once per machine, in this order. Each line says what breaks when it is missing.

- [ ] Docker with the compose plugin (OrbStack is fine). `up.sh` stops with "the Docker daemon is not reachable" otherwise.
- [ ] Free ports 5432, 4222, 8222, 8080, 1883 and 8333. Stop a Homebrew PostgreSQL (`brew services stop postgresql@18`) and any `scripts/native/up.sh` stack (`scripts/native/down.sh`).
- [ ] The Zig toolchain (0.17) and `git submodule update --init` (the `nats.zig` submodule: an empty directory makes the build stop with "unable to find nats").
- [ ] A PostgreSQL **client** on the host (`psql`, version 15 or newer) for the scenarios; no server is needed. `ZB_PGBIN` names the directory if it is not on `PATH`.
- [ ] The `zstd` command (`brew install zstd`), for `shared_gap`.
- [ ] Python 3.14 or newer, for the venv: `serve` imports `compression.zstd`. `brew install python@3.14`.
- [ ] `pnpm` and Node, then `cd zb-client-ts && pnpm install` (for `outbox_*`, `reconcile`, `hub_fallback`, `jwt_renew`) and `cd examples/04-node-consumer && pnpm install` (the Node worker of `migrate_both`, `column_flood`, `rekey_*` and others).
- [ ] The `.env.bridge` of the repository, with `DATABASE_READER_URL` and `DATABASE_WRITER_URL` pointing at `127.0.0.1:5432`.
- [ ] Optional: the leaf image for `leaf_security`: `docker build -t zebridge-leaf-nats:2.15.0-security deploy/compose/leaf`.

## Setup, in this order

Everything runs from the repository root.

**1. Build** what the stack and the scenarios run:

```sh
zig build -Doptimize=ReleaseFast
(cd libzb && zig build -Doptimize=ReleaseFast)
(cd libzb && zig build endpoint-probe)                                           # hub_fallback
zig build -Doptimize=ReleaseSafe -Dtest-faults=true -p zig-out-faults            # walls (case E)
(cd libzb && zig build -Doptimize=ReleaseSafe -Dtest-faults=true -p zig-out-faults)   # receipt_faults, recreate_recovery 4/13/15
```

**2. Start the stack.** The publication has no default and is not in `.env.bridge`: pass a name and use the same one everywhere below. The first run pulls the images and mints the NATS credentials.

```sh
BRIDGE_CDC_PUBLICATION=zb_pub scripts/dev/up.sh
```

**3. Python environment, once:**

```sh
python3.14 -m venv scripts/scenarios/.venv
scripts/scenarios/.venv/bin/pip install -r scripts/scenarios/requirements.txt
```

**4. In every terminal you use for the next steps** (the scripts are run by the venv's Python, so it must be active):

```sh
source scripts/scenarios/.venv/bin/activate
set -a && . ./.env.bridge && set +a
export BRIDGE_CDC_PUBLICATION=zb_pub               # the name given to up.sh
export NATS_URL=nats://127.0.0.1:4222              # after the line above, which may bring a tls:// URL: this NATS is plain
export ZB_PG_CTL="$PWD/scripts/dev/pg_ctl" ZB_NATS_CONTAINER=zb-dev-nats
```

The last line makes the outage scenarios (`chaos`, `nats_outage`, `matrix`, `cascade`, `churn`, `pg_restart`, `sweeper_restart`, `jwt_expiry`, `revoke_midseed`) stop, start and reload the containers instead of looking for a Homebrew server and a pid file.
`run.py` finds the S3 keys that `up.sh` generated for SeaweedFS by itself; to run `archive`, `readings_roles` or `readings_fetch` directly, add `export S3_KEYS="$PWD/scripts/dev/state/s3.json"`.

**5. Provision the baseline. Required, once per database.** `up.sh` creates an empty database; the scenarios assume the fixture tables (`test_types`, `users`, `routes`, …), their catalogue rows and the principals' tenants (`alice→acme … omar→kilo`), and `reprovision.py` creates them:

```sh
python3 scripts/reprovision.py; echo "exit $?"      # exit 0, and no line starting with ✗
```

⚠️ Do it again after `down.sh --clean` and after anything that drops the fixtures. `run.py` checks for the table and for `omar`'s tenant before it starts and stops with exit 2 if either is missing.

**6. Start the bridge, after step 5** (it publishes every schema at boot, so a bridge started before the tables existed has to be restarted). In its own terminal, ❗️ with step 4 repeated there:

```sh
zig-out/bin/bridge --pub $BRIDGE_CDC_PUBLICATION > /tmp/zb-bridge.log 2>&1     # :27434
```

Watch it with `tail -f /tmp/zb-bridge.log` in another terminal.
**Do not pipe the bridge into `tee`**: Ctrl-C goes to every program of a pipeline, so it also kills `tee`, and the bridge's graceful stop (it waits for its in-flight batches) turns abrupt. Redirect to a file instead; Ctrl-C then reaches only the bridge.

**7. Run**, in another terminal ❗️ with step 4 repeated:

```sh
python3 scripts/scenarios/run.py offline
python3 scripts/scenarios/run.py live --bridge-log /tmp/zb-bridge.log
# stop the bridge (Ctrl-C): each `owns` scenario starts its own and refuses to run beside another
python3 scripts/scenarios/run.py owns
```

`run.py all` is these three in a row, so it cannot work with the long-running bridge up: it says so and skips `owns`. `run.py --list` shows every scenario with what it does.

## The write path through admission

`MUTATION_ADMISSION` defaults to 0: the long-running bridge reads writes from the legacy public queue, and everything above runs that way.
Production runs `bridge --mutation-ingress` as a separate process and sets `MUTATION_ADMISSION=1` on the bridge (OPERATIONS, "Mutation admission cutover").
To run the `live` group through it, start admission first and the bridge after it, on a stack whose bridge has never started, because the first start decides the queue's layout.
Both processes need step 4 repeated, each in its own terminal:

```sh
MUTATION_ADMISSION=1 zig-out/bin/bridge --mutation-ingress > /tmp/zb-admission.log 2>&1       # wait for "mutation admission ready"
MUTATION_ADMISSION=1 zig-out/bin/bridge --pub $BRIDGE_CDC_PUBLICATION > /tmp/zb-bridge.log 2>&1
```

It is a switch for the whole stack, not per scenario: the queue becomes `$ZB.mutation.>`, and an `owns` scenario that publishes straight to `mutation.<principal>.…` with no admission process running gets `NoStreamResponseError` (`ratelimit` did).
Go back with `scripts/dev/down.sh --clean` and the setup steps.
Two rules of the admission path matter to scenario authors: a publisher's reply inbox must be `_INBOX.<principal>.…` (otherwise the publisher only sees a timeout), and the first write after a quiet spell is answered a few seconds later.
Measured on the Linux stack: `mutate`, `replies`, `offline`, `writable` and `invalidate` pass this way.
Admission itself is covered by `mutation_admission`, `hub_fallback`, `outage_matrix` and `receipt_faults`, which build their own stacks; this mode is for the scenarios that write through the shared one.

**Open:** with admission on, `reconcile` ends with `a write that never committed is answered indeterminate, then reconciled (['indeterminate'])` on this stack and on the Linux one. The cause is not found yet; it is the one admission-mode failure that is not explained.

## Stop and reset

```sh
scripts/dev/down.sh                     # stop; the data stays in the Docker volumes
scripts/dev/down.sh --clean             # also delete the volumes, the keys and the credentials: redo steps 2, 5 and 6
```

## Things to know

**After `owns`, the bridge's replication slot is lost.** `slot_loss.py` lowers `max_slot_wal_keep_size`, which is cluster-wide, so every inactive slot that is behind is invalidated, including the long-running bridge's (it is stopped while `owns` runs).
Starting that bridge again then stops at once with `SlotInvalidated`. Recover once: `bridge --drop-slot <slot>` (or `SELECT pg_drop_replication_slot('<slot>')`), run `scripts/reprovision.py`, and start the bridge with `ZB_FEED_RESTART=1`; it creates a new slot, and the next start needs no flag.

**The GC floor never goes down.** `zebridge_gc_watermark` is the oldest version the bridge still accepts.
If anything moves it close to now, every scenario that writes "a few minutes ago" (`mutate`, `replies`, `offline`, `intclamp`) fails with `PredatesGcWatermark`, and nothing short of resetting the database (`scripts/dev/down.sh --clean`) undoes it.
`run.py` warns at the start when the floor is less than 45 minutes behind now (`offline` stamps writes up to 40 minutes back), and prints `GC floor moved: …` after the scenario that moved it.

**Reading the bridge's errors.** The long-running bridge logs one stream for every `live` scenario, without timestamps, and many scenarios provoke errors on purpose (a refused write, a killed process, a dropped row, an outage).
With `--bridge-log`, `run.py` saves what the bridge wrote during each scenario to `zb-scenario-<name>.bridge.log` beside that scenario's log and prints how many error lines it holds.
An error there is expected if the scenario's own description says it causes one; it is worth a look if the scenario passed without meaning to. The `owns` scenarios log their own probe bridge in `/tmp/zb_*.log`, named in each scenario's output.

The scenarios drive `psql` from the host (`ZB_PGBIN`, or `ZB_PSQL`: see the scenarios' README). Only the client binaries are needed, not a running server.

## When a scenario fails to start

| symptom | cause | remedy |
| -- | -- | -- |
| `NoCredentialsError: Unable to locate credentials` (`archive`, `readings_roles`, `readings_fetch`) | the S3 keys file is missing, or the scenario was run directly without `S3_KEYS` | `ls scripts/dev/state/s3.json`; if absent, run `up.sh` again (it generates it and starts SeaweedFS; `ZB_DEV_S3=0` leaves it out); for a direct run `export S3_KEYS="$PWD/scripts/dev/state/s3.json"` |
| `ERR_MODULE_NOT_FOUND: better-sqlite3` (`outbox_*`, `reconcile`, `hub_fallback`, `jwt_renew`) | the TypeScript client's dependencies are not installed | `cd zb-client-ts && pnpm install` |
| `node worker did not come up` | the Node worker cannot resolve `@zebridge/client` (a `link:` dependency of an example outside the workspace) | `cd examples/04-node-consumer && pnpm install` |
| `from compression import zstd` fails (`serve`) | the venv is Python older than 3.14 | recreate it with `python3.14` |
| `Build the native pool probe first` (`hub_fallback`) | the probe is not built | `cd libzb && zig build endpoint-probe` |
| a fault-hook scenario is refused (`receipt_faults`, `recreate_recovery` 4/13/15, `walls` E) | the ordinary build carries no hooks | build with `-Dtest-faults=true -p zig-out-faults` (step 1) |
| `invalid value for parameter "lc_messages": "en_US.utf8"` (`standby`) | the database volume was made before the container used the `C` locale | `scripts/dev/down.sh --clean`, then steps 2, 5, 6 |
| `AuthorizationViolation` after a manual bootstrap | the running NATS has not loaded the newest signing key | `docker kill -s HUP zb-dev-nats` (`up.sh` does it) |
| `not in the nsc store` (`jwt_expiry`, `revoke_midseed`) | a stack bootstrapped before 2026-10-28 kept its private keys inside the bootstrap container | while `zb-dev-nats-bootstrap` exists: `docker cp zb-dev-nats-bootstrap:/nsc/nkeys/. scripts/dev/state/nsc-store/data/nats/nsc/keys/`; otherwise `down.sh --clean` and the setup steps |
| `TlsRecordOverflow` | `NATS_URL` is `tls://` (from `.env.bridge`), this NATS is plain | `export NATS_URL=nats://127.0.0.1:4222` after loading the env file |

## What runs

| service | image | host ports | what |
| -- | -- | -- | -- |
| `postgres` | `postgres:18.6-alpine3.24` | 5432 | the flags of `scripts/native/up.sh` (`wal_level=logical`, ten slots); `trust` for the host, loopback only |
| `nats-bootstrap` | `natsio/nats-box` | none | one shot: runs [`jwt-bootstrap.sh`](../native/jwt-bootstrap.sh) with `nsc`, then exits |
| `nats` | `nats:2.15-alpine` | 4222, 8222 (monitoring), 8080 (websocket), 1883 (MQTT) | [`nats-server.conf`](nats-server.conf), after the bootstrap |
| `seaweedfs` | `chrislusf/seaweedfs` | 8333 | the S3 endpoint of `archive`, `readings_roles` and `readings_fetch`; one identity generated by `up.sh` into `state/s3.json` (profile `s3`, on by default) |

The bootstrap writes the `nsc` keys and the resolver fragment to `scripts/dev/state/` (untracked: the keys sign every credential) and the `.creds` files to `scripts/native/creds/`, where `run.py` reads them.
The server includes `state/resolver.conf` instead of a pasted copy of the account JWT, so after a signing-key edit a restart is enough.
The bridge creates its own streams and KV buckets at boot; nothing here does.

## How the outage scenarios drive the containers

`scripts/dev/pg_ctl` answers `status`, `stop -m fast|immediate` and `start` with `docker stop`, `docker kill -s QUIT` and `docker start`, and waits for `pg_isready`.
The data stays in the volume and the flags are the container's own, so `ZB_PGDATA` and `ZB_PG_LOG` are ignored (the log is `docker logs zb-dev-postgres`).
With `ZB_NATS_CONTAINER` set the broker is stopped with `docker stop`, started with `docker start` and reloaded with `docker kill -s HUP`.
`jwt_expiry` and `revoke_midseed` find the keys of the store that `jwt-bootstrap.sh` minted by reading `state/resolver.conf` (the operator's JWT and the account's, whose signing keys name the client key), and amend that file, which the container includes.
Unset, every one of these scenarios behaves as before, against the native stack.

`up.sh` re-runs the bootstrap every time, which adds a signing key and rewrites `state/resolver.conf`, and then reloads the running NATS so that it knows the newest key.
Without that, `GET /enroll` signs with a key the server has not loaded and the enrolled client is refused with `AuthorizationViolation`.

## Not covered

TLS and the leaf listener (7422) of `nats-server-jwt.conf`: the server here is plain.
The leaf scenarios (`leaf_*`, `hub_fallback`) therefore do not use this stack's NATS: they start their own hub and leaf on the host (they need `nats-server` there), and `leaf_security` runs containers itself from the leaf image of the checklist.
The standby (`standby.py`) starts its own server too.

## Status

The Compose stack is run by the owner on a Mac (OrbStack): `offline`, `live` and `owns` pass except the items under "Open" above and the items that need the extra builds of the checklist.
The sandbox this file was last edited in has no Docker daemon, so the containers themselves are not exercised there; the same scenarios run against [`scripts/linux`](../linux/README.md) instead.
