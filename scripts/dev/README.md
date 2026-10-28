# The scenario stack in containers

PostgreSQL and NATS (operator mode) in Docker Compose, started and stopped with one command. Replaces
[`scripts/native`](../native/README.md) for running the scenarios: no Homebrew PostgreSQL, no `nats-server`,
no `nsc` on the host. The bridge, the sweeper and the scenarios still run from the host build and connect over
the published ports, so a code change needs `zig build`, not an image rebuild.

Containers add a virtualization layer. Do not take speed measurements here; `scripts/native` is for those.

## Setup, in this order

Everything runs from the repository root. Needs Docker with the compose plugin (OrbStack is fine) and the
`.env.bridge` of the repository, with `DATABASE_READER_URL` and `DATABASE_WRITER_URL` pointing at `127.0.0.1:5432`.
Nothing else may listen on 5432, 4222, 8222, 8080 or 1883: stop a Homebrew PostgreSQL first
(`brew services stop postgresql@18`, and any `scripts/native/up.sh` stack with `scripts/native/down.sh`).
The one-time installs of [the table below](#once-before-the-scenarios) come first.

**1. Build** what the stack and the scenarios run:

```sh
zig build -Doptimize=ReleaseFast
(cd libzb && zig build -Doptimize=ReleaseFast)
```

**2. Start the stack.** The publication has no default and is not in `.env.bridge`: pass a name and use the same one
everywhere below. The first run pulls the images and mints the NATS credentials.

```sh
BRIDGE_CDC_PUBLICATION=zb_pub scripts/dev/up.sh
```

**3. Python environment, once:**

```sh
python3 -m venv scripts/scenarios/.venv
scripts/scenarios/.venv/bin/pip install -r scripts/scenarios/requirements.txt
```

**4. In every terminal you use for the next steps** (they are run by the venv's Python, so it must be active):

```sh
source scripts/scenarios/.venv/bin/activate
set -a && . ./.env.bridge && set +a
export BRIDGE_CDC_PUBLICATION=zb_pub               # the name given to up.sh
export NATS_URL=nats://127.0.0.1:4222              # after the line above, which may bring a tls:// URL: this NATS is plain
# so the outage scenarios (chaos, nats_outage, matrix, cascade, churn, pg_restart, sweeper_restart, jwt_expiry,
# revoke_midseed) stop, start and reload the containers instead of looking for a Homebrew server and a pid file:
export ZB_PG_CTL="$PWD/scripts/dev/pg_ctl" ZB_NATS_CONTAINER=zb-dev-nats
```

**5. Provision the baseline. Required, once per database.** `up.sh` creates an empty database; the scenarios assume
the eight fixture tables (`test_types`, `users`, …), their catalogue rows and the principals' tenants
(`alice→acme … omar→kilo`), and `reprovision.py` is what creates them:

```sh
scripts/reprovision.py; echo "exit $?"      # exit 0, and no line starting with ✗
```

Without it the scenarios fail in dozens of places that do not name the cause (`relation public.test_types does not
exist`, `no tenant for a write … principal 'omar' is unset`). Do it again after `down.sh --clean`, and after anything
that drops the fixtures. `run.py` checks for the table and for `omar`'s tenant before it starts, and stops with
exit 2 if either is missing.

**6. Start the bridge, after step 5** (it publishes every schema at boot, so a bridge started before the tables
existed has to be restarted). Its own terminal, steps 4 repeated there:

```sh
zig-out/bin/bridge --pub $BRIDGE_CDC_PUBLICATION > /tmp/zb-bridge.log 2>&1     # :27434
```

Watch it with `tail -f /tmp/zb-bridge.log` in another terminal. **Do not pipe the bridge into `tee`**: Ctrl-C goes to every
program of a pipeline, so it also kills `tee`, and the bridge's graceful stop (it waits for its in-flight batches) turns
into an abrupt one. Redirect to a file instead, as above; Ctrl-C then reaches only the bridge.

**7. Run**, in another terminal with step 4 repeated:

```sh
scripts/scenarios/run.py offline
scripts/scenarios/run.py live --bridge-log /tmp/zb-bridge.log
# stop the bridge (Ctrl-C): each `owns` scenario starts its own and refuses to run beside another
scripts/scenarios/run.py owns
```

`run.py all` is these three in a row, so it cannot work with the long-running bridge up: it says so and skips `owns`.

**Stop and reset:**

```sh
scripts/dev/down.sh                     # stop; the data stays in the Docker volumes
scripts/dev/down.sh --clean             # also delete the volumes, the keys and the credentials: redo steps 2, 5 and 6
```

**After `owns`, the bridge's replication slot is lost.** `slot_loss.py` lowers `max_slot_wal_keep_size`, which is
cluster-wide, so every inactive slot that is behind is invalidated, including the long-running bridge's (it is stopped
while `owns` runs). Starting that bridge again then stops at once with `SlotInvalidated`. Recover once:
`bridge --drop-slot <slot>` (or `SELECT pg_drop_replication_slot('<slot>')`), run `scripts/reprovision.py`, and start the
bridge with `ZB_FEED_RESTART=1`; it creates a new slot, and the next start needs no flag.

**The GC floor never goes down.** `zebridge_gc_watermark` is the oldest version the bridge still accepts. If anything
moves it close to now, every scenario that writes "a few minutes ago" (`mutate`, `replies`, `offline`, `intclamp`)
fails with `PredatesGcWatermark`, and nothing short of resetting the database (`scripts/dev/down.sh --clean`) undoes it.
`run.py` warns at the start when the floor is less than 45 minutes behind now (`offline` stamps writes up to 40 minutes back), and prints `GC floor moved: …` after the
scenario that moved it.

**Reading the bridge's errors.** The long-running bridge logs one stream for every `live` scenario, without timestamps, and
many scenarios provoke errors on purpose (a refused write, a killed process, a dropped row, an outage). With
`--bridge-log`, `run.py` saves what the bridge wrote during each scenario to `zb-scenario-<name>.bridge.log` beside
that scenario's log and prints how many error lines it holds. An error there is expected if the scenario's own
description says it causes one (`run.py --list`); it is worth a look if the scenario passed without meaning to.
The `owns` scenarios log their own probe bridge in `/tmp/zb_*.log`, named in each scenario's output.

The scenarios drive `psql` from the host (`ZB_PGBIN`, or `ZB_PSQL`: see the scenarios' README). Only the client
binaries are needed, not a running Homebrew server.

## Once, before the scenarios

Found by running the whole battery on a clean machine; none of it is in the scenarios' own setup text.

| need | for | how |
| -- | -- | -- |
| the TypeScript client's dependencies | `outbox_*`, `reconcile`, `hub_fallback`, `jwt_renew` (`ERR_MODULE_NOT_FOUND: better-sqlite3`) | `cd zb-client-ts && pnpm install` |
| the Node worker's dependencies | `migrate_both`, `column_flood`, `rekey_*`, `offline_migrate`, `cascade_held`, `tombstone_children`, `suspension_reasons`, `rebuild_kill` (`node worker did not come up`: it cannot resolve `@zebridge/client`, a `link:` dependency of an example outside the workspace) | `cd examples/04-node-consumer && pnpm install` |
| the `zstd` command | `shared_gap` | `brew install zstd` |
| the demo `routes` table | `route_crdt` (`no such table: routes`) | `python examples/08-map/load_routes.py --create` (set `ZB_PSQL` if your `psql` is not Homebrew's) |
| Python 3.14 for the venv | `serve` (`from compression import zstd`) | `brew install python@3.14`, then `python3.14 -m venv …` |
| SeaweedFS S3 on `:8333` | `readings_roles`, `archive`, the S3 case of `readings_fetch` | start it yourself |
| the `zebridge-leaf-nats:2.15.0-security` image | `leaf_security` | no documented command builds it |
| a database created with the `C` locale | `standby` (`invalid value for parameter "lc_messages": "en_US.utf8"`: it starts a copy of the container's database with your Mac's PostgreSQL, and the container's default locale name is Linux's) | `compose.yml` now creates it that way; a volume made before needs `scripts/dev/down.sh --clean`, then steps 2, 5, 6 again |

## What runs

| service | image | host ports | what |
| -- | -- | -- | -- |
| `postgres` | `postgres:18.6-alpine3.24` | 5432 | the flags of `scripts/native/up.sh` (`wal_level=logical`, ten slots); `trust` for the host, loopback only |
| `nats-bootstrap` | `natsio/nats-box` | none | one shot: runs [`jwt-bootstrap.sh`](../native/jwt-bootstrap.sh) with `nsc`, then exits |
| `nats` | `nats:2.15-alpine` | 4222, 8222 (monitoring), 8080 (websocket), 1883 (MQTT) | [`nats-server.conf`](nats-server.conf), after the bootstrap |

The bootstrap writes the `nsc` keys and the resolver fragment to `scripts/dev/state/` (untracked: the keys sign
every credential) and the `.creds` files to `scripts/native/creds/`, where `run.py` reads them. The server
includes `state/resolver.conf` instead of a pasted copy of the account JWT, so after a signing-key edit a restart
is enough. The bridge creates its own streams and KV buckets at boot; nothing here does.

## Not covered

**Restarting the containers.** `scripts/dev/pg_ctl` answers `status`, `stop -m fast|immediate` and `start` with
`docker stop`, `docker kill -s QUIT` and `docker start`, and waits for `pg_isready`; the data stays in the volume, the
flags are the container's own (so `ZB_PGDATA` and `ZB_PG_LOG` are ignored; the log is `docker logs zb-dev-postgres`).
With `ZB_NATS_CONTAINER` set the broker is stopped with `docker stop`, started with `docker start` and reloaded with
`docker kill -s HUP`. `jwt_expiry` and `revoke_midseed` find the keys of the store that `jwt-bootstrap.sh` minted
by reading `state/resolver.conf` (the operator's JWT and the account's, whose signing keys name the client key), and
amend that file, which the container includes.
Unset, every one of these scenarios behaves as before, against the native stack.

`up.sh` re-runs the bootstrap every time, which adds a signing key and rewrites `state/resolver.conf`, and then reloads the
running NATS (`docker kill -s HUP zb-dev-nats`) so that it knows the newest key. Without that, `GET /enroll` signs with a key the
server has not loaded and the enrolled client is refused with `AuthorizationViolation`; after a manual bootstrap run the same
command does it.

A stack bootstrapped before 2026-10-28 has no private keys in `state/` (`nsc` wrote them inside the bootstrap container, because
the `nats-box` image presets `NKEYS_PATH`): `jwt_expiry` and `revoke_midseed` stop with "not in the nsc store". While
`zb-dev-nats-bootstrap` still exists, copy them out:
`docker cp zb-dev-nats-bootstrap:/nsc/nkeys/. scripts/dev/state/nsc-store/data/nats/nsc/keys/`; otherwise
`down.sh --clean` and the setup steps again.

TLS and the leaf listener (7422) of `nats-server-jwt.conf`. The server is plain, so `NATS_URL` must be `nats://`:
the `tls://` URL that the native stack's `.env.bridge` may carry makes the bridge fail with `TlsRecordOverflow`
(the server answers the TLS handshake with the plain-text `-ERR 'Authentication Error'`, and its log shows
`authentication error`). Because of both, so the leaf scenarios (`leaf_*`, `hub_fallback`) and
anything that needs a leaf link are not served by this stack; SeaweedFS for `archive.py` and `readings_roles.py`
(`:8333`); the standby (`standby.py` starts its own server).

## Status

Written on a machine without a Docker daemon. `pg_ctl` was run against a stand-in for `docker`; the NATS restarts and the
JWT scenarios on the containers have not run. Checked there: `docker compose config`, `jwt-bootstrap.sh` run with
`nsc`, and a `nats-server` booted on this configuration (`include` resolved, JetStream on, the minted creds
accepted, a credless connection refused). **Not run:** the containers themselves. Report what `up.sh` prints if a
step fails.
