# The scenario stack in containers

PostgreSQL and NATS (operator mode) in Docker Compose, started and stopped with one command. Replaces
[`scripts/native`](../native/README.md) for running the scenarios: no Homebrew PostgreSQL, no `nats-server`,
no `nsc` on the host. The bridge, the sweeper and the scenarios still run from the host build and connect over
the published ports, so a code change needs `zig build`, not an image rebuild.

Containers add a virtualization layer. Do not take speed measurements here; `scripts/native` is for those.

## Use

```sh
zig build -Doptimize=ReleaseFast        # up.sh applies the bridge's own --init-sql
BRIDGE_CDC_PUBLICATION=zb_pub scripts/dev/up.sh   # first run pulls the images and mints the NATS credentials
scripts/dev/down.sh                     # stop; the data stays in the Docker volumes
scripts/dev/down.sh --clean             # also delete the volumes, the keys and the credentials
```

Needs Docker with the compose plugin (OrbStack is fine) and `.env.bridge` with `DATABASE_READER_URL` and
`DATABASE_WRITER_URL` pointing at `127.0.0.1:5432`. The publication has no default and is not in `.env.bridge`:
pass its name, as in the first command above. Nothing else may listen on 5432,
4222, 8222, 8080 or 1883: stop a Homebrew PostgreSQL first (`brew services stop postgresql@18`, and any
`scripts/native/up.sh` stack with `scripts/native/down.sh`).

`up.sh` ends by printing the next steps. From the repository root:

```sh
# once: the scenarios' Python environment (nats-py, nkeys, msgpack, duckdb, boto3)
python3 -m venv scripts/scenarios/.venv
scripts/scenarios/.venv/bin/pip install -r scripts/scenarios/requirements.txt

# every terminal: reprovision.py and run.py are run by the venv's Python, so activate it
source scripts/scenarios/.venv/bin/activate
set -a && . ./.env.bridge && set +a && export BRIDGE_CDC_PUBLICATION=zb_pub   # the name given to up.sh
export NATS_URL=nats://127.0.0.1:4222   # after the line above, which may bring a tls:// URL: this NATS is plain

scripts/reprovision.py                  # baseline tables, catalogue rows and tenant mappings
zig-out/bin/bridge --pub $BRIDGE_CDC_PUBLICATION   # in its own terminal: the long-running bridge on :27434, for the `live` group
scripts/scenarios/run.py all            # in another terminal, same environment
```

The scenarios drive `psql` from the host (`ZB_PGBIN`, or `ZB_PSQL`: see the scenarios' README). Only the client
binaries are needed, not a running Homebrew server.

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

TLS and the leaf listener (7422) of `nats-server-jwt.conf`. The server is plain, so `NATS_URL` must be `nats://`:
the `tls://` URL that the native stack's `.env.bridge` may carry makes the bridge fail with `TlsRecordOverflow`
(the server answers the TLS handshake with the plain-text `-ERR 'Authentication Error'`, and its log shows
`authentication error`). Because of both, so the leaf scenarios (`leaf_*`, `hub_fallback`) and
anything that needs a leaf link are not served by this stack; SeaweedFS for `archive.py` and `readings_roles.py`
(`:8333`); the standby (`standby.py` starts its own server).

## Status

Written on a machine without a Docker daemon. Checked there: `docker compose config`, `jwt-bootstrap.sh` run with
`nsc`, and a `nats-server` booted on this configuration (`include` resolved, JetStream on, the minted creds
accepted, a credless connection refused). **Not run:** the containers themselves. Report what `up.sh` prints if a
step fails.
