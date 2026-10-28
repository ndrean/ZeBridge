# The scenarios' stack on Linux, without Docker

The same stack as [`scripts/dev`](../dev/README.md) (PostgreSQL with logical replication, NATS in operator mode, credentials
minted by `nsc`, the bridge's init SQL), as plain processes. For a sandbox, a CI runner or a workstation where containers are not
an option. On a Mac use `scripts/dev` (containers) or `scripts/native` (Homebrew, for speed measurements).

```sh
scripts/linux/up.sh                       # BRIDGE_CDC_PUBLICATION=zb_pub scripts/linux/up.sh if .env.bridge does not name it
scripts/linux/down.sh                     # stop; the data stays
scripts/linux/down.sh --clean             # also delete the data, the keys and the credentials
```

After `up.sh` the steps are those of [`scripts/dev/README.md`](../dev/README.md) from step 3 (the Python environment, the
fixtures with `scripts/reprovision.py`, the long-running bridge, the groups of `run.py`). Only the exports of step 4 differ, and
`up.sh` prints them with the right values:

```sh
export NATS_URL=nats://127.0.0.1:4222
export ZB_PGBIN=/usr/lib/postgresql/16/bin ZB_PG_CTL=$PWD/scripts/linux/pg_ctl ZB_JWT_STATE=$PWD/scripts/linux/state
```

No `ZB_NATS_CONTAINER`: here the scenarios that restart NATS signal the `nats-server` process (its pid is in
`scripts/native/nats-server.pid`, which `up.sh` writes), and `ZB_PG_CTL` is a wrapper that stops and starts the cluster
`up.sh` initialised.

## Needs

| need | for | how |
| -- | -- | -- |
| PostgreSQL server binaries (`initdb`, `pg_ctl`, `psql`; 16 and 18 are used) | the database | the `postgresql` package; `ZB_PGBIN` names the directory when it is not `/usr/lib/postgresql/*/bin` or on `PATH` |
| `nats-server` 2.15 or newer | the broker (2.11 lacks `default_max_consumers`) | [releases](https://github.com/nats-io/nats-server/releases) |
| `nats` and `nsc` | the scenarios, and the credentials | [natscli](https://github.com/nats-io/natscli/releases), [nsc](https://github.com/nats-io/nsc/releases) |
| the bridge, built | `--init-sql` is what is applied | `zig build -Doptimize=ReleaseFast` |
| `curl`, `bash` | `up.sh` | |

Run as root (a container, a sandbox) PostgreSQL starts as the `postgres` operating-system user, because it refuses to run as
root, and its data lives in `/var/lib/zebridge-dev/pgdata` (that user cannot traverse most home directories). As any other user
it lives in `scripts/linux/state/pgdata`. `ZB_PGDATA` moves it.

## Where things are

`scripts/linux/state/` (untracked: the keys sign every credential) holds the `nsc` store, `resolver.conf`, the generated
`nats-server.conf` (that of `scripts/dev` with its two container paths made local), the JetStream directory and the logs
(`postgres.log`, `nats-server.log`, `jwt-bootstrap.log`). `pg.env` there is what `scripts/linux/pg_ctl` reads. The `.creds`
files go to `scripts/native/creds/`, where `run.py` reads them.

## Differences from the container stack

- `up.sh` runs the credential bootstrap every time, like `scripts/dev/up.sh`, and reloads a `nats-server` that is already up
  (`kill -HUP`), for the same reason: the account JWT it loaded does not know the newest signing key.
- `wal_compression=on` instead of `zstd` (a server built without zstd refuses the latter).
- Not covered, as in `scripts/dev`: TLS and the leaf listener, SeaweedFS. The write path through admission works the same way
  (the section of `scripts/dev/README.md` about it; measured on this stack).

## Status

Run on Debian with PostgreSQL 16 as root: `up.sh` from nothing, `down.sh --clean`, `scripts/reprovision.py`, and the scenarios
that restart or reload the servers (`pg_restart`, `nats_outage`, `jwt_expiry`, `revoke_midseed`).
