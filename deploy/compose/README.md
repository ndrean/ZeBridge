# ZeBridge with Docker Compose

One server runs NATS, the bridge, the sweeper, HAProxy and Let's Encrypt, Prometheus and Grafana, and optionally PostgreSQL. Two files are all it needs, `compose.yml` and a `.env`: HAProxy's, Prometheus's and Grafana's configuration ships inside the release's image (`ghcr.io/ndrean/zebridge`), and follows `ZB_VERSION`.

| name | port | what |
| -- | -- | -- |
| `BRIDGE_HOST` | 443 | HAProxy → the bridge: `GET /enroll`, `/renew`, `/status`, rate-limited |
| `WS_HOST` | 443 | HAProxy → the NATS websocket, for browsers |
| `NATS_HOST` | `NATS_PUBLIC_PORT` | NATS with TLS, for native clients (phones, libzb) |
| (any) | 80 | Let's Encrypt's challenge; everything else is redirected to https |
| Grafana | 127.0.0.1:3000 | the server's loopback only: an SSH tunnel reaches it |

PostgreSQL, Prometheus, NATS monitoring and the bridge's `/metrics` are not published.

## Setting up

Docker Engine, the standard (rootful) daemon, with the compose plugin. Rootless Docker does not fit: it cannot publish ports 80 and 443, and its port forwarding hides the clients' addresses, so HAProxy's per-address limits would count every client as one.

1. Three DNS records (A, and AAAA if the server has IPv6) pointing at the server. With Cloudflare, "DNS only": the certificates are issued on port 80, and native clients reach NATS directly.
2. The two files, on the server:

   ```sh
   V=0.2.7
   mkdir zebridge && cd zebridge
   curl -fsSLO https://raw.githubusercontent.com/ndrean/ZeBridge/v$V/deploy/compose/compose.yml
   curl -fsSL -o .env https://raw.githubusercontent.com/ndrean/ZeBridge/v$V/deploy/compose/.env.example
   chmod 600 .env
   ```

   In `.env`: the three domain names, the ACME e-mail, and `ZB_VERSION`, and COMPOSE_PROFILES set to 'postgres' for a local server, commented otherwise (compose will not run PostgreSQL)

   ```txt
   # .env
   COMPOSE_PROFILES=postgres  # <-- local Postgres server on
   ZB_VERSION=0.2.6
   BRIDGE_HOST=               #<-- bridge.example.com
   WS_HOST=                   #<-- ws.example.com
   NATS_HOST=                 #<-- nats.example.com
   NATS_PUBLIC_PORT=4222

   ACME_EMAIL=                #<-- your@email

   PG_ADMIN_PASSWORD=
   ZB_READER_PASSWORD=
   ZB_WRITER_PASSWORD=
   ZB_INVITER_PASSWORD=
   GRAFANA_ADMIN_PASSWORD=

   BRIDGE_CDC_PUBLICATION=zb_pub
   BRIDGE_CDC_SLOT=zb_slot
   ```

   The passwords (letters and digits only), made on the server and written straight into `.env`, never shown:

   ```sh
   for v in PG_ADMIN_PASSWORD ZB_READER_PASSWORD ZB_WRITER_PASSWORD ZB_INVITER_PASSWORD GRAFANA_ADMIN_PASSWORD; do
     sed -i "s|^$v=.*|$v=$(openssl rand -hex 24)|" .env
   done
   ```

   With an external PostgreSQL instead of the one in this stack: remove the `COMPOSE_PROFILES=postgres` line, set `DATABASE_READER_URL` and `DATABASE_WRITER_URL` (the roles are created with the names and passwords in them), and see [External PostgreSQL](#external-postgresql) before starting.
3. Start:

   ```sh
   docker compose up -d
   ```

   The first start issues the certificate, generates the NATS operator and accounts, and sets up the database: roles, functions, publication, and `zb_inviter`.
4. Check:

   ```sh
   docker compose ps -a
   docker compose exec bridge sh -c 'set -a; . /etc/zebridge/.env.nats; set +a; bridge --diagnose'
   curl https://$BRIDGE_HOST/status
   docker compose exec certbot certbot renew --dry-run --http-01-port 8888 --no-random-sleep-on-renew
   ```

   `ps -a`: `conf-init`, `nats-init` and `pg-init` exited with 0, the rest are up. Until Let's Encrypt's certificate arrives (seconds, once the names resolve), HAProxy and NATS serve a temporary self-signed one; `docker compose logs certbot` says which. certbot shown as `Restarting`: its log names the setting to fix (an `ACME_EMAIL` that is not an address). `--diagnose` needs the NATS settings that the bridge's own start loads. The renewal dry run checks that Let's Encrypt reaches the certbot container through HAProxy; without `--no-random-sleep-on-renew` it first waits up to 8 minutes, silently.

5. Copy `operator.store` off the server: it holds the root seeds, which only `--init-nats --update`, `--mint-leaf` and `--mint-responder` need.

   ```sh
   docker compose run --rm --no-deps --entrypoint cat nats-init /etc/zebridge/operator.store > operator.store
   ```

## Invites

A device enrolls with a one-time invite. `zb_inviter` is the login that creates them, and it may do nothing else: it cannot read, spend or delete an invite.

```sh
export INVITER_DATABASE_URL=postgres://zb_inviter:<ZB_INVITER_PASSWORD>@postgres:5432/zebridge
docker compose exec -e INVITER_DATABASE_URL bridge bridge --invite --principal alice --tenant acme --expires '1 hour'
```

It prints the code alone. A backend on this server runs the same command after its own sign-in; a backend elsewhere calls `SELECT zebridge_invite('alice', 'acme', interval '1 hour')` over a connection as `zb_inviter`, which needs a PostgreSQL it can reach (this stack's is not published).

## Metrics

Prometheus scrapes the bridge and NATS (through the NATS exporter) every 15 s and keeps 30 days or 2 GB, whichever comes first (`PROMETHEUS_RETENTION_TIME`, `_SIZE`). Grafana shows them with two dashboards, the bridge's and NATS's. From your machine:

```sh
ssh -L 3000:127.0.0.1:3000 <user>@<server>
```

then <http://localhost:3000>, user `admin`, the password `GRAFANA_ADMIN_PASSWORD` from `.env`.

## Sensor readings and their archive

Sensors publish readings to NATS, the bridge keeps them in one stream per tenant, and two services per tenant take it from there: an **archiver** writes them to Parquet files in an S3 bucket, and a **readings responder** answers questions over the files and the recent readings. They are in a second file, `compose.readings.yml`, with one tenant (`acme`) to copy and rename:

```sh
curl -fsSLO https://raw.githubusercontent.com/ndrean/ZeBridge/v$V/deploy/compose/compose.readings.yml
```

In `.env`, uncomment `COMPOSE_FILE`, `S3_ENDPOINT` and `S3_BUCKET`. Then, per tenant, next to `compose.yml`:

| file | holds | from |
| -- | -- | -- |
| `creds/archiver-acme.creds` | the archiver's NATS identity | `bridge --mint-archiver --name archiver-acme --tenant acme` |
| `creds/readings-acme.creds` | the responder's | `bridge --mint-responder --name readings-acme --tenant acme` |
| `readings-acme-write.env` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | a key that can write the bucket |
| `readings-acme-read.env` | the same two names | a key that can only read it |

The mints need `operator.store`: run them on the machine that holds it (`--store <path>`) and copy the `.creds` here. The containers read the `.creds` as uid 10001; compose reads the `.env` files itself, as whoever runs it:

```sh
sudo chown 10001:10001 creds/*.creds && sudo chmod 600 creds/*.creds
chmod 600 readings-*.env
```

`docker compose up -d` starts them. The tenant's stream appears with its first row in `zebridge_user_tenants`, and both services wait for it.

## External PostgreSQL

`pg-init` sets up the database with the admin's URL, which this stack never stores. Run it once before the first start, and again after each upgrade:

```sh
read -rs ADMIN_DATABASE_URL && export ADMIN_DATABASE_URL     # paste it: nothing is shown or kept
docker compose run --rm --no-deps -e ADMIN_DATABASE_URL pg-init
unset ADMIN_DATABASE_URL
```

The database needs `wal_level = logical` (most managed services have a switch for it).

## What runs, when, and where it lives

| service | does | when | kept in |
| -- | -- | -- | -- |
| `conf-init` | copies the configuration out of the image | every start | volume `zb-conf` |
| `nats-init` | `bridge --init-nats operator` | once | volume `zb-etc`: conf, `.env.nats`, creds, JetStream data |
| `pg-init` | `bridge --init-sql`, then `zb_inviter` | every start: the SQL is idempotent, so an upgrade brings its new functions and stored data stays | the database |

⚠️ Never delete the `zb-etc` volume (`docker compose down -v` does): a new operator would refuse every enrolled device. Back it up with the database.

Certificates: `certbot` gets the first one and renews it twice a day, both through HAProxy, which alone holds port 80; NATS and HAProxy reload a new certificate without dropping connections. Kept in volume `letsencrypt`, copies in `certs`.

## Upgrading

`compose.yml` and the image go together: the image carries the scripts `compose.yml` names, so take both from the new release. `.env` stays (compare it with the release's `.env.example` for new settings):

```sh
V=<new release>
curl -fsSLO https://raw.githubusercontent.com/ndrean/ZeBridge/v$V/deploy/compose/compose.yml
sed -i "s|^ZB_VERSION=.*|ZB_VERSION=$V|" .env
docker compose up -d --remove-orphans
```

The images, the configuration and the database's functions follow, and a service the release retired is removed. With `compose.readings.yml`, download the release's too. With an external PostgreSQL, run `pg-init` as above too.

## Firewall

Docker's published ports bypass ufw: 80, 443 and `NATS_PUBLIC_PORT` are open to the internet whatever ufw says, and nothing else is published (Grafana listens on the server's loopback). A firewall in front of the server (the provider's network firewall) does apply: allow 22, 80, 443 and `NATS_PUBLIC_PORT`.

## A leaf node

See [Leaf nodes](../../LEAF_NODES.md) for the enforced tenant and identity model. Build the hub bridge image from these sources first; the published `0.2.6` image does not contain mutation admission or signed trust refresh. The admission health check and bridge startup check deliberately refuse an incompatible image or legacy queue. Run the [queue cutover](../../OPERATIONS.md#mutation-admission-cutover) before starting the updated workers against an existing deployment.

1. Apply current init SQL and register the leaf's explicit tenant list and URLs in `zebridge_leaf_scopes`. Stage the updated libzb/TS clients. Leaf devices and responders use `<leaf>.<device>` identities; existing root identities continue on the hub.
2. Mint `leaf.creds` with `--mint-leaf --name lf-leaf2 --tenant demo` and `--mirror` if seeding. Mint a separate `--keeper` with **the same name and tenants** for seeding. Keep both files 0600. Extract the public operator/resolver trust block from the hub configuration into `trust.conf`; keep `operator.store` offline.
3. Copy the **whole** `deploy/compose/leaf/` directory from this checkout to the leaf, including `.dockerignore`, `Dockerfile`, `leaf_trust.py` and `mirrors.sh`. Copy `.env.example` to `.env` and install trust/credentials under `hub/`. Do not download only an older release's compose file.
4. Configure `LEAF_NAME`, `LEAF_TENANTS` (required space-separated list), `LEAF_HOST`, `HUB_NATS_HOST`, `TRUST_FEED_URL=https://bridge.<domain>/nats-account`, email and allowed web origins. The list must match the PostgreSQL registry and minted credentials. Set a distinct `LEAF_JS_DOMAIN` for seeding, or leave the domain empty for forwarding. Leave the confirmed `TRUST_MAX_STALE_SECONDS=300` default.
5. On the hub, enable `NATS_ACCOUNT_CONF` and expose `/nats-account` through the provided HAProxy rule. Verify HTTPS returns only `account` and `jwt`. Keep the hub leaf listener restricted to the leaf IPs. The hub must have a JetStream domain and a TLS leaf listener; the root Compose stack does not create that listener automatically.
6. Run `docker compose build nats`, then `docker compose up -d --force-recreate`. Check `docker compose logs nats mirrors` and `docker compose exec nats wget -qO- http://127.0.0.1:8222/leafz`. Clients dial the regional TLS/WSS URL, with `jsDomain: "hub"` and the seeding domain in `seedDomain`.

The custom NATS image adds Python and PyNaCl for signed account refresh. Its persistent `trust-state` volume preserves revocations and the verified lease. A missing or invalid feed pauses NATS after five minutes and recovers automatically after verification. Certbot and the keeper remain separate services. A seeding leaf mirrors only allowed object snapshots and combines tenant-filtered manifest projections; the keeper never deletes a cache because the origin is missing.

When upgrading existing leaves, retain and later revoke the old public keys, and explicitly retire broad legacy caches before the restricted deployment is considered complete. Copy all leaf files when upgrading and rebuild/recreate NATS. No production upgrade or key revocation is performed by these local instructions.

Published leaf ports: 80 (Let's Encrypt), `NATS_PUBLIC_PORT`, and `LEAF_WS_PORT`. Native NATS DNS must be DNS-only in Cloudflare. Follow [checkpoint and recovery](../../OPERATIONS.md#hub-checkpoints-and-recovery) for the authoritative hub; leaf caches are replaceable.
