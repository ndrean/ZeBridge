# ZeBridge with Docker Compose

One server runs NATS, the bridge, the sweeper, HAProxy and Let's Encrypt, and optionally
PostgreSQL. The images are the releases': `ghcr.io/ndrean/zebridge` (built by GitHub
Actions from the release's `.deb`), `nats`, `haproxy`, `certbot`, `postgres`.

| name | port | what |
| -- | -- | -- |
| `BRIDGE_HOST` | 443 | HAProxy → the bridge: `GET /enroll`, `/renew`, `/status`, rate-limited |
| `WS_HOST` | 443 | HAProxy → the NATS websocket, for browsers |
| `NATS_HOST` | `NATS_PUBLIC_PORT` | NATS with TLS, for native clients (phones, libzb) |
| (any) | 80 | Let's Encrypt's challenge; everything else is redirected to https |

PostgreSQL, NATS monitoring and the bridge's `/metrics` are not published.

## Setting up

1. Three DNS records (A, and AAAA if the server has IPv6) pointing at the server. With
   Cloudflare, "DNS only": the certificates are issued on port 80, and native clients
   reach NATS directly.
2. Copy this folder to the server, then the settings:

   ```sh
   cp .env.example .env && chmod 600 .env
   ```

   Fill in the names, the e-mail and the three passwords (letters and digits only:
   `openssl rand -hex 24`).
3. Start:

   ```sh
   docker compose up -d
   ```

   `COMPOSE_PROFILES=postgres` in `.env` adds the PostgreSQL service, to this and every
   other command (`down`, `logs`, `ps`). Without that line, the stack uses the external
   PostgreSQL whose URLs are in `.env`.

   The first start issues the certificate, generates the NATS operator and accounts, and
   creates the database's roles, functions and publication. Each of these runs once.
4. Check:

   ```sh
   docker compose ps -a
   docker compose exec bridge bridge --diagnose
   curl https://$BRIDGE_HOST/status
   ```

5. Copy `operator.store` off the server: it holds the root seeds, which only
   `--init-nats --update`, `--mint-leaf` and `--mint-responder` need.

   ```sh
   docker compose run --rm --no-deps --entrypoint cat nats-init /etc/zebridge/operator.store > operator.store
   ```

## What runs once, and where it lives

| service | does | kept in |
| -- | -- | -- |
| `certs-init` | the first certificate | volume `letsencrypt`; copies in `certs` |
| `nats-init` | `bridge --init-nats operator` | volume `zb-etc`: conf, `.env.nats`, creds, JetStream data |
| `pg-init` | `bridge --init-sql \| psql` | the database |

⚠️ Never delete the `zb-etc` volume (`docker compose down -v` does): a new operator would
refuse every enrolled device. Back it up with the database.

Renewals: `certbot` checks twice a day; NATS and HAProxy reload the new certificate
without dropping connections.

## Upgrading

Set `ZB_VERSION` in `.env` to the new release, then `docker compose up -d`.

## Firewall

Docker's published ports bypass ufw: 80, 443 and `NATS_PUBLIC_PORT` are open to the
internet whatever ufw says, and nothing else is published.
