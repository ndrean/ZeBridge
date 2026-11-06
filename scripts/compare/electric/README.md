# ZeBridge and Electric: read-side comparison

This is the Compose starting point for a read-only comparison. It layers Electric onto the ZeBridge quickstart, so both services can read the same PostgreSQL database and the same `storm_items` tenant fixture. The ZeBridge fixture is explicitly outbound-only (`writable => false`); the comparison does not measure writes.

Electric's own quickstart starts Postgres and Electric with Compose. This overlay follows that deployment shape, uses the same PostgreSQL 17 major as Electric's official quickstart, and shares one database between both systems. It overrides the ZeBridge quickstart's Postgres image, so use a fresh Compose volume (or remove the old one) when changing versions. Electric's official quickstart uses `electricsql/electric:latest`; record exact image digests before publishing results, since `latest` moves.

## Start the stack

From the repository root, on a clean Compose project (Docker Desktop or OrbStack). Keep these snippets in the same terminal so the exported secrets remain available. If reusing the project after `down -v`, remove its old test identities too; the new NATS operator will not accept their old credentials.

```sh
rm -rf /tmp/zebridge-electric-compare /tmp/electric-read-compare
export ELECTRIC_COMPARE_JWT_SECRET="$(openssl rand -hex 32)"
export QS_BRIDGE_PORT=27435
docker compose -p zebridge-electric -f docker-compose.quickstart.yml -f scripts/compare/electric/compose.overlay.yml \
  up -d --wait postgres init nats bridge sweeper electric electric-auth-proxy
docker compose -p zebridge-electric -f docker-compose.quickstart.yml -f scripts/compare/electric/compose.overlay.yml exec -T postgres \
  psql -U postgres -d app -v ON_ERROR_STOP=1 -f - < scripts/compare/electric/fixture.sql
export ELECTRIC_COMPARE_TOKEN="$(node scripts/compare/electric/token.mjs)"
curl -fsS http://127.0.0.1:30001/healthz
```

The ZeBridge quickstart initializes its standard demo tables as well. The comparison client must subscribe only to `storm_items` and tenant `cmp`. The explicit Compose project name keeps this stack separate from a regular quickstart. Reset it with `docker compose -p zebridge-electric -f docker-compose.quickstart.yml -f scripts/compare/electric/compose.overlay.yml down -v`; this deletes the comparison database and Electric's shape cache.

The targeted initial `up` starts only the services needed for comparison, not the quickstart's browser demos. The cold-seed instructions below stop one sync engine before timing.

## Cold-seed runners

The primary Electric client path uses Electric's TanStack collection adapter and TanStack DB's Node SQLite persistence adapter. It stores the collection in a local SQLite file and verifies both the queryable collection rows and persisted SQLite row count/checksum. The timer stops only after both the in-memory collection and the durable local row table hold all 50,000 rows by default (`COMPARE_ROWS` can override the target when the fixture is changed to match). The ZeBridge runner uses the repository's Python binding over libzb and its local SQLite replica.

The TanStack Electric runner intentionally supplies no item validation schema: the Electric adapter documents `schema` as optional, but still requires a primary-key accessor (`getKey`). It also uses `schemaVersion` to version the local persisted collection. This lets us measure dynamic row payloads, while exposing the integration difference: ZeBridge obtains table definitions from Postgres and builds its replica schema, whereas this TanStack collection is given its key rule and a local persistence version. Static TypeScript typing still benefits from a declared row type.

`seed_electric.mjs` is retained as a lower-level protocol reference. It uses Electric's `ShapeStream` but applies operations with benchmark-specific SQLite code; its timing is not the primary product comparison. `seed_tanstack_electric.mjs` uses the official Electric collection and the TanStack SQLite persistence adapter.

Build the host libzb once and install the pinned comparison dependencies:

```sh
(cd libzb && zig build lib -Doptimize=ReleaseFast)
npm install --prefix scripts/compare/electric/client
```

Prepare ZeBridge identities before switching services. The fixture creates up to 100 one-use invites, all scoped to tenant `cmp`:

```sh
PYTHONPATH="$PWD/zb-python/src" ZB_LIB="$PWD/libzb/zig-out/lib/libzbcore.dylib" \
  python3 scripts/compare/electric/seed_zebridge.py prepare 10
export ELECTRIC_COMPARE_TOKEN="$(node scripts/compare/electric/token.mjs)"
```

Keep the same terminal open so the Compose helper and exported secrets persist. Run each engine alone. Electric starts from fresh local SQLite files on every run. ZeBridge also clears its local replicas while retaining the enrolled identities.

```sh
# Set this helper once. Electric: leave only Postgres, Electric and its proxy running.
dc() { QS_BRIDGE_PORT=27435 docker compose -p zebridge-electric -f docker-compose.quickstart.yml -f scripts/compare/electric/compose.overlay.yml "$@"; }
dc stop bridge sweeper nats
node scripts/compare/electric/client/seed_tanstack_electric.mjs 10

# Switch to ZeBridge, keeping the same Postgres fixture.
dc stop electric electric-auth-proxy
dc up -d --wait nats bridge sweeper
PYTHONPATH="$PWD/zb-python/src" ZB_LIB="$PWD/libzb/zig-out/lib/libzbcore.dylib" \
  python3 scripts/compare/electric/seed_zebridge.py seed 10
```

For the next Electric run, stop ZeBridge, then run `dc up -d --wait electric electric-auth-proxy`. Use the same client count and alternate which engine runs first across repetitions. The first Electric seed creates the server-side shape; later runs reuse Electric's persisted shape cache while still starting from empty client stores. Report that as a warm server cache. A true cold-shape run needs a fresh `electric-data` volume as well as empty client stores. Record CPU, memory and the exact image digests with the output; no CDN is in this local Compose setup.

Electric itself stays inside the Compose network. Clients connect to the proxy at `http://127.0.0.1:30001/v1/shape`; it accepts a one-hour HS256 JWT for the test identity `compare-reader`, checks its signature, issuer, audience and expiry, maps that subject to tenant `cmp`, and fixes the table and tenant predicate upstream. The client cannot supply its own table, `where`, columns or subset filter. Host-side ZeBridge clients use `nats://127.0.0.1:14223` and enroll through `http://127.0.0.1:27435` using their one-use `zb-read-compare-*` invites.

This small proxy and its locally generated signing secret are comparison scaffolding, not a production identity provider. `ELECTRIC_INSECURE=true` is acceptable only because Electric has no host-published port and accepts traffic only from the proxy on the private Compose network. A production Electric deployment should connect the proxy or gatekeeper to the application's real identity provider and tenant membership source.

## What to compare

Use the same physical Postgres, table rows, client host, and client-visible tenant data. Run one sync service at a time for throughput measurements, while keeping Postgres and the fixture unchanged. A simultaneous side-by-side run is useful for functional parity but makes the services compete for CPU, disk and Postgres resources.

| Phase | Measure | Controls |
| --- | --- | --- |
| Cold seed | Time from empty local store to all 50,000 rows applied and queryable | Delete local store; include transfer, decode and local apply; verify row count and checksum |
| Warm restart | Time to queryable after reconnect with local store retained | Preserve the local store; report catch-up separately from initial seed |
| Server-side burst | Apply a known Postgres insert/update batch, then time until the client has applied all changes | Use administrator SQL for the source write; report commit-to-received and commit-to-applied separately |
| Fan-out | Bytes and completion time for increasing concurrent readers | Same number of real clients and same initial dataset; include service CPU, memory and disk |
| Disconnect/reconnect | Time to catch up after a bounded disconnection | Make the same source changes during each disconnect; verify no missing or duplicate rows |

Do not call a payload-receive time “seed time”: both clients must finish decoding and applying into their local SQLite database before the primary seed number stops. For ZeBridge, also report the chain/object download interval separately if practical. For Electric, record the HTTP response bytes and local operation-application interval separately.

### Tenant and authorization parity

ZeBridge's client for this fixture must use a credential scoped to tenant `cmp`; its NATS grants define the accessible tenant stream. Electric's direct local quickstart runs with `ELECTRIC_INSECURE=true`, so it does not provide equivalent tenant authorization by itself. To compare secured tenant deployments, put Electric behind a small auth proxy that derives `where tenant_id = $1` from a validated identity and never accepts the tenant filter from the client. Keep the proxy in the Electric topology and disclose its resource use. Do not treat a client-chosen `where` clause as a security boundary.

That is a product-level comparison as well as a performance comparison: ZeBridge binds tenant access to the issued NATS identity and the database tenant roster; Electric offers shape-level selection, with authorization normally supplied by an application proxy or gatekeeper. Compare both the bare sync engines and the deployed secure path, and label them separately.

## First local measurement

### Preliminary 10,000-row run

An earlier OrbStack run used 10 concurrent clients and 10,000 rows per client. Both runners verified row SHA-256 `e84f7e397c2cfdd0e545950dd455c829bac4d6c742678ef182647926f375b6ca`. The reported client time runs from client startup until all expected rows are queryable; checksum verification happens after all clients stop.

| Engine | Median per-client seed | Slowest client | Whole batch wall time |
| --- | ---: | ---: | ---: |
| ZeBridge | 0.760 s | 0.778 s | — |
| Electric | 1.570 s | 2.688 s | 2.731 s |

### 50,000-row ShapeStream reference run

On the same OrbStack setup, two rounds used 10 concurrent clients, each starting with an empty local SQLite database and seeding the same 50,000-row `cmp` tenant (about 200 bytes per row). Both systems verified the same row SHA-256: `f23eb95715da0c1c44811f949d27bb4bae36a8357b16244b67514f226ed574b7`.

| Round | ZeBridge median / slowest client | Electric median / slowest client | Electric batch wall time |
| --- | ---: | ---: | ---: |
| 1 | 2.904 / 2.928 s | 5.200 / 6.179 s | 6.190 s |
| 2 | 2.998 / 3.041 s | 5.318 / 5.611 s | 5.641 s |

ZeBridge's chain was built during identity preparation. Electric's first successful round created its shape; the second reused the persisted shape cache. Both rounds used empty client stores. These figures came from the lower-level ShapeStream plus custom SQLite materializer, not the primary TanStack DB path. Do not use them as the final product-stack comparison. No CDN was used.

### 50,000-row TanStack DB + SQLite run

One isolated OrbStack round used 10 clients and the same 50,000-row fixture. It ran Electric with its auth proxy and Postgres, then ZeBridge with NATS, bridge, sweeper, and the same Postgres. Each client's SQLite data table held all 50,000 rows when its timer stopped; both systems passed SHA-256 `f23eb95715da0c1c44811f949d27bb4bae36a8357b16244b67514f226ed574b7`.

The later shared local run also included PowerSync against that same Postgres fixture, with repeated runs and explicit server-cache notes. Its results and the three-system comparison are recorded in the [PowerSync comparison results](../../../COMPARISON.md#local-cold-seed-zebridge-powersync-and-electric).

| Client path | Median per-client seed | Slowest client | Batch wall time |
| --- | ---: | ---: | ---: |
| ZeBridge / libzb | 3.077 s | 3.113 s | not emitted |
| Electric / TanStack DB / Node SQLite | 15.137 s | 20.102 s | 20.538 s |

This is a more complete client-path comparison than the ShapeStream reference above. It includes TanStack's reactive collection and durable SQLite writes. The Electric shape cache was warm; all client SQLite stores were empty. The Electric path was slower in this run, but this is still one local Node run, not a mobile-device result or a broad throughput claim. It uses the Node `better-sqlite3` persistence adapter; TanStack provides separate React Native and Expo SQLite adapters, whose device performance is not measured here. The TanStack packages were pinned to `1.0.0-rc.1.3`; Electric was `electricsql/electric:latest`, digest `sha256:9f820a9f52096713bc4d926b8193dd1249962170d15539416fed95e3ee6a8790`.

Two one-shot `docker stats` samples during this isolated round showed Electric at 0.77% and 3.80% CPU, its Postgres at 3.02% and 0.06%, and the auth proxy at 11.05% and 9.66%. ZeBridge samples showed bridge at 0.18% twice, NATS at 1.33% and 0.42%, and Postgres at 2.88% and 0.02%. These are instantaneous samples, not time-weighted averages. Compare the bridge service with Electric's service directly, and consider NATS, Postgres, and proxy separately when comparing whole deployments.

The seed runners measure initial read seeding only. They do not compare ongoing CDC latency, mutation/write paths, offline duration, reconnect behavior, or geographic leaf-node placement. The TanStack DB / SQLite path adds a reactive client collection and durable SQLite persistence; its in-memory collection layer is also part of the measured Electric client path.

### StreamDB is a separate experiment

StreamDB is not a client materializer for Electric Postgres Sync. It creates a TanStack DB over a Durable Stream and requires a state schema; connecting Postgres data would require publishing that data into a Durable Stream first. Treat it as a separate architecture/product comparison, not another Electric client adapter. Sources: [StreamDB](https://durablestreams.com/stream-db), [Electric Postgres Sync + TanStack DB](https://electric.ax/sync/tanstack-db).

## References

- [Electric Quickstart](https://electric.ax/docs/sync/quickstart) starts Postgres and Electric with Docker Compose.
- [Electric Shapes](https://electric.ax/docs/sync/guides/shapes) describes table, `where`, and column selection; shapes are currently single-table.
- [Electric Auth](https://electric.ax/docs/sync/guides/auth) recommends authorizing through a proxy or gatekeeper and says the rules are optional.
- [Electric Writes](https://electric.ax/docs/sync/guides/writes) explicitly describes Electric as read-path sync without a built-in write-path solution.
- [Electric Deployment](https://electric.ax/docs/sync/guides/deployment) documents the Postgres connection, logical replication, HTTP API, storage persistence, and authorization boundary.
- [TanStack Electric Collection](https://tanstack.com/db/latest/docs/collections/electric-collection) documents the Electric collection adapter and its optional runtime item schema.
- [TanStack SQLite Persistence](https://tanstack.com/db/latest/docs/guides/sqlite-persistence) lists SQLite persistence adapters for React Native, Expo, Node, and other runtimes.
