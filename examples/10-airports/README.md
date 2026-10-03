# 10-airports — a shared flight and a map service over NATS

This demo shows two things:

1. **A write travels through PostgreSQL to the other browsers.** Users of one tenant edit the same flight. When one sets the departure or the arrival, PostgreSQL accepts the write and every other browser of that tenant draws the new route.
2. **A browser asks a service over NATS and gets an answer.** A DuckDB service holds a copy of 9,253 airports and answers "which airports are near here?" for the map. PostgreSQL never sees these questions.

You need a running ZeBridge: PostgreSQL with the init SQL applied, NATS, and the bridge ([SUPABASE_TEST.md](../../SUPABASE_TEST.md) sets one up on a cloud database). The commands below run from the repository root, with the admin URL of your database in `$ADMIN_URL`.

## 1. The table

```sh
psql "$ADMIN_URL" -f examples/10-airports/airports.sql
```

[airports.sql](airports.sql) names the publication `my_pub`: change it to your bridge's (`BRIDGE_CDC_PUBLICATION`). It creates `airports` and enables it: public (the same rows for every client) and read-only (clients read it, nobody writes to it from a device). Every text column is bounded, so no row can outgrow the bridge's buffer. The bridge follows the new table at once, with no restart: the last two lines of the output read `T3 bridge LIVE` and `T4 nats LIVE`.

## 2. Check

```sh
set -a; . zb-nats/.env.nats; . ./.env.bridge; set +a    # the bridge's own two files
bridge --diagnose
```

It ends with `🩺 DIAGNOSE: all clear`. Two notes about `airports` are expected: it is outbound-only, and it has no tombstone column (rows are never deleted).

## 3. The data

```sh
psql "$ADMIN_URL" -c "\copy airports(code, icao, name, latitude, longitude, elevation, url, time_zone, city_code, country, city, state, county, type) FROM 'examples/10-airports/airports-world.csv' WITH (FORMAT csv, HEADER)"
```

`COPY 9253`. The bridge streams the rows to NATS as they commit, and its next snapshot holds them all.

## 4. The DuckDB service

[airport_service.py](airport_service.py) follows `airports` into a local DuckDB file and answers on `query._default.<name>`: the open tenant, which every client may ask. It needs a responder's credentials, minted from the NATS setup's offline store, and libzb built:

```sh
bridge --mint-responder --name airports --tenant _default --store zb-nats/operator.store > zb-nats/creds/airports.creds
```

```sh
PYTHONPATH=zb-python/src python3 examples/10-airports/airport_service.py \
  --url nats://127.0.0.1:4222 --creds zb-nats/creds/airports.creds
```

It seeds the 9,253 airports from the snapshot (about a second) and keeps them live from the change stream.

Two questions:

| question | parameters | answer |
| --- | --- | --- |
| `airports_in_view` | `south`, `west`, `north`, `east`, `limit` (500) | the airports in the box. A box across the antimeridian wraps. When more than `limit` are inside, one per cell of a grid over the box, so a world view spreads over every continent (`complete: false`, `total`). |
| `airports_near` | `lat`, `lng`, `radius_km` (100), `limit` (20) | the nearest airports, with `distance_km` |

The service answers on libzb's own thread, inside the callback that hands it the question; a question wakes libzb's poll the moment it lands. Measured over NATS on the same host: 3 to 5 ms in DuckDB, about 6 ms round trip; idle, it uses 0.1% of a core.

## 5. The map

[web/](web/) is one page: a map centred on San Mateo, California, about 200 km across.
After every pan or zoom it asks `airports_near` for the airports within 100 km of the
centre (the dashed circle), draws them, and writes the count under the map. It follows no
table: the answer is all it holds.

```sh
cd examples/10-airports/web
pnpm install --ignore-workspace
pnpm dev                     # http://localhost:5175/?invite=<code>
```

The first visit enrolls with an invite (any tenant: the service answers on the open one),
and the browser keeps the identity, so later visits need no invite:

```sql
INSERT INTO zebridge_invites (code, principal, tenant_id) VALUES ('<a random code>', 'alice', 'acme');
```

The dev server proxies NATS's WebSocket (`/nats`) and the bridge (`/bridge`, for `/enroll`
and `/renew`), so the page talks only to its own origin. Their addresses are the two lines
of [vite.config.ts](web/vite.config.ts), `ZB_NATS_WS_ORIGIN` and `ZB_BRIDGE_ORIGIN`. The
tiles are OpenStreetMap's, for testing only: their usage policy forbids more.

PostgreSQL plays no part in an answer: the data was loaded at Supabase in London, and
every question is answered from the service's DuckDB on the laptop.

## 6. A flight, edited together

```sh
psql "$ADMIN_URL" -f examples/10-airports/flights.sql
```

[flights.sql](flights.sql) creates `flights`, one row per tenant, and enables it: writable,
divided by tenant, with a version, a tiebreak and a tombstone. Its `doc` holds two
registers, the departure and the arrival, each `{v, t, w}`: the airport, its stamp and its
writer ([COOPERATIVE_EDITING.md](../../COOPERATIVE_EDITING.md)).

The page follows `flights` into the browser's own SQLite. Click an airport, choose
**Departure** or **Arrival**: the end shows hollow until PostgreSQL accepts the write, then
solid, and the great circle between the two ends is drawn with its distance and heading.
Everyone in the same tenant sees the same flight, live:

- two people move different ends at once: both moves survive;
- they move the same end: the later stamp wins, on every screen, and the other is told
  ("bob's LAX came after your SFO");
- whoever changes an end, the others read who did it, and when, under the map.

Someone in another tenant has a flight of their own and never sees this one. A stamp is
the bridge's time as the browser estimates it (`zb.stamp()`), never behind what the browser
has seen: a device whose clock is off does not win a race by its error.

To try it with two people in one browser, give each an invite in the same tenant and a
name: `?as=alice&invite=…` in one tab, `?as=bob&invite=…` in another. Measured on Supabase:
an end set in one tab appears in the other with its author, and a third person in another
tenant sees an empty flight.


## 7. On an iPhone

[flutter/](flutter) is the same map and the same flight in a Flutter app on libzb (through
zb-dart). The phone and the browsers of one tenant edit the one flight: an end set on the
phone appears in the browsers, and the other way round. The stamp comes from libzb's
`stamp()`, the merge from libzb's `mergeRegisters`.

The phone needs an invite in the same tenant as the browsers. From
`examples/10-airports/flutter`:

```sh
tool/build-libzb-ios.sh
tool/export-roots.sh            # the trusted roots the app ships (see below)
flutter pub get
flutter build ios --release --dart-define=ZB_BRIDGE_URL=https://bridge.example.com --dart-define=ZB_INVITE=<code>
xcrun devicectl device install app --device <udid> build/ios/iphoneos/Runner.app
```

The enrollment answer gives the app its NATS address (`tls://…:4222`). Zig reads no
trust store on iOS, so the app ships Apple's root certificates, exported from your Mac by
`tool/export-roots.sh`, and gives libzb the file as `caFile`: libzb checks the bridge's
`https://` certificate and the NATS server's against it. On a local network without TLS,
pass `http://` and `nats://` addresses instead (`ZB_NATS_URL`), with the bridge listening on
the network (`BRIDGE_BIND=0.0.0.0`), and allow "Local Network" when iOS asks.

The invite is used once; later launches open on the identity kept on the phone. Tap an airport to make it the departure or the arrival.
