# 10-airports — the world's airports, on a map, without querying PostgreSQL

9,253 airports in one PostgreSQL table. A DuckDB service holds a replica and answers the
map's questions over NATS; the browser asks it, and PostgreSQL never sees a query.

You need a running ZeBridge: PostgreSQL with the init SQL applied, NATS, and the bridge
([SUPABASE_TEST.md](../../SUPABASE_TEST.md) sets one up on a cloud database). The commands
below run from the repository root, with the admin URL of your database in `$ADMIN_URL`.

## 1. The table

```sh
psql "$ADMIN_URL" -f examples/10-airports/airports.sql
```

[airports.sql](airports.sql) names the publication `my_pub`: change it to your bridge's
(`BRIDGE_CDC_PUBLICATION`). It creates `airports` and enables it: public (the same rows for
every client) and read-only (clients read it, nobody writes to it from a device). Every
text column is bounded, so no row can outgrow the bridge's buffer. The bridge follows the
new table at once, with no restart: the last two lines of the output read `T3 bridge LIVE`
and `T4 nats LIVE`.

## 2. Check

```sh
set -a; . zb-nats/.env.nats; . ./.env.bridge; set +a    # the bridge's own two files
bridge --diagnose
```

It ends with `🩺 DIAGNOSE: all clear`. Two notes about `airports` are expected: it is
outbound-only, and it has no tombstone column (rows are never deleted).

## 3. The data

```sh
psql "$ADMIN_URL" -c "\copy airports(code, icao, name, latitude, longitude, elevation, url, time_zone, city_code, country, city, state, county, type) FROM 'examples/10-airports/airports-world.csv' WITH (FORMAT csv, HEADER)"
```

`COPY 9253`. The bridge streams the rows to NATS as they commit, and its next snapshot
holds them all.

## 4. The DuckDB service and the map

To come: a service that follows `airports` into DuckDB and answers `airports_in_view`
for the map, and a page that asks it as you pan.
