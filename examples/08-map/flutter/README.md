# ZeMap — the phone holds what it asked for

A Flutter map of France: the vector tiles from an R2 bucket, the OpenStreetMap points
of interest around the viewport as markers, editable. NOTES §10hj, design B.

`charge_points` (16,173 OpenChargeMap points of France, `examples/08-map/load_chargers.py`
— §10hr) is an **on-demand** table on the phone: libzb creates it from the descriptor,
nothing seeds it and nothing is tailed. On every move the map asks the map service —
`examples/08-map/poi_service.py`, a DuckDB replica of all of France answering
`query._default.chargers_near` from its own copy, PostgreSQL never asked — for the points
around the centre (the radius follows the zoom,
at most 5 km, the 2,000 nearest; below zoom 10 the map stops asking and draws what it holds), keeps the
answer in its SQLite through the same version-guarded upsert a seed uses, and draws
from the local table. Offline, every area visited is still there.

The markers colour by what they are: green for rapid (≥43 kW), blue for the rest, grey
for out of service, orange for one this phone added. The bolt button asks for the rapid
ones only.

Tap "+" then the map to add a charge point, tap a marker to rename or erase it. Each is a
`mutate` on the local table: optimistic at once, sent to the bridge, judged upstream (the
verdict lands in the status line's next poll), and the service's replica has it before
the next ask. An erase sets `deleted_at` on the master (the delete guard) and every
replica drops the row. A new point carries its PostGIS bytes (`ewkbPoint` in main.dart).

Needs, besides the dev stack: `charge_points` loaded (`load_chargers.py --create`, which
publishes it public and writable), the map service
running (`scripts/scenarios/.venv/bin/python3 examples/08-map/poi_service.py`), libzb
and libduckdb installed for the service (the app itself needs only SQLite), and the
`omar` creds of the dev stack. The `mapper` principal of the earlier cell design was
revoked with its grid, and a revoked principal stays revoked.

**One route, two editors** (NOTES §10ho). The directions button enters route mode; a tap
sets the start, the next the end, and both pins are drawn from the row `routes.doc` as CDC
delivers it, not from the tap. That row is a jsonb map of registers — `start` and `end`,
each with its writer's stamp and name — over a plain LWW row: the phone writes its own
registers merged into the document it last saw (`mergeRegisters`, the library's own rule
through `zb_call`), and reconciles until the row contains what it wrote. Open the web
consumer (`examples/05-tables/web-consumer`, its "One route, two editors" panel) as another
principal and move the end while the phone moves the start: both land. Move the same end
on both: the later stamp wins on every replica, and the loser's pin jumps. The table comes
from `examples/08-map/load_routes.py --create`; `scripts/scenarios/route_crdt.py` is the
same war between two libzb clients, asserted.

`examples/08-map/poi_phone.py` is the same phone in Python — `--edit`, `--tour`, `--fuel`
— and the first thing to run when the app shows nothing.

## Threading

`lib/zebridge.dart` is the FFI card, `lib/zebridge_worker.dart` the worker from the 05-tables example: ONE long-lived isolate owns the libzb handle and runs the poll loop; sync, poll, query, mutate, flush and close all happen there, the UI isolate only sends messages and receives reports.

💡 A libzb handle is **not thread-safe**, so `Isolate.run` around each poll is the wrong shape: the poll would block on one thread while the UI thread still queried and mutated the same handle.

## Run

The dev stack (PostgreSQL, nats-server, the bridge on `my_pub`/`my_slot`), the library built with `zig build -Doptimize=ReleaseFast` in `libzb/` (the map service's DuckDB engine opens libduckdb at run time, so DuckDB must be installed), the creds path at the top of `lib/main.dart`, then `flutter run -d macos`.

The vector tiles come from `france.pmtiles` on R2, through the Worker in `../worker`, read by HTTP range request so only the tiles in view travel. That is the ONE source. A local `test_region.pmtiles` used to stand behind it as a fallback and has been removed: a second archive that nobody refreshes draws a different map and says nothing about it. When the archive is unreachable the map shows plain OSM raster tiles and the status line says why.

API keys, if any, live in `.env*` files, which git ignores.

## Generate France PMTiles with Planetiler

```sh
mkdir -p data
docker run -e JAVA_TOOL_OPTIONS="-Xmx8g" \
  -v "$(pwd)/data":/data \
  ghcr.io/onthegomap/planetiler:latest \
  --area=france \
  --output=/data/france.pmtiles
```

Configure `rclone`:

```sh
rclone config update r2 \
> access_key_id "xxx" \
> secret_access_key "xxx" \
> endpoint "https://c0344d0aa37d230e463fd7e8ddd58f0b.r2.cloudflarestorage.com" \
> regiion auto
```

Check:

```sh
rclone lsd r2:

#         -1 2026-09-18 14:29:14        -1 ze-map
```

Copy via `rclone` the local file _france_pmtiles_  into the R2 bucket "ze-map":

```sh
rclone copyto france.pmtiles :s3:ze-map/france.pmtiles \
  --s3-provider Cloudflare \
  --s3-endpoint "https://c0344d0aa37d230e463fd7e8ddd58f0b.r2.cloudflarestorage.com" \
  --s3-access-key-id "<ACCESS_KEY_ID>" \
  --s3-secret-access-key "<SECRET_ACCESS_KEY>" \
  --progress
```

From the /data folder:

```sh
rclone copy france.pmtiles r2:ze-map/ -P
```

## When tiles do not load

`tool/tiles_probe.dart` opens the archive through a URL with the app's own reader and
fetches tiles across France, in and out of the first view:

```sh
dart run tool/tiles_probe.dart https://ze-map-worker.ze-map.workers.dev/france.pmtiles
```

Ten tiles in 180–590 ms each means the door and the reader are fine and the problem is in
the screen — the one time it happened, a redraw on every pan event was starving the tile
layer, hence the debounce in `onPositionChanged`.
