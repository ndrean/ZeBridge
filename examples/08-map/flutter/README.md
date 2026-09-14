# ZeMap — one screen, one table, one tenant

A Flutter map of Nantes whose markers are the rows of a `pois` table in PostgreSQL.

The table reaches the app through the bridge and libzb; the app never creates it.

Tap "+" then the map to add a point, tap a marker to edit its note or erase it. Every device of the tenant sees the change on its next poll, and a row inserted straight into PostgreSQL shows up the same way.

## The table

Created on the master and enabled for the bridge, nothing else:

```sql
CREATE TABLE public.pois (
  uid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  lat double precision NOT NULL,
  lng double precision NOT NULL,
  note text,
  tenant_id text NOT NULL,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  deleted_at timestamptz
);

SELECT * FROM public.zebridge_enable(
  'public.pois'::regclass,
  tenant_col => 'tenant_id', writable => true, version_col => 'updated_at',
  tombstone_col => 'deleted_at',
  publication => 'my_pub',
  dry_run => false
);
```

An erase sets `deleted_at` on the master (the tombstone) and every replica drops the row: the local table never holds an erased point.

## Cells: the map cut into tenants

The map is cut into geohash-5 cells, about 5 km a side at this latitude, and every
cell is an ordinary tenant named `c_<geohash>`. A POI belongs to the cell of its
coordinates. `examples/08-map/provision.py` makes a principal, `mapper`, a member of
the 5×5 grid around Nantes, enrols it once, and writes its creds; with `--seed` it
adds a few POIs, each in its cell. Restart the bridge once after it, so every cell has
its CDC stream.

The app follows only the 3×3 ring around the centre of the view. As you pan, it leaves
the cells that fell out of the ring and joins the ones that came in, through libzb's
join and leave: a join seeds the cell's rows into the local `pois` table, a leave
deletes them. The grid is drawn on the map, the followed ring in teal. A tap to add a
POI outside the ring is refused, since its echo would never come back.

The reach is fixed at enrolment: the JWT carries one tag per cell, and a join outside
them is refused by the broker. Growing the grid means provisioning the new cells and
re-enrolling.

## Threading

`lib/zebridge.dart` is the FFI card, `lib/zebridge_worker.dart` the worker from the 05-mobile example: ONE long-lived isolate owns the libzb handle and runs the poll loop; sync, poll, query, mutate, flush and close all happen there, the UI isolate only sends messages and receives reports.

💡 A libzb handle is **not thread-safe**, so `Isolate.run` around each poll is the wrong shape: the poll would block on one thread while the UI thread still queried and mutated the same handle.

## Run

The dev stack (PostgreSQL, nats-server, the bridge on `my_pub`/`my_slot`), the library built with `zig build -Doptimize=ReleaseFast` in `libzb/`, the paths at the top of `lib/main.dart` (alice's creds, the pmtiles file), then `flutter run -d macos`.

The vector tiles come from `test_region.pmtiles` with the Protomaps light theme (the source is named `protomaps`); without the file the map falls back to OSM raster tiles.

API keys, if any, live in `.env*` files, which git ignores.
