# Roads for the map: a Valhalla server

The tour and the route queries of `../poi_service.py` measure straight lines until a
routing engine answers. Valhalla is that engine: an OpenStreetMap extract in, a graph
built once, `/route` and `/sources_to_targets` out. It runs here as a container beside
the dev stack; the service calls it on `localhost:8002`, the phone never does.

## The graph

The extract goes into `custom_files/`; the container builds the graph from every `.pbf`
it finds there on its first start and keeps it in the same folder. Pays de la Loire
(~430 MB, minutes to build) covers Nantes, where the map lives; all of France is
`france-latest.osm.pbf` (~4 GB, hours, ~10 GB of graph) when the tour needs it.

    curl -L -o custom_files/pays-de-la-loire-latest.osm.pbf \
      https://download.geofabrik.de/europe/france/pays-de-la-loire-latest.osm.pbf

    docker run -dt --name valhalla -p 8002:8002 \
      -v "$(pwd)/custom_files":/custom_files \
      -e serve_tiles=True -e build_elevation=False -e build_admins=False -e build_time_zones=False \
      ghcr.io/gis-ops/docker-valhalla/valhalla:latest

    docker logs -f valhalla          # "Found valhalla tiles!" then the server on :8002

The `docker` CLI is OrbStack's here (`docker context ls` shows `orbstack *`); with Docker
Desktop also installed, `docker --context orbstack run …` leaves no doubt. Pays de la
Loire built in two minutes on this Mac; `custom_files/` is git-ignored (the extract alone
is 372 MB).

## The check

    curl -s http://localhost:8002/route --data '{"locations":[{"lat":47.2184,"lon":-1.5536},{"lat":47.2076,"lon":-1.5497}],"costing":"auto","units":"kilometers"}' \
      | python3 -c 'import json,sys; r=json.load(sys.stdin)["trip"]; print(r["summary"]["length"], "km,", round(r["summary"]["time"]/60), "min")'

Nantes centre to the Île de Nantes, a couple of kilometres, a few minutes. A point
outside the extract answers `No path could be found` — the graph's edge, not a fault.

## Not the DuckDB extension, for now

`INSTALL valhalla_routing FROM community` installs, loads, and declares the functions
(`travel_time`, `travel_time_matrix`, `valhalla_route_wkb`, `travel_time_load_config`):
routing inside the replica, no container, is the shape this service wants. But every
build in the community repository — osx_arm64 (4.9 MB), linux_arm64 (9.8 MB),
linux_amd64 (10.8 MB), checked 2026-09-18 on the tiles built above — answers
`Valhalla not available: extension built without Valhalla support`. The extension is a
work in progress (its README says so); the day a build carries Valhalla, the service's
`route` becomes one SQL statement on the replica. Until then, the container.

## In the service

`poi_service.py --valhalla http://localhost:8002` (the default) makes two things real:
`route` (two or more points → the road polyline, kilometres, minutes) and, when it is
reachable, the tour's legs by road instead of straight lines. Without a reachable
Valhalla the service says so in the answer and falls back to straight lines.
