# 13 — routing as a ZeBridge service: Valhalla behind `zb-respond`

Valhalla, the OpenStreetMap routing engine, answering over NATS: a phone asks `route`, `matrix`
or `tour` and gets Valhalla's answer, with no Python, no HTTP endpoint exposed, and PostgreSQL
never asked. `zb-respond` (libzb, a native binary) serves the questions and forwards each to the
Valhalla server beside it.

The stops are data: the chargers of 08-map are a PostgreSQL table, replicated to the phone, which
picks them locally and asks only for the route between them. Routing is a question, because the
regional graph (hundreds of MB of tiles) does not belong on a phone.

## Run it

Valhalla, on the tiles of 08-map (Pays de la Loire):

```sh
docker run -d --name valhalla -p 8002:8002 \
  -v "$PWD/../08-map/valhalla/custom_files:/custom_files" \
  -e use_tiles_ignore_pbf=True -e force_rebuild=False -e serve_tiles=True \
  -e build_elevation=False -e build_admins=False -e build_time_zones=False \
  ghcr.io/gis-ops/docker-valhalla/valhalla:latest
```

A responder identity, and the service (set `url` and `creds` in `routing.json`):

```sh
bridge --mint-responder --name routing-nantes --tenant _default --store operator.store > routing.creds
(cd ../../libzb && zig build -Doptimize=ReleaseFast)
../../libzb/zig-out/bin/zb-respond routing.json
```

Ask, from any client (the payload is Valhalla's own request):

```sh
nats req query._default.route \
  '{"locations":[{"lat":47.218,"lon":-1.553},{"lat":47.478,"lon":-0.563}],"costing":"auto","units":"km"}'
```

## What it measured

On a laptop, Valhalla in OrbStack, 2026-10-05: Nantes → Angers 92.5 km, 56 min by car (58 by
truck), 36–48 ms in the service; a matrix from Nantes to three towns, 38 ms; an optimised tour
of four stops, 415 km, 208 ms. A point Valhalla cannot route answers at once with its own error
(`No suitable edges near location`, 400). Idle, `zb-respond` uses no CPU: with no table to
follow, libzb's poll waits on the questions. After 250 questions its memory had not moved
(3,999 allocations, 770 KB; `leaks`: none).

## The config

`routing.json`: the NATS server (a leaf near the askers, ideally), a responder's creds
(`--mint-responder`), `jsDomain` behind a leaf, the instance's `name` (each answer's `by`), the
queue group its instances share, the tenants it serves, and each question's HTTP target. The
payload is POSTed as it is; a JSON answer gains `by` and `ms`; an HTTP error comes back as
`{"error","status","detail"}`.
