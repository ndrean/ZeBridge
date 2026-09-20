# ZeMap in a browser — the fuel prices and the shared route

The two features of the map example that need no local dataset, in a browser, on the
same client library the phone runs (`zb-client-ts` here, libzb there). It exists to be
the SECOND editor: open it beside the Flutter app and move the route from both.

    pnpm install --ignore-workspace     # the repo's pnpm-workspace.yaml lists no packages
    pnpm dev                            # http://localhost:5174/?principal=alice

`?principal=` picks the credential (`alice` by default, `omar` is the phone's). The
page talks only to its own origin: Vite proxies the NATS websocket at `/nats` and
serves `public/creds`, a symlink to `scripts/native/creds`.

**The fuel prices are an ASK.** `request('query.<tenant>.fuel_near')` reaches the POI
service, which answers from its DuckDB replica of all of France; PostgreSQL is never
queried. Nothing is stored in the browser: no table, no stream, no position — the page
holds the answer for as long as it draws it. Measured here: 25 stations within 6 km,
73 ms in the service, 80 ms round trip. Pick a fuel, then pan: it asks again.

**The shared route is a ROW.** `routes.doc` is a jsonb map of registers — `start` and
`end`, each carrying a value, its writer's stamp and its writer's name — replicated
into this browser's OPFS SQLite like any table and written with `mutate`. Turn the
route on and click twice; the pins are drawn from the ROW as CDC delivers it, never
from the click, so what is on screen is what the row holds, whoever moved it last. Two
editors converge because each ships the union of its own registers merged into the
document it last saw (`mergeRegisters`, the library's own rule, the same one libzb
gives the phone), and reconciles until the row contains what it wrote. The row
underneath is ordinary last-write-wins. NOTES §10ho; `scripts/scenarios/route_crdt.py`
asserts the same war between two libzb clients.

Needs the dev stack (bridge, nats-server, PostgreSQL), the POI service running for the
fuel, and `examples/08-map/load_routes.py --create` once for the table.

**The basemap is OpenStreetMap raster**, not the R2 vector tiles the phone renders: a
vector basemap in a browser needs a style sheet this demo does not need to own. Two
things about it are worth knowing, because both are invisible when wrong. The page is
cross-origin ISOLATED (OPFS requires it), so the tiles are fetched with CORS
(`crossOrigin` on the layer) — without that every tile request succeeds and the map is
a grey void. And the dev server listens on `localhost` only: `127.0.0.1:5174` is
refused.
