# The tiles' door

A Cloudflare Worker in front of the R2 bucket `ze-map`, serving `france.pmtiles` by HTTP
range requests — what the map's PMTiles reader does — cached at the edge, with no
credentials in the app or in the Worker: the bucket is a *binding*, granted at deploy time.

    cd examples/08-map/worker
    npx wrangler deploy                      # logs you in once; the dev one is https://ze-map-worker.ze-map.workers.dev
    curl -s -o /dev/null -w "%{http_code} %header{content-range} %header{x-cache}\n" -r 0-127 https://ze-map-worker.ze-map.workers.dev/france.pmtiles   # 206 bytes 0-127/…, MISS then HIT

Then `_pmtilesUrl` in `../flutter/lib/main.dart` becomes that address. A custom domain on
the Worker (or on the bucket itself, under R2 → Settings → Custom domains, which needs no
Worker at all if you have a zone on Cloudflare) removes the `workers.dev` name.

What it does and does not do: GET and HEAD of the names in `KEYS`, `Range` and
`If-None-Match` passed to R2, `Content-Range`/`ETag`/`Accept-Ranges` answered, one day of
edge cache keyed by URL and range, CORS for a web build. No listing, no other keys, no
writes.

What it does and does not do, continued: a hit is served only when its stored
`Content-Range` is exactly the range asked for — the cache key carries the range in the
query string, and a colliding or stale entry is treated as a miss rather than served
(version 3 answered concurrent range requests with another range's body; NOTES §10hj).
`X-Worker` carries the version constant at the top of `src/index.js`; bump it with every
change, it is how a deploy is told apart from the previous one.

The two checks after a deploy, from the repository:

    curl -sI -r 0-127 https://ze-map-worker.ze-map.workers.dev/france.pmtiles | grep -i x-worker
    cd ../flutter && dart run tool/tiles_probe.dart https://ze-map-worker.ze-map.workers.dev/france.pmtiles   # ten tiles, no FAILED
