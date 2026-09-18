# The tiles' door

A Cloudflare Worker in front of the R2 bucket `ze-map`, serving `france.pmtiles` by HTTP
range requests — what the map's PMTiles reader does — cached at the edge, with no
credentials in the app or in the Worker: the bucket is a *binding*, granted at deploy time.

    cd examples/08-map/worker
    npx wrangler deploy                      # logs you in once; prints https://ze-map-tiles.<you>.workers.dev
    curl -sI -r 0-127 https://ze-map-tiles.<you>.workers.dev/france.pmtiles   # 206, Content-Range

Then `_pmtilesUrl` in `../flutter/lib/main.dart` becomes that address. A custom domain on
the Worker (or on the bucket itself, under R2 → Settings → Custom domains, which needs no
Worker at all if you have a zone on Cloudflare) removes the `workers.dev` name.

What it does and does not do: GET and HEAD of the names in `KEYS`, `Range` and
`If-None-Match` passed to R2, `Content-Range`/`ETag`/`Accept-Ranges` answered, one day of
edge cache keyed by URL and range, CORS for a web build. No listing, no other keys, no
writes.
