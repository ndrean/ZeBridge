// examples/08-map/worker — the R2 bucket `ze-map` served the way PMTiles reads it:
// HTTP range requests, cached at Cloudflare's edge, no credentials in any client.
//
// GET  /france.pmtiles          Range: bytes=a-b   → 206 + Content-Range (what the app does)
// HEAD /france.pmtiles                             → 200 + Content-Length + ETag
// Anything else                                    → 404; any method but GET/HEAD → 405
//
// Why a Worker and not the r2.dev address: r2.dev is a development door — uncached and
// rate-limited. Why not the S3 endpoint: it wants signed requests, and a signing key in
// an app is a key in every user's hands. A binding needs neither.

const KEYS = new Set(["france.pmtiles"]);          // what this door serves; add a name to add a file

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const key = url.pathname.replace(/^\/+/, "");
    const cors = corsHeaders(env.ALLOW_ORIGIN || "*");
    if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: cors });
    if (request.method !== "GET" && request.method !== "HEAD") return new Response("method", { status: 405, headers: cors });
    if (!KEYS.has(key)) return new Response("not found", { status: 404, headers: cors });

    // The edge cache, keyed by URL + Range: a tile fetched once by one phone is served from
    // the edge to the next. R2 reads only on a miss.
    const cache = caches.default;
    const cacheKey = new Request(url.toString() + "#" + (request.headers.get("Range") || ""), { method: "GET" });
    const hit = await cache.match(cacheKey);
    // Stored as 200 (the Cache API refuses 206); a Content-Range header says it was partial.
    if (hit) return withHeaders(hit, cors, request.method === "HEAD", hit.headers.has("Content-Range") ? 206 : 200);

    const opts = { onlyIf: request.headers, range: request.headers };
    const obj = await env.TILES.get(key, opts);
    if (obj === null) return new Response("not found", { status: 404, headers: cors });

    const headers = new Headers(cors);
    obj.writeHttpMetadata(headers);
    headers.set("ETag", obj.httpEtag);
    headers.set("Accept-Ranges", "bytes");
    headers.set("Cache-Control", "public, max-age=86400, immutable");  // a pmtiles file is replaced, never edited
    let status = 200;
    if (obj.range) {
      const { offset, length } = obj.range;
      const end = offset + (length ?? obj.size - offset) - 1;
      headers.set("Content-Range", `bytes ${offset}-${end}/${obj.size}`);
      headers.set("Content-Length", String(end - offset + 1));
      status = 206;
    } else {
      headers.set("Content-Length", String(obj.size));
    }
    // `onlyIf` satisfied → R2 answers without a body: the app's own ETag revalidation.
    if (!obj.body) return new Response(null, { status: 304, headers });

    const response = new Response(request.method === "HEAD" ? null : obj.body, { status, headers });
    if (request.method === "GET") {
      // The partial response is small (a tile, a directory). Stored as 200: the Cache API
      // does not keep 206, and the Range is part of the key anyway.
      await cache.put(cacheKey, new Response(response.clone().body, { status: 200, headers }));
    }
    return response;
  },
};

function corsHeaders(origin) {
  return {
    "Access-Control-Allow-Origin": origin,
    "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
    "Access-Control-Allow-Headers": "Range, If-None-Match",
    "Access-Control-Expose-Headers": "Content-Range, Content-Length, ETag, Accept-Ranges",
  };
}

function withHeaders(response, cors, head, status) {
  const headers = new Headers(response.headers);
  for (const [k, v] of Object.entries(cors)) headers.set(k, v);
  return new Response(head ? null : response.body, { status, headers });
}
