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

const VERSION = "4";                             // bumped with every change: `X-Worker` says what runs
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
    // ⚠️ The Range goes in the QUERY STRING of the key, never after `#`: the Cache API
    // ignores fragments, so a key built that way made every range of the file one entry —
    // the reader asked for 142,446 bytes and got another range's 14,415 (measured).
    const cache = caches.default;
    const keyUrl = new URL(url.toString());
    keyUrl.searchParams.set("range", request.headers.get("Range") || "");
    const cacheKey = new Request(keyUrl.toString(), { method: "GET" });
    const rangeHeader = request.headers.get("Range");
    const wanted = parseRange(rangeHeader);
    const hit = await cache.match(cacheKey);
    // Stored as 200 (the Cache API refuses 206); a Content-Range header says it was partial.
    // ⚠️ Served only if it is THE range asked for: a wrong entry under a colliding key was
    // measured (a 60,001-byte ask answered with 75,194 bytes) — a mismatch is a miss.
    if (hit && sameRange(hit.headers.get("Content-Range"), wanted)) {
      return withHeaders(hit, { ...cors, "X-Cache": "HIT", "X-Worker": VERSION }, request.method === "HEAD", hit.headers.has("Content-Range") ? 206 : 200);
    }

    // The range from the request itself: R2's `obj.range` describes what it applied, but
    // its shape varies (offset+length, offset+end, suffix) and a missing field produced
    // "bytes 0-undefined" in the first deployment. `bytes=a-b`, `bytes=a-`, `bytes=-n`.
    const opts = { onlyIf: request.headers };
    if (wanted) opts.range = wanted.suffix != null ? { suffix: wanted.suffix } : { offset: wanted.start, length: wanted.end == null ? undefined : wanted.end - wanted.start + 1 };
    let obj;
    try {
      obj = await env.TILES.get(key, opts);
    } catch (e) {
      // R2 throws on a range that starts past the end; answer as HTTP does.
      const head = await env.TILES.head(key);
      if (head && wanted) return new Response("range", { status: 416, headers: { ...cors, "Content-Range": `bytes */${head.size}` } });
      throw e;
    }
    if (obj === null) return new Response("not found", { status: 404, headers: cors });

    const headers = new Headers(cors);
    obj.writeHttpMetadata(headers);
    headers.set("ETag", obj.httpEtag);
    headers.set("Accept-Ranges", "bytes");
    headers.set("Cache-Control", "public, max-age=86400, immutable");  // a pmtiles file is replaced, never edited
    headers.set("X-Cache", "MISS");
    headers.set("X-Worker", VERSION);
    let status = 200;
    if (wanted) {
      const size = obj.size;
      const start = wanted.suffix != null ? Math.max(0, size - wanted.suffix) : wanted.start;
      const end = wanted.suffix != null || wanted.end == null ? size - 1 : Math.min(wanted.end, size - 1);
      if (start >= size) return new Response("range", { status: 416, headers: { ...cors, "Content-Range": `bytes */${size}` } });
      headers.set("Content-Range", `bytes ${start}-${end}/${size}`);
      headers.set("Content-Length", String(end - start + 1));
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

function parseRange(h) {
  if (!h) return null;
  const m = /^bytes=(\d*)-(\d*)$/.exec(h.trim());
  if (!m) return null;
  if (m[1] === "" && m[2] !== "") return { suffix: Number(m[2]) };
  if (m[1] === "") return null;
  return { start: Number(m[1]), end: m[2] === "" ? null : Number(m[2]) };
}

// Does a stored Content-Range ("bytes a-b/n") describe exactly the range asked for?
// An entry without one is a whole-file answer and matches only a request without Range.
function sameRange(contentRange, wanted) {
  if (!wanted) return !contentRange;
  if (!contentRange) return false;
  const m = /^bytes (\d+)-(\d+)\/(\d+)$/.exec(contentRange);
  if (!m) return false;
  const start = Number(m[1]), end = Number(m[2]), size = Number(m[3]);
  if (wanted.suffix != null) return start === Math.max(0, size - wanted.suffix) && end === size - 1;
  const wantEnd = wanted.end == null ? size - 1 : Math.min(wanted.end, size - 1);
  return start === wanted.start && end === wantEnd;
}
