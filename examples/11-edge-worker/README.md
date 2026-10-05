# 11 — an edge Worker asks a responder

A Cloudflare Worker that asks the airports service of [10-airports](../10-airports) a
question over NATS and returns the answer: an HTTP request at the edge becomes a NATS
request to a DuckDB responder, with no replica in the Worker. It uses nats.js directly, the
NATS client inside zb-client-ts.

```
GET /?lat=50.11&lng=8.682
{"connect_ms":149,"request_ms":31,"service_ms":7,"count":5,"bytes":420,
 "airports":["FRA","WIE","MHG","HDB","SGE"]}
```

## Run it

```sh
npm install
PYTHONPATH=../../zb-python/src python3 enroll.py --bridge https://bridge.example.com \
    --invite <code> --ws wss://ws.example.com
npx wrangler dev                    # Cloudflare's runtime (workerd), on this machine
curl 'http://localhost:8787/?lat=50.11&lng=8.682'
```

`enroll.py` redeems an invite once, with libzb, and writes the principal's creds to
`.dev.vars`, where `wrangler dev` reads local secrets. A deployed Worker takes them as a
secret (`wrangler secret put ZB_CREDS_B64`), never in its code or its configuration.

## What it shows

* **The Worker speaks the grants.** It connects over WebSocket with the principal's creds and
  sets its inbox prefix to `_INBOX.<principal>`: a client may receive replies only there.
* **Answers are compressed.** The service sends zstd frames; the Worker decodes them with
  fzstd, as zb-client-ts does.
* **The connection is the cost.** Deployed (2026-10-05), served from Cloudflare's Paris
  location, the hub in Frankfurt: the question takes 21–25 ms, DuckDB's 6–10 ms included;
  connecting takes 70–240 ms (the WebSocket, TLS, the server's challenge signed with the
  seed), about 80 ms once warm. A Worker connects on every request, since nothing lives
  between requests: a call costs about a quarter of a second, most of it connection. A Durable
  Object could hold one connection for many requests, and a replica in its SQLite.
* **The Worker does not enroll.** `enroll.py` redeemed the invite once, on the developer's
  machine; the Worker only reads the resulting creds from its secrets, and never contacts the
  bridge.

## What it does not show

* zb-client-ts at the edge: it opens a replica and syncs on `connect()`, and has no "ask
  only" mode yet. This uses nats.js the way zb-client-ts's `request()` does.
* Renewal. The creds of an invite last a day (`ENROLL_JWT_TTL_SECONDS`); a phone renews its
  own through `/renew`, but a Worker cannot rewrite its secret. An edge function in production
  needs a longer-lived identity, or to keep its JWT where it can renew it (a KV namespace, a
  Durable Object).
