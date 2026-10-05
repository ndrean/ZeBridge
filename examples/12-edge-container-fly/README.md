# 12 — the responder as an edge container (Fly.io)

The airports responder of [10-airports](../10-airports), as a container
([deploy/airports-responder.Dockerfile](../../deploy/airports-responder.Dockerfile)), on
Fly.io: libzb, DuckDB as its replica, and the service, in a machine near the users. The same
image runs on a leaf node's host, any Kubernetes, or another edge container platform.

It opens no port. It dials out to NATS, seeds its replica from the hub's chain, follows CDC,
and joins queue group `airports` beside the other responders.

## Run it

Build the image, for the machine's CPU, in the Docker engine `flyctl` will read:

```sh
deploy/build-linux.sh                                    # libzbcore.so for x86_64
docker build --platform linux/amd64 -f deploy/airports-responder.Dockerfile \
  --build-arg ARCH=x86_64 -t zb-airports .
```

A responder identity, minted where `operator.store` lives (ten years: no renewal to arrange):

```sh
bridge --mint-responder --name airports-fly --tenant _default --store operator.store > airports-fly.creds
```

Then, from this folder (set `app` and `NATS_URL` in `fly.toml` first):

```sh
fly apps create zb-airports-test
base64 < airports-fly.creds | tr -d '\n' | sed 's/^/ZB_CREDS_B64=/' | fly secrets import --stage -a zb-airports-test
rm airports-fly.creds
fly deploy --local-only -a zb-airports-test               # pushes the local image
fly logs -a zb-airports-test                              # replica: … airports; answering …
```

The creds become a Fly secret, written into the machine as `/creds`. Fly's dashboard deploys
from a repository; `flyctl` deploys a local image. With OrbStack, point it at OrbStack's
engine: `DOCKER_HOST=unix://$HOME/.orbstack/run/docker.sock fly deploy …`.

When done: `fly apps destroy zb-airports-test`, then `bridge --revoke airports-fly`.

## What it measured

Fly region `cdg` (Paris), shared CPU, 512 MB; the hub in Frankfurt; 2026-10-05.

* Seed: 9,253 airports in 4.2 s (1.9 s on a leaf 4 ms from the hub).
* DuckDB: 4.8–10.6 ms per question, as on the hub.
* 30 questions asked through the hub: the hub's service answered 17 (33 ms round trip),
  `airports-fly` 13 (52 ms).

## What it shows

**A responder serves fast the askers connected to its own NATS server.** Joined directly to the
hub, the Fly machine is one more member there: NATS shares the hub's questions between it and
the hub's own service at random, and every answer from Paris detours through Frankfurt
(asker → hub → Paris → hub → asker), 19 ms more. Placed near the users but reached through the
hub, it is slower, not faster.

The shape that helps is a NATS leaf beside the responder, with the region's devices connecting
to that leaf: their questions are then answered without leaving the region, as on a leaf
node's host (`deploy/ansible/leaf.yml`, then this image beside it). On Fly that is a second
process, `nats-server` as a leaf, in the same machine or a second one: not built here.
