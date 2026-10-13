# Installing the airports responder as a container

The airports responder ([README](README.md) §4) as a Docker container, on any Linux host
that can reach a NATS server of the deployment: the hub, or a leaf node near its users.
It keeps a DuckDB replica of `airports` and answers `airports_in_view` and
`airports_near`.

The image is built from [deploy/airports-responder.Dockerfile](../../deploy/airports-responder.Dockerfile).
libzb comes from the ZeBridge release you name, checked against its `SHA256SUMS`; DuckDB
from its own release. No Zig build is needed.

## 1. Before you start

- The `airports` table exists and is enabled in the database ([README](README.md) §1).
- The host has Docker, and is x86_64 or aarch64.
- Behind a leaf node: the hub has a JetStream domain (DEPLOYMENT "Adding a leaf node").

## 2. The credentials

Where `operator.store` is kept (never on a server), mint the responder's creds. It
answers the open tenant `_default`, which every client may ask:

```sh
bridge --mint-responder --name airports-leaf --tenant _default --store operator.store > airports-leaf.creds
```

Copy the file to the host. The container runs as the image's `airports` user, so the file
must belong to that user's **uid**, not to a name on the host:

```sh
docker run --rm --entrypoint id zb-airports:0.1.7          # uid=999(airports): the number to use
sudo install -m 0600 -o 999 -g 999 airports-leaf.creds /etc/zebridge/creds/airports-leaf.creds
```

## 3. Build the image

From a clone of the repository, on the host itself (no registry needed). `ARCH` is the
host's CPU (`uname -m`), `ZB_VERSION` a ZeBridge release:

```sh
docker build -f deploy/airports-responder.Dockerfile \
  --build-arg ARCH=x86_64 --build-arg ZB_VERSION=0.1.7 -t zb-airports:0.1.7 .
```

The build needs only four paths of the repository: the Dockerfile and its
`.dockerignore`, `zb-python/src/zebridge/`, and `examples/10-airports/airport_service.py`.
A host without a clone can receive just those, with their paths kept.

## 4. Run it

```sh
docker run -d --name airports --network host --restart unless-stopped \
  -v /etc/zebridge/creds/airports-leaf.creds:/creds:ro \
  -v airports-data:/var/lib/airports \
  -e NATS_URL=tls://leaf.example.com:4222 \
  -e NATS_JS_DOMAIN=hub \
  -e ZB_SERVICE_NAME=airports-leaf \
  zb-airports:0.1.7
```

The replica is DuckDB by default (`airports.duckdb` in the volume). Add `--engine sqlite` after
the image name for SQLite, built into libzb, with no DuckDB at all (`airports.sqlite3`); it
needs libzb 0.1.7 or later, and says so at start otherwise.

- `NATS_URL`: the NATS server this host reaches, with TLS when the port has it.
- `NATS_JS_DOMAIN`: only behind a leaf node, the hub's domain. Leave it out on the hub.
- `ZB_SERVICE_NAME`: said in every answer (`by`), so you can tell instances apart.
- `airports-data`: the DuckDB replica. Keep this volume: a restart resumes from it in under
  a second; without it, the service seeds the 9,253 airports again.

Several instances, on the hub and on leaves, share the questions: they join the same queue
group (`airports`), and NATS gives each question to one of them.

## 5. Check

```sh
docker logs airports
```

```
info(libzb): airports-leaf: connected in 272 ms: …
replica: 9253 airports in /var/lib/airports/airports.duckdb, 0.3 s
answering ['airports_in_view', 'airports_near'] on query._default.<name>, queue group 'airports' (2 subject(s))
```

Two lines are expected and harmless:

- `airports-leaf has no mapping … public tables follow`: a responder has no tenant; it
  follows the public `airports` table only.
- after replacing a container, `ConsumerConfigMismatch … set aside`, then `✅ … readable
  again` a moment later: the new process met the old one's consumer and recovered.

## 6. Upgrade

Build the new release's image, then swap, keeping the old container for a rollback:

```sh
docker build -f deploy/airports-responder.Dockerfile \
  --build-arg ARCH=x86_64 --build-arg ZB_VERSION=0.1.7 -t zb-airports:0.1.7 .
docker rename airports airports-old
docker stop airports-old && docker update --restart=no airports-old
docker run -d --name airports …same options as above… zb-airports:0.1.7
```

Back to the old one: `docker rm -f airports && docker rename airports-old airports && docker
start airports`. Once the new one has run cleanly, remove the old container and image
(`docker rm airports-old`, `docker image rm zb-airports:<old version>`). Never remove the
`airports-data` volume unless you want the service to seed again.
