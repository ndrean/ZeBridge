# Leaf nodes

A leaf node is a nats-server near a group of devices, joined to the hub's NATS. It runs no PostgreSQL and no bridge. Devices enroll at the hub's bridge as usual, then dial their leaf instead of the hub. A leaf is the unit you add per region: one server and one DNS name each.

There are two kinds. A **forwarding leaf** relays devices to the hub. A **seeding leaf** also serves their seeds from its own copies of the hub's snapshots.

## What each kind takes off the hub

| work | no leaf | forwarding leaf | seeding leaf |
| --- | --- | --- | --- |
| connections and TLS handshakes | hub | leaf | leaf |
| seeds: manifests and snapshot objects | hub | hub, through the leaf | **leaf** (hub as fallback) |
| live changes (`CDC_*` consumers) | hub | hub, through the leaf | hub, through the leaf |
| schemas, writes, verdicts, heartbeats | hub | hub, through the leaf | hub, through the leaf |
| enrollment and renewal | hub's bridge | hub's bridge | hub's bridge |

The streams stay on the hub in both cases: a leaf is a gateway to them, not a second copy. Live changes would arrive no sooner from a copy, since a copy receives them over the same link.

## A forwarding leaf

The leaf holds every device connection and its TLS session, and passes the devices' requests to the hub. It has no JetStream: a device reaches the hub's JetStream through the domain it enrolled with (`js_domain`, `hub` by default).

What it gives: the hub holds a handful of leaf links instead of thousands of device connections, and a reconnect storm hits the leaves first and reaches the hub spread over time. A device's first contact (DNS, TCP, TLS) is with a nearby server.

## A seeding leaf

The leaf also has its own JetStream, under its own domain (for example `strasbourg`). It holds mirrors of the hub's snapshots: the manifests (`KV_generations`) and the chain objects of each tenant it serves (`OBJ_gen-<tenant>`, the public tables' `OBJ_gen-_default` included). A mirror is a read-only copy with the hub's own sequence numbers, kept up to date by the leaf's nats-server.

A device on the leaf seeds from these copies: the manifest, then the objects it names. Everything else still goes to the hub. When the leaf cannot serve a seed (it is down, its copy is behind the hub's stream, or an object has not arrived yet), the device seeds from the hub instead. A seed never waits on the leaf.

What it gives, beyond a forwarding leaf: the seed's bytes leave the hub, and the round trips with them. libzb reads an object 1 MB per request, so a far hub costs one long round trip per megabyte; a near leaf makes a large seed much faster. This is the leaf that lets a fleet grow by region: each new region adds its own seeding capacity, and the hub's share per device shrinks to the live changes.

### Three containers

| container | image | does |
| --- | --- | --- |
| `nats` | `nats` | the leaf's nats-server: devices' connections with TLS (4222 for native clients, a websocket for browsers), the link to the hub, and the leaf's JetStream with the mirrors |
| `certbot` | `certbot/certbot` | the leaf's certificate from Let's Encrypt on port 80, renewed twice a day; nats-server reloads it without dropping a device |
| `mirrors` | `natsio/nats-box` | the keeper of the mirrors (below); idle on a forwarding leaf |

There is no ZeBridge image on a leaf: the scripts are written inside `compose.yml`. The leaf's state is two volumes, the certificate and the JetStream store; both rebuild themselves if lost (a new certificate, mirrors copied again from the hub).

### Two files from the hub

A leaf trusts what the hub trusts, and presents its own identity to it. Both come from the hub's operator, put in `hub/` next to `compose.yml`:

- **`trust.conf`**: the hub's operator and its accounts, the lines of the hub's `nats-server.conf` from `operator:` to the end of `resolver_preload`. The leaf checks each device's JWT against them, as the hub would. They change when the hub's account is signed again (`--init-nats --update`); copy them to every leaf then, and restart its nats-server.
- **`leaf.creds`**: the leaf's own credential, minted where `operator.store` is kept: `bridge --mint-leaf --name <leaf> --mirror`. It is a secret, like a device's identity, kept 0600.

What the credential allows on the link to the hub:

| it may | so that |
| --- | --- |
| carry what any device or responder may (each tenant read as "any") | the devices behind the leaf reach the hub |
| create a consumer on any hub stream, receive its deliveries (`$JS.M.…`), receive the answer to its creation (`$JSC.R.…`), and pace it (`$JS.FC.…`) | a mirror copies its stream from the hub |
| list the hub's streams | the keeper finds the snapshot streams |
| create, list and delete streams in its own JetStream (`$JS.API.…`) | the keeper manages the mirrors |

It never may write a CDC event, or create or delete a stream on the hub: with a JetStream domain on both sides, `$JS.API.…` stays on the leaf, and `--mirror` refuses a hub without a domain. The devices' own JWTs allow the seed reads in any domain (`$JS.*.API.…`), read-only and for their own tenants: the hub's account carries them once it is signed (`--init-nats --update`) by a bridge that has them.

### The keeper

The `mirrors` container checks every 30 s, with the leaf's credential:

1. **It lists the hub's snapshot streams**: `KV_generations`, `OBJ_gen-_default`, and each `OBJ_gen-<tenant>` (only those of `LEAF_TENANTS`, when set).
2. **It creates the mirrors the leaf lacks.** A mirror's settings are copied from the hub stream's own (one message per key for the manifests, rollups, direct reads), without its subjects, with the hub stream as source. A mirror with other settings would not read as a KV bucket or an object store.
3. **It removes the mirrors the hub no longer has**, or that are not this leaf's tenants.

If the hub's list does not include `KV_generations` (the hub unreachable, a partial answer), it changes nothing: a network failure never empties a leaf. A new tenant is mirrored within 30 s of its first snapshot; until then its devices seed from the hub. `docker compose logs mirrors` names every change.

### Sizing a leaf

| limit | where | what sets it |
| --- | --- | --- |
| devices connected | the leaf's memory | about 1 MB per device in nats-server: a 2 vCPU, 4 GB leaf holds a few thousand |
| live-change consumers | the **hub's** streams, shared by all leaves | about two per device on each stream it follows, `CDC_PUBLIC` holding every tenant's; the hub caps a stream at 5,000 |
| seed consumers | the leaf's mirrors | one per seed in progress, reaped 5 min after it goes idle; the leaf caps a stream at 5,000 |
| disk | the leaf's JetStream store | the size of the mirrored snapshots: the tenants it serves, plus the public tables |
| CPU | the leaf | TLS handshakes and seed reads during a storm: 130% of 2 vCPU for 250 devices at once |

The hub's limits do not grow with the leaves: the live-change consumers of every region land on the hub's streams. Raise the hub's `default_max_consumers` as the whole fleet grows.

## Measured

On 2026-10-18: a hub in Frankfurt (6 vCPU), a leaf in Strasbourg and one in Canada (2 vCPU each), 500 devices starting at once with empty replicas, from a Mac in France, each seeding 10,000 rows. CPU-seconds are the hub's nats-server CPU summed over the run.

| run | hub CPU, busiest second | hub CPU-seconds | synced |
| --- | --- | --- | --- |
| no leaf | 450% | 53 | 500 of 500 |
| one forwarding leaf (Canada) | 210% | 24 | 499 of 500 |
| two forwarding leaves, 250 each | 180% | 16 | 491 of 500 |
| two seeding leaves, 250 each | 160% | 10 | 500 of 500 |

One device starting through Strasbourg was synced in 1.3 s, against 7.5 s through Canada. The forwarding leaves took the largest share (connections and TLS); the mirrors took a third of what was left (the seeds), and removed the timeouts of long-distance seed requests. What remains on the hub is mostly JetStream work: the consumers of live changes, schemas and reads.

## Setting one up

Both kinds run from `deploy/compose/leaf`: see the compose [README](deploy/compose/README.md#a-leaf-node), sections "A leaf node" and "A seeding leaf". The Ansible playbook `deploy/ansible/leaf.yml` sets up a forwarding leaf.

The hub needs a JetStream domain and a leafnodes listener on port 7422, open to each leaf's addresses. A hub set up with Ansible has both; the compose stack of `deploy/compose` has neither yet.

A device on a leaf is given the leaf's address (`natsUrl`) and, for a seeding leaf, its domain (`seedDomain`). Enrollment does not hand these out yet: the app passes them.
