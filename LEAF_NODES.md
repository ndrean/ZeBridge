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

What it gives, beyond a forwarding leaf: the seed's bytes leave the hub, and the round trips with them. libzb reads an object 1 MB per request, so a far hub costs one long round trip per megabyte; a near leaf makes a large seed much faster.

The leaf keeps its own mirrors. A `mirrors` service checks every 30 s: it adds a mirror for each snapshot stream of the hub the leaf should hold, made from the hub stream's own settings, and removes those the hub no longer has. The hub does not know its leaves: adding a region changes nothing on the hub.

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

Each leaf presents its own credentials to the hub (`bridge --mint-leaf`, with `--mirror` for a seeding leaf), minted where `operator.store` is kept, and a copy of the hub's trust lines. After every `--init-nats --update` on the hub, copy the trust lines to every leaf again.

A device on a leaf is given the leaf's address (`natsUrl`) and, for a seeding leaf, its domain (`seedDomain`). Enrollment does not hand these out yet: the app passes them.
