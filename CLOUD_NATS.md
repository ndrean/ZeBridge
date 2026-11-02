# ZeBridge on a hosted NATS

What ZeBridge asks of a NATS account it does not run itself, measured against Synadia Cloud's free tier. The aim is a hub whose NATS is hosted: the provider runs NATS and JetStream, a managed PostgreSQL runs in the same region, and the only things you run are the three ZeBridge processes (the bridge, mutation admission and the sweeper), next to the database.

**Status: not yet tried.** The figures below are the free tier's limits as the account form showed them on 2026-11-02, and ZeBridge's needs as its code creates them. Nothing has run against a hosted account yet.

## The question that decides it

At `/enroll`, the bridge signs each device's JWT with an account signing key, and the JWT carries the device's tenant grants (see [Enrollment](AUTHENTICATION.md#users-invites)). On a self-run hub, ZeBridge holds the operator and that key (`bridge --init-nats operator`). On a hosted account the provider holds the operator. So the account must let the bridge hold a **scoped signing key** whose template carries ZeBridge's per-tenant grants, or enrollment needs another path. This is the first thing to check.

## What ZeBridge uses on one account

Everything runs in **one account**: devices, the bridge, admission and responders, kept apart by subjects and by the grants in each JWT. ZeBridge uses no imports and no exports.

**Connections.** Each online device holds one; each hub process and each responder holds at least one.

**Streams.** A KV bucket and an object store are each a stream. The bridge creates:

| Stream | What it holds |
| --- | --- |
| `MUTATIONS`, `MUTATIONS_ERRORS` | the admission queue and its dead letters (admission mode) |
| `VERDICTS` | one verdict per write |
| `KV_schemas`, `KV_tenants`, `KV_generations` | table schemas, tenant mappings, chain manifests |
| `KV_time_manifests` | the readings archive's manifests |
| `KV_live` | the fleet monitor's presence bucket |
| `CDC_PUBLIC`, `OBJ_gen-_default` | the open tenant's changes and snapshots (the GC floor's table alone needs them) |
| per tenant: `CDC_<tenant>`, `OBJ_gen-<tenant>` | each tenant's changes and snapshot chains |
| per tenant, on demand: `OBJ_res-<tenant>` | a responder's answers too large for one message |

That is **10 streams before the first tenant, and 2 more for each tenant.**

**Consumers per stream.** Every online device holds one consumer on `CDC_PUBLIC` and one on its tenant's stream. A reconnect leaves the old one for about two minutes, and a cold start opens one per object of its chain (a full and its deltas), each kept for 5 minutes. In a test with 500 clients starting at once, one object store needed over 5,000 at its peak.

**Storage.** The bridge sets byte limits when it creates its streams: 1 GiB each for `MUTATIONS` and `VERDICTS`, `CDC_MAX_BYTES` (default 128 MiB) for each CDC stream. JetStream counts those limits against an account's storage when the account has a storage limit, so they need to fit in it before any data arrives. `CDC_MAX_BYTES` is a setting ([OPERATIONS](OPERATIONS.md#configuration)); the 1 GiB limits are fixed in the code.

## Synadia Cloud's free tier, against those needs

| Limit (free tier, 2026-11-02) | Value | What it means for ZeBridge |
| --- | --- | --- |
| Connections | 50 by default, 100 at most | about 95 devices online at once |
| Imports, exports | 5 / 1 by default, 10 / 2 at most | not used |
| Leaf node connections | 5 by default, 10 at most | one per regional leaf; a hub alone needs none |
| JetStream storage | 2.5 GB at most | the base topology's byte limits take about 2.3 GB of it, if reserved as above |
| Streams | 10 at most | the open tenant only: **no named tenant fits** |
| Consumers per stream | 100 at most | well under 100 devices online, counting reconnects and cold starts |
| Stream size | 5 GB at most | not a constraint |

So the free tier can answer the signing-key question and run a small demo whose tables have no tenant column. It cannot run a multi-tenant app such as the depot, nor a crowd of clients; those need a paid tier.

## What would make the fit easier

- **A smaller base topology.** Some of the 10 fixed streams could merge, or be created only when used: `KV_time_manifests` only with the readings archive, `KV_live` only with the fleet monitor.
- **Byte limits as settings,** so `MUTATIONS` and `VERDICTS` can be sized to a small account.

Neither is built.

## The trial

1. Create the account with every limit at its maximum, and look for scoped signing keys.
2. Point the bridge at it (`NATS_URL`, its credentials, the key for `/enroll`), with a managed PostgreSQL in the same region, and the bridge next to the database: writes pay their round trips to PostgreSQL, not to NATS.
3. Run an open-tenant demo end to end: enrollment, sync, a write and its verdict, a responder.
4. Record here what worked, what the account refused and why, then decide whether a paid tier or the changes above come first.
