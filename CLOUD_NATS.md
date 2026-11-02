# ZeBridge on a hosted NATS

What ZeBridge asks of a NATS account it does not run itself, measured against Synadia Cloud's free tier. The aim is a hub whose NATS is hosted: the provider runs NATS and JetStream, a managed PostgreSQL runs in the same region, and the only things you run are the three ZeBridge processes (the bridge, mutation admission and the sweeper), next to the database.

**Status: not yet tried.** The figures below are the free tier's limits as the account form showed them on 2026-11-02, and ZeBridge's needs as its code creates them. Nothing has run against a hosted account yet.

## The question that decides it

At `/enroll`, the bridge signs each device's JWT with an account signing key, and the JWT carries the device's tenant grants (see [Enrollment](AUTHENTICATION.md#users-invites)). On a self-run hub, ZeBridge holds the operator and that key (`bridge --init-nats operator`). On a hosted account the provider holds the operator. So the account must let the bridge hold a **scoped signing key** whose template carries ZeBridge's per-tenant grants, or enrollment needs another path. This is the first thing to check.

**What Synadia Cloud offers** (its Control Plane documentation, 2026-11-02): signing keys come in **Signing Key Groups**, set per account, not on the account creation form. Every user belongs to a group; a **scoped** group carries permissions and limits that its users inherit, which is the model ZeBridge's client template uses (`{{tag(tenant)}}`, `{{name()}}`). A user created in the Control Plane can download its JWT and credentials. The documentation does not say whether a group's private seed can be exported, so that an application signs its own users.

**Questions for Synadia:**

1. Can a scoped signing key's seed be exported, or a key we generate be added to the account, so that our service signs user JWTs itself (one per device, at enrollment)?
2. Do scoped templates accept the tag functions our grants use, `{{tag(tenant)}}` and `{{name()}}`?
3. If not: can a service create users through the Control Plane's API at the rate devices enroll, and what does a user cost against the account's limits?
4. Do the JetStream limits count a stream's `max_bytes` against the account's storage when the stream is created, or only the bytes stored?
5. What do the paid tiers allow in streams, consumers per stream and connections per account?

If the answer to 1 is no and to 3 is yes, enrollment would ask the Control Plane's API for each device's user instead of signing it in the bridge: a change in the code, not built.

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

From a new account's JWT (2026-11-02), which is what NATS enforces, and the account form's maximums:

| Claim | As created | At most | What it means for ZeBridge |
| --- | --- | --- | --- |
| `conn`, connections | 5 | 10 | a few devices online, beside the hub's own processes |
| `subs`, subscriptions (account-wide) | 50 | 100 | each device holds several (its CDC tails, verdicts, its inbox) |
| `payload`, message size | 512 KiB | | the bridge sizes its messages from the server's limit, not the account's: a larger event or object chunk would be refused |
| `leaf`, leaf connections | 1 | | one per regional leaf; a hub alone needs none |
| `imports` / `exports` | 7 / 1 | | two imports are Synadia's own (metrics); ZeBridge uses none |
| JetStream `streams` | 5 | 10 | the open tenant only: **no named tenant fits** |
| JetStream `consumer`, per stream | 50 | 100 | well under that many devices, counting reconnects and cold starts |
| JetStream `disk_storage` | 2.5 GiB | | the base topology's byte limits take about 2.3 GiB of it |
| JetStream `disk_max_stream_bytes` | 5 GiB | | not a constraint |
| JetStream `max_bytes_required` | true | | **every stream must declare a byte limit when created**; the bridge creates its KV buckets and object stores without one, so the account would refuse them |

**Probed with the bridge's user** (2026-11-02, nats-server v2.15.2 at `tls://connect.ngs.global`): a KV bucket created without a byte limit is refused, `err_code=10113 'account requires a stream config to have max bytes set'`; the same bucket with a 1 MiB limit is created and written. The R1 tier allows no memory storage (`max_memory 0`), so a stream ZeBridge kept in memory would be refused too. The server announces an 8 MiB `max_payload`, while the account allows 512 KiB.

The account's JWT lists two signing keys, both plain (unscoped) public keys: Synadia's, whose seeds it holds, used for the users you create in its interface. Neither can sign the bridge's device JWTs.

So the free tier can answer the signing-key question, and with the changes below run a small demo whose tables have no tenant column. It cannot run a multi-tenant app such as the depot, nor a crowd of clients; those need a paid tier.

## What would make the fit easier

- **A smaller base topology.** Some of the 10 fixed streams could merge, or be created only when used: `KV_time_manifests` only with the readings archive, `KV_live` only with the fleet monitor.
- **Byte limits as settings,** so `MUTATIONS` and `VERDICTS` can be sized to a small account.
- **A byte limit on every stream the bridge creates,** the KV buckets and object stores included: required by an account with `max_bytes_required`.
- **The account's message size,** not only the server's, in the bridge's sizing: the server announces 8 MiB, the account allows 512 KiB.
- **File storage for every stream,** if any is kept in memory: the free tier allows none.

None is built.

## The trial

1. Create the account with every limit at its maximum, and look for scoped signing keys.
2. Point the bridge at it (`NATS_URL`, its credentials, the key for `/enroll`), with a managed PostgreSQL in the same region, and the bridge next to the database: writes pay their round trips to PostgreSQL, not to NATS.
3. Run an open-tenant demo end to end: enrollment, sync, a write and its verdict, a responder.
4. Record here what worked, what the account refused and why, then decide whether a paid tier or the changes above come first.
