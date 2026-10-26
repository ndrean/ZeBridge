# Leaf nodes

A leaf is a regional NATS server. PostgreSQL, the bridge, authoritative CDC streams, mutations and verdicts remain on the hub. A forwarding leaf relays client traffic. A seeding leaf also runs JetStream under a distinct domain and caches the snapshots needed by its explicitly authorized tenants.

| Work | Forwarding leaf | Seeding leaf |
| --- | --- | --- |
| Connections and TLS | leaf | leaf |
| Manifest and snapshot reads | hub through the link | local cache, with hub fallback |
| CDC, schemas, mutations and verdicts | hub through the link | hub through the link |
| Enrollment, renewal and signed account feed | hub HTTPS | hub HTTPS |

A leaf's JetStream is a cache, not an authority for client writes. Clients use `jsDomain: "hub"` and, on a seeding leaf, `seedDomain: "<leaf-domain>"`. A failed cache read falls back to the hub through the existing connection. Enrollment also supplies ordered connection endpoints: the assigned leaf first, then the hub. If the leaf cannot be reached or its connection fails, the client tries the hub with the same credentials, replica and queued writes. It stays on a working hub connection when the leaf returns. A live leaf whose link to the hub has failed does not trigger a connection switch yet; hub-domain work waits for that path to recover. The hub remains the authority and must be available for writes and live CDC.

A leaf never mints a cut, an incarnation or an epoch: the manifests it mirrors keep their hub meaning, and a client that recovers from a leaf's chain proves everything against the hub's stream (the incarnation the chain was cut on, the event after its cut still retained). That includes a **fence**: a follower whose position is already past the leaf's chain (a busy table) rewinds its cursors to that chain's cut and replays the suffix from the hub, with the leaf's objects as the snapshot; the hub's newer chain is not needed unless the leaf's is invalid (an older epoch, or a suffix the hub no longer retains). A leaf whose manifest arrives before its objects (two mirrors) is checked object by object before anything changes, and the client uses the hub's chain instead. A leaf manifest that names an earlier incarnation of the CDC stream (a copy left from before a recreate) is refused the same way, and the hub's chain is used. There is no age cutoff on a replayable leaf chain: an arbitrary one would send a storm of clients back to the hub. The cost is reported instead — each fence logs the chain's age and the CDC messages left after its cut, and the `chain` measure event carries `cut_age_ms` and `cdc_behind` — so a cost-based fallback can be decided on evidence. `scripts/scenarios/leaf_fence.py` runs these cases on a local hub and leaf.

## Tenant and identity boundaries

An administrator registers each leaf in PostgreSQL, after applying the current `bridge --init-sql` output:

```sql
INSERT INTO public.zebridge_leaf_scopes(leaf_id, tenant_ids, nats_url, nats_ws_url)
VALUES ('lf-leaf1', ARRAY['demo','storm'],
        'tls://leaf1.example.com:4222', 'wss://leaf1.example.com');
```

Use the **same explicit tenant list** in this row, credential minting and deployment configuration. Empty configuration fails closed; it never means all tenants. Public `_default` data remains available. A leaf id starts with `lf-` (`leaf_prefix` in the grammar) and no other principal may, so a plain name can never shadow a leaf's namespace: clients and responders behind `lf-leaf1` use names such as `lf-leaf1.alice` and `lf-leaf1.routing`. PostgreSQL, the mint commands and the mutation subject parser all enforce it. A leaf cannot relay a mutation as a root identity or another leaf's identity. PostgreSQL guards reject memberships and invitations outside the registered list, including direct edits through privileged application roles. They also reject reusing a historical root principal as a leaf namespace and shrinking a policy while incompatible memberships remain.

The registry is the canonical enrollment policy. JWT grants are fixed when minted: editing a registry row does not revoke existing credentials. Retire memberships and keys and clear obsolete cached data as part of any tenant removal. A compromised leaf remains trusted for its configured tenants and principal namespace. Do not install unrestricted hub client or service credentials on a leaf; their bearer authority can be delegated by anyone possessing them.

For a scoped principal, `/enroll` and `/renew` return `nats_urls` and `nats_ws_urls`: the registry's regional URL followed by the hub's `ENROLL_NATS_URL` or `ENROLL_NATS_WS_URL`, with duplicate addresses removed. Root principals receive the hub endpoints. Singular `nats_url` and `nats_ws_url` carry the first address for clients that do not read lists. Both libraries persist the lists in the identity; reopening uses them without an HTTP request while the JWT remains valid. Renewal saves changed addresses for the next client instance; the active connection pool keeps the list it opened with. An explicit `natsUrl` replaces the entire enrolled list. The app still supplies `seedDomain`. Existing root identities remain usable on the hub; moving them behind a restricted leaf requires new scoped identities and replicas. Drain or deliberately export pending local outboxes before changing an identity; do not discard an offline replica with unsubmitted writes.

## Credentials and trust from the hub

Keep the offline `operator.store` off every leaf. Mint two different keys with the **same leaf name and tenant list**:

```sh
bridge --mint-leaf --name lf-leaf1 --tenant demo --tenant storm --mirror --store operator.store > leaf.creds
bridge --mint-leaf --name lf-leaf1 --tenant demo --tenant storm --keeper --store operator.store > keeper.creds
chmod 600 leaf.creds keeper.creds
```

Omit `--mirror` for a forwarding link; it needs no keeper. `leaf.creds` is restricted to LEAFNODE connections. It cannot authenticate on a client port or create/delete hub snapshot streams. Its hub API, CDC, readings, inbox and verdict grants are limited to its tenants and principal namespace. Literal `OBJ_res-<allowed-tenant>` creation remains permitted for responders' large results. It cannot create consumers on the shared `KV_generations` stream, because a consumer's body filter is not a subject permission boundary.

`keeper.creds` accepts ordinary connections from loopback only and manages known local cache streams. It cannot administer hub-domain streams or authenticate as a leaf link. Its private reply prefix is `$ZB.KEEPER.<leaf>`. Docker shares the NATS network namespace; Ansible runs the keeper beside NATS. Both dial the leaf's certificate name mapped to loopback, preserving TLS verification.

The offline `trust.conf` pins the operator and account. The hub serves only its public current account claim at `GET /nats-account` when `NATS_ACCOUNT_CONF` is set. Both deployments use the same `leaf_trust.py` supervisor: HTTPS verification, operator signature verification, account identity, increasing claim revisions and monotonic revocations are checked before NATS validates and reloads a candidate. Operator/SYS trust stays pinned. Persisted state retains revocations and the verified lease across restarts. Credential tags must match the configured leaf and tenants.

The supervisor pauses NATS after **300 seconds without a verified refresh**, then restarts it automatically when trust refresh succeeds. Devices retain their local replicas during that outage. This bounds stale account authentication after feed loss; it is not a substitute for protecting the hub and its HTTPS feed. Treat operator/account replacement as a separate manual trust migration. A normal, nonrenewable device JWT stops working at its signed expiry, but `ENROLL_JWT_TTL_SECONDS` is a bridge issuance setting, not a server-enforced maximum for an attacker holding a signing seed. A stale leaf may accept freshly forged or much longer-lived JWTs. Losing the HTTPS feed also does not imply losing the NATS data link: a stale leaf can still forward an authenticated client's allowed mutation traffic to a live hub, which authenticates the leaf link rather than the downstream JWT. The `leaf_trust` scenario reproduces that delivery path; it does not assert a PostgreSQL commit. Keeping leaves online indefinitely with stale trust would therefore be an explicit availability tradeoff, not a general revocation bound equal to normal client lifetime.

## How seeding caches are maintained

The hub creates one filtered **source projection** per tenant, `KV_generations_leaf_<tenant>`, from the shared manifest bucket. The local `KV_generations` combines only the leaf's allowed projections and `_default`. It never mirrors the complete shared bucket. Object caches mirror `OBJ_gen-<allowed-tenant>` and `OBJ_gen-_default`, receiving data on `$ZB.MIRROR.<leaf>.>`.

NATS 2.15 requires `feature_flags { js_api_reply_v2: true }` on the leaf so mirror control replies include the stream name and can be authorized narrowly. The deployment templates enable it. The shared `mirrors.sh` keeper checks the known origin names every 30 seconds, validates configurations, and creates missing local caches. It never deletes caches based on a missing origin, a listing, or a network error. Legacy/unexpected configurations are refused for explicit migration. Monitor disk usage and replication lag; cache retirement and out-of-policy data removal are deliberate operator actions.

Query replies use `_INBOX.<principal>.query.<tenant>.<nonce>`. libzb's C ABI and TS transport generate that internally, without changing public request APIs or mutation messages. Update native/shim and TS clients **before** installing restricted leaf keys: older query clients use generic inboxes that the restricted link cannot authorize safely.

## Deployment and migration

Docker uses all files in `deploy/compose/leaf/`, including its Dockerfile, trust supervisor and keeper. Ansible's `leaf.yml` supports both forwarding and seeding leaves. See the [Compose guide](deploy/compose/README.md#a-leaf-node) and [Ansible guide](deploy/ansible/README.md). Build bridge/client binaries and images from these sources before deploying; old published images do not implement the new commands.

For an existing installation: stage updated clients, apply PostgreSQL guards and register explicit scopes, prepare hub manifest projections, migrate the mutation queue as described in [Operations](OPERATIONS.md#mutation-admission-cutover), then install scoped identities and both new leaf keys. Preserve old public user keys before replacing files. Ansible records retired keys in `/etc/zebridge/retired-leaf-keys`; revoke them on the hub after verified cutover, reload NATS and verify every leaf has refreshed trust. Replacing a file alone does not invalidate a stolen credential. Existing broad caches must be retired/rebuilt explicitly after preserving any needed diagnostics; the keeper intentionally does not erase them automatically.

The hub requires a JetStream domain and a leaf listener on 7422 restricted to the leaf IPs. Native NATS DNS records must bypass Cloudflare's HTTP proxy; HTTPS/WSS use the configured proxy and strict origin TLS.

## Capacity and recovery limits

Leaves offload connection/TLS and snapshot bytes. CDC consumer work, PostgreSQL mutation work and global queue/storage limits remain on the hub. Choose disk and consumer budgets from measured tenant sizes and reconnect loads. Neither two leaves nor mirrored seeds makes the single hub highly available. [Operations](OPERATIONS.md#hub-checkpoints-and-recovery) describes coordinated checkpoints and rebuilding a fresh feed; the recovery point is the last completed checkpoint, not every later acknowledged write.

## Historical measurements

These measurements predate the restricted leaf credentials, filtered manifest projections and trust supervisor. They have not been rerun with this hardening. On 2026-10-18: a hub in Frankfurt (6 vCPU), a leaf in Strasbourg and one in Canada (2 vCPU each), 500 devices starting at once with empty replicas, from a Mac in France, each seeding 10,000 rows. CPU-seconds are the hub's nats-server CPU summed over the run.

| run | hub CPU, busiest second | hub CPU-seconds | synced |
| --- | --- | --- | --- |
| no leaf | 450% | 53 | 500 of 500 |
| one forwarding leaf (Canada) | 210% | 24 | 499 of 500 |
| two forwarding leaves, 250 each | 180% | 16 | 491 of 500 |
| two seeding leaves, 250 each | 160% | 10 | 500 of 500 |

One device starting through Strasbourg was synced in 1.3 s, against 7.5 s through Canada. The forwarding leaves took the largest share (connections and TLS); the mirrors took a third of what was left (the seeds), and removed the timeouts of long-distance seed requests. What remains on the hub is mostly JetStream work: the consumers of live changes, schemas and reads.
