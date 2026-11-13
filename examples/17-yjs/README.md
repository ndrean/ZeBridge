# 17 — collaborative text with Yjs

Two editors can change the same text offline and keep both edits when they reconnect.
Yjs merges text; ZeBridge persists the updates locally, uploads them, and replicates
the PostgreSQL rows. There is no Yjs server and no change to the bridge's LWW rules.

## Storage model

`setup.sql` creates one tenant-scoped table, `yjs_updates`. Each row contains:

| Column | Purpose |
| --- | --- |
| `id` | A fresh UUID for this immutable update |
| `document_id` | Groups updates belonging to one Yjs document |
| `update_data bytea` | The binary update produced by Yjs |
| `tenant_id`, `updated_at`, `last_writer`, `deleted_at` | The ordinary ZeBridge scope and write metadata |
| `replaces` | Only on a [client compaction](#from-the-editor)'s INSERT: the JSON array of IDs it merged. Always NULL once stored |

There is no document table in this minimal example: the `doc` URL parameter selects
a document UUID. A real application can keep document titles, access rules and workflow
state in a separate ordinary table.

**Adding a replaceable BYTEA column to a shared document row is insufficient.** Two
writers replacing that row still compete under LWW. Here they insert different rows,
so both updates survive; the browser's Yjs library determines their combined meaning.
A PostgreSQL trigger prevents changing an update's bytes or scope. Clients append updates;
rows are retired only by a compaction (from the editor, or by the table owner), and the
existing sweeper can physically remove their tombstones. Identical retries may still advance a row's
version/tiebreak.

The TypeScript API sends and receives `Uint8Array`. CDC carries MessagePack binary;
SQLite stores BLOBs. The client's JSON-backed outbox encodes bytes internally using
the existing `{"$bin":"<base64>"}` convention and restores them before publishing.
The application does not encode Base64 or compress individual updates.

## Run against an existing stack

Use a current bridge with a writable PostgreSQL connection, a named publication, and
generations enabled. The bridge's init SQL must already be installed. See
[example 15](../15-shared-record/README.md#run-it) for the local dev stack, or the
[deployment guide](../../DEPLOYMENT.md) for an authenticated stack.

Run as the DBA, using the same publication as the bridge:

```sh
psql "$ADMIN_DATABASE_URL" -v ON_ERROR_STOP=1 -v publication=my_pub \
  -f examples/17-yjs/setup.sql
```

Run this setup once. It creates and enables the table without deleting existing data.
Check the `zebridge_enable` result for errors. No initial Yjs row needs to be seeded.
The bridge detects the new table through its existing catalogue/DDL handling.

For an unauthenticated local NATS dev stack, map both editor names to one tenant:

```sql
INSERT INTO public.zebridge_user_tenants (principal, tenant_id)
VALUES ('alice', 'acme'), ('bob', 'acme') ON CONFLICT DO NOTHING;
```

Install the client and the example separately (the example links the local client):

```sh
cd zb-client-ts
pnpm install --frozen-lockfile
cd ../examples/17-yjs/web
pnpm install
ZB_NATS_WS_ORIGIN=ws://127.0.0.1:8080 \
ZB_BRIDGE_ORIGIN=http://127.0.0.1:27434 pnpm dev
```

The NATS WebSocket default is `8082`, matching example 15; `--init-nats dev` uses
`8080`, hence the explicit override above. Open:

- <http://localhost:5181/?as=alice>
- <http://localhost:5181/?as=bob>

Each editor name has its own identity and OPFS replica. Use one tab per editor name.
Both links select the same default document. A **New document** link generates a UUID;
copy that `doc` parameter to the other editor to share it.

For an authenticated stack, enroll each editor with its own one-time invite for the
same tenant: `?as=alice&invite=…` and `?as=bob&invite=…`. The `as` parameter names the
local replica; credentials determine the actual principal. Existing identities are
reused on later visits. For a deployed build, set `VITE_ZB_BRIDGE_URL` and, if needed,
`VITE_ZB_NATS_URL`. Static hosting must send the isolation headers in `web/public/_headers`.

## Observe the merge

1. Type a sentence as Alice. Bob receives it.
2. Click **Go offline** in both editors. This actually closes their NATS connections.
3. Insert different words at the same position in each editor. Each now has a different
   text and pending uploads; the updates are stored in its local replica/outbox.
4. Reconnect both. Their text converges, retaining both insertions. Yjs decides the
   order of concurrent insertions.
5. Reload a tab. The text reconstructs from stored binary updates. Offline reload of
   the page itself is not provided: this example has no service worker.

The footer reports update rows, payload bytes and pending uploads. **Download text**
exports the visible document, including any work that needs manual recovery.

Inspect PostgreSQL:

```sql
SELECT document_id, count(*) AS updates, sum(octet_length(update_data)) AS payload_bytes
FROM public.yjs_updates GROUP BY document_id;
```

## Potential improvement: durable client-side batching

**Planned, not implemented.** The current provider immediately queues each emitted
Yjs update through ZeBridge, so ordinary typing typically creates one PostgreSQL row
per keystroke. A future improvement is to combine this client's unpublished updates
into fewer rows while keeping the backend unaware of Yjs.

The intended flow is:

```text
local Yjs edits → durable local buffer → merged batch in durable outbox → one new PostgreSQL row
```

Offline write durability is a requirement of this improvement:

- Persist each edit before reporting it as locally saved. A timer or an in-memory
  debounce alone is insufficient; page reloads and process crashes must not lose
  acknowledged local edits. Surface persistence failures explicitly.
- Batch only local, unpublished updates for the same tenant and document. Choose
  bounded time/size limits so merged payloads still fit ZeBridge's row limits.
- Transfer the batch into the existing durable outbox atomically with retiring its
  buffer entries, or use a recoverable, idempotent handoff. A crash between these
  steps must leave a durable copy of every edit.
- Persist the batch's identity and exact payload before publishing. Once publication
  has been attempted, retries must use that same identity and payload; later edits
  belong in a new batch. Keep a durable copy until server acceptance is confirmed.
- Restore pending buffers and sealed batches after restart and reconnect, using the
  same behavior online and offline. Client batching grants no authority to prune
  accepted PostgreSQL rows.

Acceptance checks must kill/restart the client before and during batch handoff, before
and after publication, and after server acceptance but before its acknowledgment.
They must also cover storage failures, repeated retries, and concurrent remote edits:
every locally saved edit survives recovery and all replicas converge.

This reduces the rate at which new rows accumulate. It does not bound historical
storage by itself; the optional server-side compactor below remains a separate choice
and requires Yjs on the maintenance host.

## Compact a document

Editing and reconnecting need no extra steps. Compaction is optional: it combines accepted
updates into one ordinary update row, in the same table, and retires the rows it merged. A
fresh client then loads that merged update plus edits received since compaction. It can run
from the editor, through the ordinary write path, or as a maintenance command.

### From the editor

**Editor compaction trusts every writer of the tenant.** PostgreSQL can check that the rows a
compaction names belong to this tenant and document; it cannot check that the merged bytes keep
their Yjs history, since it does not decode Yjs. It relies on the editor for that, so a buggy
client could replace history with an incomplete merge. That suits cooperating writers, and it is
the main difference from the maintenance command below, which runs as the table owner.

**Compact document** merges this document's confirmed updates (those PostgreSQL already holds,
so not the ones still in the outbox or awaiting their echo) and sends one `INSERT` through
`mutate`, with the merged bytes and `replaces`, the IDs it merged. It needs no other connection
and works offline: the replacement waits in the outbox like any edit.

What PostgreSQL enforces, in two triggers (`maintenance.sql`):

- every listed row exists, is active, and has the replacement's tenant and document;
- only the listed rows are retired, and only if the replacement row is actually inserted, in the
  same transaction; edits arriving concurrently have other IDs and stay active;
- 1 to 256 distinct IDs, not including the replacement itself;
- one compaction of a document at a time, client or maintenance, so a second compaction of
  the same rows sees them retired and is refused;
- the list is consumed by the triggers and stored as NULL, so it never replicates.

The retirement runs in an `AFTER INSERT` trigger (`yjs_updates_replace()`, `SECURITY DEFINER`,
owned by the table owner) because the bridge writes `INSERT … ON CONFLICT (id) DO UPDATE …
WHERE <version guard>`. A replacement whose ID is already taken by a newer row is declined by
the guard and inserts nothing: `BEFORE INSERT` triggers have run by then, and only an
`AFTER INSERT` trigger is sure the row went in. The `BEFORE INSERT` trigger only sets the list
aside for the transaction and stores NULL.

A refusal is a check violation, so the write gets a final `rejected` verdict: the editor reports
it and nothing is lost, since the inputs stay active and the document already holds their
content. A retry of an accepted compaction meets its receipt and cannot retire anything more.

**Size.** A row must fit ZeBridge's row slot (4 KB by default; ZeBridge's width guard refuses a
wider one). The editor merges the smallest confirmed updates first, up to 3,000 bytes in all, and
does nothing unless two fit. A large document therefore compacts in several passes, and an update
over the budget stays as it is.

### As a maintenance command

If this example was set up before compaction was added, upgrade its guard once as the DBA:

```sh
psql "$ADMIN_DATABASE_URL" -1 -v ON_ERROR_STOP=1 -f examples/17-yjs/maintenance.sql
```

From `examples/17-yjs/web`, install the maintenance dependencies with `pnpm install`, then
run as the table owner (or a role inheriting the owner's privileges), using a dedicated
database connection. Do not give this connection string to the browser or use the
bridge's ordinary writer role for compaction:

```sh
YJS_MAINTENANCE_DATABASE_URL="$ADMIN_DATABASE_URL" pnpm run compact \
  --tenant acme --document 11111111-1111-4111-8111-111111111117
```

The command runs once. It skips documents with fewer than 1,000 active rows; use
`--min-updates 2` to try it on a short editing session. Its JSON result reports the
replacement ID, retired rows and payload sizes. Run it occasionally for documents that
accumulate edits; no scheduler is installed by this example.

The command holds a per-document transaction advisory lock, reads the active updates,
and uses `Y.mergeUpdates` to preserve their CRDT information. In the same transaction it
inserts a new UUID row and tombstones **only the exact input IDs**. Concurrent uploads
remain untouched; another compactor of the same document waits. Any failure rolls back
both the replacement and retirements. The merged row must fit the existing table/row
limits, just like other data synchronized by ZeBridge.

Retirement stamps a fresh `deleted_at` and advances the row version. Normal CDC removes
retired rows from local replicas; applying the replacement preserves their document
content. An older offline Yjs document can still merge with it. The adapter does not
interpret retirement of an SQL update row as deletion of text.

**`bridge_sweeper` needs no changes.** It discovers this table through the existing
catalogue and reaps the retired rows after the normal `GC_THRESHOLD_MS` retention.
Keep the ordinary [chain/GC retention rules](../../OPERATIONS.md#sweeper); compaction
does not require shortening them. Tombstones occupy database space until cleanup.
This merges update encodings; it does not discard Yjs history or implement Yjs GC.

## Code and checks

- `web/src/provider.ts` loads document updates, applies them with its own Yjs origin,
  and serializes local inserts through `mutateWithId`. The origin prevents echo loops;
  update IDs avoid repeatedly applying already-seen rows. Yjs handles causal dependencies.
- `web/src/main.ts` binds a `Y.Text` to CodeMirror, provides the offline switch, and
  shows errors. Undo/redo uses Yjs's editor binding rather than a second text history.
- The provider stops accepting further local uploads after a failed queue operation
  or an adverse verdict. The page disables editing and leaves text available to export.
  It does not pretend a rejected CRDT operation can be removed from an existing history.

```sh
cd examples/17-yjs/web
pnpm test
pnpm run build
```

The adapter tests use real Yjs documents and SQLite BLOBs with controlled delivery:
concurrent offline edits, reversed/duplicate updates, reload, scope filtering, and
queue/server failures. The client tests separately cover binary outbox replay and
binary rollback after a reseed. The adapter tests also cover receiving a compacted
replacement while an older editor has offline edits, and the editor's compaction: only
confirmed updates, the size budget (smallest first), and a refused compaction that is reported and not fatal,
also after a reload.

For an end-to-end test against the running stack after setup:

```sh
NATS_URL=nats://127.0.0.1:4222 ZB_TEST_PRINCIPAL=alice pnpm run test:live
```

For operator mode, also set `NATS_CREDS` to the test principal's credentials, and
`ZB_JS_DOMAIN` if the stack uses a JetStream domain. The test creates a new document;
it checks offline replay after reopening a replica, convergence, pending acknowledgments,
and reconstruction in a fresh replica. It prints the document UUID and temporary replica
directory for inspection and leaves the document's update rows in the test database.

Set `YJS_MAINTENANCE_DATABASE_URL` as well to exercise compaction while a client is
offline, catch-up of the live client, and initialization of a new replica from one
merged row before the offline edit returns.

The dedicated compaction tests additionally check concurrent uploads/compactors,
transaction rollback, tenant/document scope, writer restrictions, the editor's compaction as
the writer role (exact retirement, every refusal, two writers racing, a replacement the
version guard declines) and actual sweeper
cleanup. Run these **only on a disposable test stack**: they temporarily shorten the
database's GC retention and advance its watermark, scoped to this example's table:

```sh
YJS_MAINTENANCE_DATABASE_URL="$TEST_ADMIN_DATABASE_URL" \
DATABASE_WRITER_URL="$TEST_WRITER_DATABASE_URL" pnpm run test:compaction
```

The test stack must map `alice` to `acme`, and have `bridge_sweeper` built in the
repository's `zig-out/bin` (or set `YJS_TEST_SWEEPER`).

## Boundaries

This is an application example, not a generic CRDT service. ZeBridge never interprets
Yjs payloads. Another library would supply a different adapter over the same immutable
update pattern; it cannot interpret an existing Yjs document.

There is no document deletion, presence/cursors, or per-document sharing within a
tenant. All updates for the tenant replicate; the selected document is filtered locally.
The provider re-queries that document's active history on changes, so this implementation
is intended for small documents. Compaction reduces active rows and query overhead;
large documents may still need incremental loading. Manual deletion of active update
rows loses information: retire them only after saving their merged replacement.

Tenant authorization and normal PostgreSQL constraints still apply. Strict business rules
belong in server commands or require a Yjs-capable validator to inspect document content.
The normal ZeBridge GC/receipt retention limits still bound offline write replay.

Yjs persistence and update semantics: [document updates](https://docs.yjs.dev/api/document-updates).
