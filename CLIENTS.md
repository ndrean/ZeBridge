# The two clients — what each does, and where they differ

Two client libraries speak the protocol. This is their parity, as it stands, so a
lifecycle lesson learned in one is not silently missing from the other.

| | libzb | zb-client-ts |
| --- | --- | --- |
| language | Zig core + shell, C ABI | TypeScript core + shell |
| hosts | Python, Node (ctypes/FFI), Flutter (Dart FFI) | browser, Node |
| local engine | SQLite | SQLite (sqlocal, better-sqlite3), PGlite |
| loop | host-driven: `sync`, `poll`, `flush` | self-driven: `connect()` runs it |
| tables followed | the explicit `tables` list | every key in the `schemas` bucket |

## Pinned by fixtures — identical by construction

One conformance suite, `zb-client-ts/fixtures/core-fixtures.json`, drives both cores
(`core.ts` through `core.test.ts`, `core.zig` through `libzb/python/runner.py`). A
rule in a fixture group cannot diverge without a test failing on one side. The groups,
34 today: seedGate, tombstoned, chainPlan, outboxWatermark, fullPredates, scope,
position, fkKind, pgTsToWire, lsnToNumber, normalizeVersion, nextVersion, hlcVersion,
subjectSafe, envelope, keyChange, upsert, pgArrayLiteral, update, exists, delete,
chainUpsert, chainRowParams, columnDdl, fkClauses, createTable, rebuildSteps,
diffColumns, fkDiffer, viewSteps, indexPlan, heartbeat.

Everything below the core — the shells — is where parity is by hand, and where this
document earns its place.

## Parity matrix

✓ same behaviour · ≠ different · — not applicable. The NOTES section that made each
row a rule is named.

| contract | libzb | zb-client-ts | source |
| --- | --- | --- | --- |
| seed gate by stream seq, never by LSN | ✓ | ✓ | §10i, fixture |
| seeding scoped to gapped streams, shared route included | ✓ | ✓ | §10n, §10bq, fixture |
| a chain older than the replica's position is refused | ✓ | ✓ | §10n, fixture |
| seed with foreign keys off, on again after | ✓ | ✓ | §10cp |
| consumer floor from the seed's cutoff, not the tail | ✓ | ✓ | §10cp |
| chain-orphan check (cutoff vs stream first seq) | ✗ | ✗ | §10n residual, designed not built |
| a table with no chain yet | re-asks at every poll until seeded | waits 90 s, then excludes the table for the process's life; a re-seed kick retries for 90 s | §10cq item 3, §10dg |
| position persisted per batch, held and gated events included | ✓ | ✓ | §10m D1 |
| quiet stream: stored 0 takes the tail | ✓ | ✓ | §10m |
| position beyond the stream's last seq = restart, reset | ✓ | ✓ | §10bm, fixture |
| table existence decided from the database, not memory | ✓ | ✓ | §10m finding 9 |
| rebuild copies data with foreign keys off | ✓ | ✓ | §10m |
| indexes dropped before ALTERs, synced on every outcome | ✓ | ✓ | §10bi |
| schema changes applied | live, a KV watch drained at every poll | live, a KV watch | §10bi, §10de |
| shape record: a re-key rebuilds empty + re-seeds, a re-type alters or rebuilds keeping rows | ✓ `_zbz_shape` | ✓ `_zebridge_shape` | §10dg, fixtures `shape`/`retyped` |
| a rebuild that cannot carry the rows degrades to emptied + re-seed | ✓ | ✓ | §10dg |
| descriptor epoch above the stored one → watermark dropped, re-seed; a manifest below the descriptor's epoch refused | ✓ | ✓ | §10df |
| a chain object naming a column the replica lacks | wait for the next full | wait for the next full | §10dg |
| suspended descriptor | table skipped this sync | table skipped, surfaced to the app | |
| drop tombstone → local table dropped | ✓ | ✓ | |
| CDC batch in one transaction, position in it | ✓ | ✓ | §10cq item 2 |
| child before parent: held durably, retried, pruned by a seed past it or a drop | ✓ `_zbz_inbox` | ✓ `_zebridge_inbox` | §10m D1, §10de |
| foreign keys deferred inside a batch, isolated replay with holds when COMMIT refuses | ✓ (2026-09-06) | ✓ `defer_foreign_keys` + `applyBatchIsolated` | §10e, §10dg |
| held events discarded when their row's DELETE goes by; multi-level holds released to a fixpoint | ✓ | ✓ | §10dg `cascade_held` |
| cascaded deletes idempotent | ✓ | ✓ | §10cq |
| the duty outlives the instrument (tail, status watch) | reopen-then-swap; reopen from the stored position on a consumer death | tail loop with idle guard; status loop recreated until close | §10cq items 1, 5, 6, 7 |
| outbox durable, optimistic apply + queue in one transaction | ✓ | ✓ | PROTOCOL MUST 1 |
| version stamped by the client (HLC) | ✓ | ✓ | (NOTES §10cp said libzb's caller supplied it; no longer true) |
| verdicts: accepted / stale / rejected / row_deleted, two revert targets | ✓ | ✓ | PROTOCOL MUST 4 |
| a stale UPDATE rebased onto the winning row when the columns are disjoint, dropped and surfaced when they overlap; the winner before or after the verdict, a slow clock | ✓ `mutate_at` stamps a write explicitly | ✓ `mutate(…, { version })` | §10do, PROTOCOL §7.6, `rebase_stale` |
| a write with no socket queues in the outbox and goes out on the next connect | ✓ (the host's flush) | ✓ (was a silent return) | §10dp |
| the CDC echo that confirms a write carries the write's own stamp — another client's row on the same key is not our echo | — (settles on verdicts only) | ✓ (was by key alone) | §10dt, `rebase_stale` §D |
| an UPDATE that changes a key column is refused before it is queued (`KeyChange`); rename = delete + create | ✓ core | ✓ core | §10dv, PROTOCOL §7.7, fixture |
| the host can see refusals: cumulative verdict counts by status | ✓ `flush` report `verdicts{…}`; refusals printed with reason and detail | ✓ per-verdict log lines | §10dx, `drip` |
| the optimistic row carries the write's own stamp in the version column | ✓ | ✓ | §10dx |
| `row_deleted` removes the local row (the server's word is "deleted"); `rejected` restores the before-image | ✓ (restored both, until §10dy) | ✓ | §10dy |
| the wire grammar compiled in; `grammarHash` at open refuses a fork; a bridge that cannot be reached does not block opening | ✓ `@embedFile`, `zb_grammar_hash()` | ✓ packaged copy, `grammarHashHex()`, pinned by test | §10dq, PROTOCOL §1, `grammar_served` |
| missed verdicts recovered by direct get | ✓ | ✓ | §7.4b |
| a chain object past 2 MiB (a full with tombstones) | ✓ own chunk reader | ✓ pull-consumer reader with the object's SHA-256 checked (the object store's push reader stalls at 2 MiB, @nats-io/obj 3.4.0) | §10eh |
| one seed per table at a time — a second request joins the one in flight | — (one sync path) | ✓ (was twice at first sight) | §10eh |
| the gap rule LIVE: a delivered sequence beyond stored + 1 is a hole the stream pruned under the consumer — re-seed at once, never read past it | ✓ per poll (was connect-time only) | ✓ per delivery (was connect-time only) | §10ei, PROTOCOL §7, `cdc_wall` |
| a chain whose cutoff fell off the stream is refused (`predates the stream`), the next generation awaited | ✓ | ✓ | §10ei |
| outbox watermark gate before a flush | ✓ | ✓ | §10at, §10au, fixture |
| a failed optimistic echo still queues the write | ✓ | ✓ | §10cp |
| liveness of the NATS connection | host-driven (`NoResponders` → reopen tails) | RTT poll every 10 s, re-sync on recovery | §10cn |
| fleet heartbeat (PROTOCOL §9) | on every poll and at sync end | from tenant resolution, before the seed | §10dc |
| auth error named | ✓ `AuthorizationViolation` / `AuthExpired` from `poll` | ✓ the server's `error` status and `closed()`'s reason logged by name (2026-09-06) | §10cj, §10dl `revoke_midseed` |
| the ban (`mutation_ack.<p>.revoked`): hang up now, stay hung up on reconnect, every call answers Revoked | ✓ `error.Revoked` | ✓ `revoked`, logged, closed | §10dm |
| the wipe is explicit, never automatic | ✓ `zb_client_wipe` | ✓ `wipe()` | §10dm |
| a principal with no tenant mapping (revoked, never enrolled) | tenant-scoped tables skipped audibly, public followed | same; a purged mapping's DEL marker reads as none | §10dl |
| tenant revoked while connected | next connect | next connect | |
| inbox pruning | — (no inbox) | ✓ | |
| chain dictionary cache | per process | persisted (`_zebridge_dicts`) | §10x |
| zstd chain objects | built in | Node built in; browser needs `zstdDecompress` | §10w |
| survives its host killed mid-seed | proven (`client_kill.py`) | not tested | §10ce |
| killed mid-migration: first sight drops the watermark, a half-finished rebuild is adopted, no shape record → the physical key decides | ✓ | ✓ | §10dj `rebuild_kill` |
| `query()` cannot write — the bookkeeping is out of the application's reach | ✓ a READONLY SQLite connection | ✓ a read-only connection on Node; `core.isReadOnlySql` where there is one handle (OPFS, PGlite) | §10di, `outbox_break` |

## Divergences that matter, ranked

1. ~~libzb loses held events with its host~~ — closed 2026-09-06: `_zbz_inbox`,
   written in the batch transaction, retried from the table, pruned by a seed past
   it or a drop. Parity with the TS inbox (§10de).
2. ~~Migrations reach libzb late~~ — closed 2026-09-06: libzb drains a watch on the
   schemas bucket at the top of every poll (§10de); both clients now walk every
   migration shape of `MIGRATIONS.md` side by side (`scripts/scenarios/migrate_both.py`).
3. **The TypeScript client follows every schema key.** No table list, so a ghost key
   — a probe table dropped while no bridge watched, hence no tombstone — costs every
   fresh client a 90 s wait and a permanent exclusion (item 3 of §10cq). libzb's
   explicit list is immune. Two fixes, not exclusive: a `tables` option, and the
   bridge purging keys for tables no longer in the publication at boot.
4. **Missing chain: retry or exclude.** libzb retries on the next sync; the TypeScript
   client gives up for the process's life. A retryable exclusion is the open product
   question of §10cq item 3.
5. **Chain-orphan check** is built in neither. Retention ≥ cadence keeps it rare, not
   impossible.

## What is tested, per client

Both clients walk every migration shape together in `migrate_both` (owns).
libzb carries the chaos program: `matrix`, `churn`, `cascade`, `client_gap`,
`shared_gap`, `client_kill`, `chain_kill`, `jwt_expiry`, `clockskew`, `fleet`,
`grammar_served`, `leaksoak`, and the swarm's Python workers. The TypeScript client
is exercised by `objstore_race`, the swarm's Node/PGlite workers (§10cp, §10cq), the
Node consumer example, and the browser by hand. Nothing kills a TypeScript host
mid-seed, nothing migrates a table under a TypeScript client in the battery, and the
browser path has no automated run at all.
