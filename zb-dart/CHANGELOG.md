## 0.3.3

- No change in the package: its version follows ZeBridge's releases. This README documents write order: `mutateWith(…, after: …)`, `independent: true`, and the `msgId` that `mutate` returns.

## 0.3.2

- libzb on Android creates its `no_backup/` directory even when there is no identity to move into it.

## 0.3.1

- No change in the package: its version follows ZeBridge's releases. 0.3.1 fixes libzb's DuckDB replicas (a reserved column name) and moves the Flutter examples to Android Gradle Plugin 9.1.1.

## 0.3.0

- libzb speaks ABI 9 (`zbAbi = 9`): the package refuses an older libzb.
- Write order. `mutate` returns a map with the write's `msgId`. By default a write waits for this device's most recent unsettled write; `mutateWith(table, op, key, values: …, after: parentId)` waits for a named one, and `independent: true` for none. Both `ZeBridge` and `ZeBridgeWorker` offer it.
- A write belongs to its table's incarnation: a write made against a table since dropped and re-created is answered `rejected` / `TableIncarnationChanged` instead of being applied to the new rows. A new edit can be refused before anything is queued (`TableNotWritable`, `IncarnationUnknown`, `ReplacementIncomplete`).
- A refused write's local undo is a durable obligation, retried until it is done.
- A first private tenant assignment reaches a running client without a restart.

## 0.2.11

- `PollReport.floorMarginMs`: how long the oldest waiting write has before the GC floor refuses it (libzb's `floor_margin_ms`).

## 0.2.10

- Fixes in libzb's write path: an echo no longer swallows a verdict, which fires once; a rebase is judged against the winning row; the confirmed row outlives the first stale decision, and a failed settlement is retried.

## 0.2.9

- No change in the package: its version follows ZeBridge's releases. 0.2.9 reads DuckDB results with `duckdb_fetch_chunk` (libzb's DuckDB replicas).

## 0.2.8

- libzb is built with Zig 0.17. No change in the package's API.

## 0.2.7

- libzb speaks ABI 7 (`zbAbi = 7`): the package refuses an older libzb.
- Two new write outcomes and `PollReport.reconciling`: `indeterminate` (the write settled but its result is unknown; the table reseeds from the hub), then `reconciled` (the local copy is the authoritative state again).
- A client that reconnects after missing generation cuts applies the generation deltas instead of replaying the CDC backlog, decided per stream. Only for tables that guarantee tombstones: the bridge marks a guarded table's generations `"deletes":"tombstoned"`.
- `seedDomain`, a connect option: the first seed is read from a seeding leaf's mirrors, with the hub's as a fallback.
- Enrollment and renewal return ordered endpoints (a leaf first, then the hub): the client fails over between them and keeps its identity, replica and outbox.
- Replaying the outbox after a reconnect is bounded by a durable window of writes in flight.
- Fixes: a reseed rewinds the table's own stream to the chain's cut (deletes made during the reseed were lost); a heal reopens every healed stream; a client recovers when its leaf loses the hub; an open survives a JetStream that answers late; one device sends far fewer JetStream requests (live tails park their pull requests on the server, a consumer is created before it is looked up).

## 0.2.6

- No change in the package: its version follows ZeBridge's releases. 0.2.6 fixes the
  bridge's HTTP servers (a POST that declared no body could crash a Debug build, and was
  undefined behaviour in a release one).

## 0.2.5

- libzb's Android builds name their API level to the NDK's headers (`-Dandroid-api`, 29):
  before, an older NDK assumed every Android API existed.
- No other change in the package: its version follows ZeBridge's releases. 0.2.5 adds
  `bridge --demo-invites` (a public demo's invite door), Maven Central publishing from CI,
  and fixes the sweeper audit and the compose deployment's certificate e-mail check.

## 0.2.4

- No change in the package: its version follows ZeBridge's releases. 0.2.4 fixes the Docker
  Compose deployment (IPv6 for a cloud PostgreSQL, HAProxy alone on port 80, upgrades).

## 0.2.3

- No change in the package: its version follows ZeBridge's releases. 0.2.3 adds
  `bridge --invite` (invites from a backend, without the admin) and the all-in-one Docker
  Compose deployment.

## 0.2.2

- libzb: two clients in containers no longer collide on a stream. A live-tail consumer was
  named after the process id, which is 1 in every container: a second client was then
  refused, or shared the first one's consumer and got only part of the changes. The name is
  now random.

## 0.2.1

- No change in the package: its version follows ZeBridge's releases. 0.2.1 adds the readings
  responder image and the quickstart's shared-record page.

## 0.2.0

- Versions now follow ZeBridge's releases: every ZeBridge package carries the release's
  version, so `zebridge 0.2.0` holds libzb 0.2.0.
- libzb: SQLite's math functions (`sin`, `cos`, `asin`, `radians`, `pow`, `floor`, `mod`…)
  are built in, so a distance can be computed in plain SQL on the local replica.

## 0.1.4

- On iOS, the identity (the device's key) is flagged out of iCloud and computer backups,
  so restoring a backup onto a new iPhone no longer copies it. An identity saved before
  gets the flag on the next start.

## 0.1.3

- On Android, the identity (the device's key) is kept in the app's `no_backup/` folder,
  which Android's backup skips: a reinstall or a new phone no longer brings back an old
  identity. An identity beside the replica is moved there on the first start.
- `ZeBridgeWorker.principal`: who this device is. Read it there, not from the identity
  file.

## 0.1.2

- pubspec.yaml declares the supported platforms: Android, iOS, Linux, macOS. There is
  no Windows build of libzb yet.

## 0.1.1

- The example is `example/zebridge_example.dart`, where pub.dev looks for it.
- The repository's pubspec.yaml now carries the repository URL pub.dev verifies.

## 0.1.0

- First release: the ZeBridge client for Dart and Flutter, with libzb (C ABI 6)
  built in for Android, iOS, macOS and Linux, delivered by a build hook.
