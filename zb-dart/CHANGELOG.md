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
