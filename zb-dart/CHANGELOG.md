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
