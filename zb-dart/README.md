# zebridge (Dart and Flutter)

A ZeBridge client for Dart and Flutter over libzb's C ABI: one isolate owns the client
and its poll loop, the app talks to it with futures and a report stream. Which library
for which host: [DISTRIBUTION.md](https://github.com/ndrean/zebridge/blob/main/DISTRIBUTION.md).

```sh
dart pub add zebridge     # or: flutter pub add zebridge
```

The package carries libzb, built for Android (arm64, arm, x64), iOS (device and
simulator), macOS (Apple silicon, Intel; 13+) and Linux (x64, arm64; glibc 2.35+). Its build
hook hands the right one to every build, a Flutter app's or a plain `dart run`: there
is nothing to install or link. Dart 3.10 or later (Flutter 3.38).

```dart
import 'package:zebridge/zebridge.dart';

final zb = await ZeBridgeWorker.spawn({
  'bridgeUrl': 'https://zb.example.com',
  'invite': code,                 // first run only: what your backend handed the device
  'tables': ['orders'],
});
zb.reports.listen((r) => refresh(r.changedTables));
final open = await zb.query('SELECT * FROM orders WHERE status = ?', ['open']);
await zb.mutate('orders', 'UPDATE', {'id': 7}, {'status': 'done'});
await zb.close();
```

- **The options** are the ones every ZeBridge client reads ([CLIENTS.md](https://github.com/ndrean/zebridge/blob/main/CLIENTS.md)), passed to libzb
  as they are. The first run enrolls with `invite` and keeps the identity next to the
  replica; later runs need neither, and the JWT renews itself.
- **The isolate.** `zb_client_poll` blocks its thread while nothing arrives, and Dart's
  FFI holds the isolate for the call — on the UI isolate that froze a frame every idle
  second. The worker isolate owns the handle; calls are served between polls
  (`pollWaitMs`, 100 ms). `pause()`/`resume()` follow the app's lifecycle.
- **libzb** comes from the package's build hook (`hook/build.dart`), which picks the
  library made for the target from `prebuilt/`; in this repository,
  `scripts/build-prebuilt.sh` builds them.
- **Errors** are `ZeBridgeException` with libzb's words; `unseeded` lists what the first
  sync could not seed.

`dart run example/enroll.dart <bridgeUrl> <identityPath> <dbPath> [invite]` runs the whole
app side: enroll, read, write, the verdict.
