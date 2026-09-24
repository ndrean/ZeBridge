# 06-large-table / flutter — one big table, seeded on a phone by libzb

The native measurement: the same seed as `../web` and `../react-native`, through
**libzb** — the C ABI, Zig inside — loaded by Flutter over `dart:ffi`. `zb_client_sync`
streams the chain object, sorts each window and applies it in C; Dart keeps the time.
One table, `test_types` (3,055,002 rows on globex), one clock, the three facts at the
end, checked against PostgreSQL:

```
rows / distinct uid / sum(age)   →   3,055,002 / 3,055,002 / 138,916,285
```

## Measured (2026-09-24)

| | iPhone 17 simulator | **iPhone 12 (A14, iOS 26.6)** |
| --- | --- | --- |
| connect → usable, libzb | **30.2 s** | **70.8 s** (62.9 s on a rerun) |
| connect → usable, zb-client-ts (../react-native) | 371.4 s | 725 s |
| replica | 1.02 GB | 1.02 GB |
| rows / distinct uid / sum(age) | exact | exact |

Ten times the JavaScript client on the same phone; the phone was never the limit, the
JS runtime was. libzb has no seed progress yet (one blocking call), so the screen shows
a clock, not a bar — a `seeding` field in `zb_client_poll`'s report is the next piece.

⚠️ The first phone run "finished" in 15.8 s with 0 rows: the seed had failed inside
libzb, on a stderr nobody could see (attach it: `xcrun devicectl device process launch
--console --device <udid> dev.zebridge.zebridgeLargeTable`). The cause was a
slow-consumer runaway in the object reader over a real network — fixed in nats.zig
(patch 18) and told in NOTES §10iy. `zb_client_connect`/`zb_client_sync` reporting
their failure reason through the C ABI is the other lesson.

## Build

libzb for iOS is a static library, both slices in one xcframework:

    tool/build-libzb-ios.sh      # zig build lib for aarch64-ios and -simulator, repacked

Then, from this directory:

    cp ../../../scripts/native/creds/bob.creds assets/creds/     # dev only, git-ignored
    flutter pub get
    flutter build ios --simulator --debug && xcrun simctl install booted build/ios/iphonesimulator/Runner.app
    flutter build ios --release --dart-define=ZB_NATS_URL=nats://192.168.1.11:4222   # a real phone: the Mac's LAN address
    xcrun devicectl device install app --device <udid> build/ios/iphoneos/Runner.app

What `ios/Flutter/*.xcconfig` carries, and why (each was a failed link or launch):

* `-force_load` on the slice: nothing references the `zb_*` symbols by name (Dart looks
  them up at run time through `DynamicLibrary.process()`), so the linker would drop
  every object;
* `STRIP_STYLE = non-global` and `-exported_symbol "_zb_*"`: a release build strips an
  app's globals — the simulator debug build found the symbols, the phone's release
  build did not (`dlsym … symbol not found`);
* `DEVELOPMENT_TEAM`: a personal Apple team signs the device build.

And in `ios/Runner/Info.plist`, `NSLocalNetworkUsageDescription`: without it iOS
refuses connections to LAN addresses silently — `zb_client_connect` returned 0 with no
other symptom. libzb's side: `bundle_compiler_rt` (Xcode's ld found `roundq`
undefined) and a trace-free panic on iOS (`std.debug`'s stack traces want a dyld
symbol the SDK does not export) — both in `libzb/build.zig` and `libzb/src/capi.zig`.
