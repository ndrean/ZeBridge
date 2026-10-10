# zb-react-native — libzb for React Native

libzb, the ZeBridge C client (Zig inside), as an Expo module: the ZeBridge client for
React Native, on iOS and Android. `Libzb.connect(opts)` seeds, tails, queries and writes in
native code; the options are the ones zb-client-ts takes (CLIENTS.md). `mergeRegisters`
is libzb's own register merge, the rule every client applies.

```ts
import { Libzb } from '@zebridge/react-native';

const zb = await Libzb.connect({ natsUrl, creds, principal, tables: ['orders'], dbPath });
await zb.sync();                                   // schema, seed, positions
const { rows } = await zb.query('SELECT count(*) FROM orders');
```

## Install

    npx expo install @zebridge/react-native

The package carries libzb already built: an xcframework for iOS (device and simulator)
and `libzb.so` for Android (arm64-v8a, armeabi-v7a, x86_64). It is native code, so the app
runs in a [development build](https://docs.expo.dev/develop/development-builds/introduction/)
(`npx expo run:ios`, `npx expo run:android`, or EAS Build), not in Expo Go. Expo SDK 53 or later.

After upgrading the package, regenerate the native projects (`npx expo prebuild --clean`, or
`pod install` in `ios/`): CocoaPods copies the library's slices by the names it saw at
the last install, and a build against stale ones fails with `library 'zb' not found`.

## Build (in this repository)

    scripts/build-ios.sh       # both iOS slices → ios/ZbCore.xcframework, ABI checked first
    scripts/build-android.sh   # zb-android's libzb.so per CPU, for the Android module

Each build is a **copy** of libzb. After any libzb change, run the script again and
rebuild the app. The first call compares the copy's `zb_abi_version()` with `src/abi.ts`
and refuses a mismatch with the fix — a stale copy once sat in `connect` for minutes.
The version lives in `libzb/abi.json`; `libzb/python/abi_check.py` (the offline battery's
`abi`) fails when libzb's exported functions or connect options change without a bump,
and when this package's pin disagrees.

⚠️ Each slice is **prelinked** (`ld -r -exported_symbol '_zb_*'`): libzb's SQLite, zstd
and nats stay private to it, so another SQLite in the same app (expo-sqlite's, say) does
not collide with it.
