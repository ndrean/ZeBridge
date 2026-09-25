# zb-react-native — libzb for React Native

libzb, the ZeBridge C client (Zig inside), as an Expo module. Two uses:

* **the libzb engine** — `Libzb.connect(opts)`: seed, tail and query in native code.
  The options are the ones zb-client-ts takes (CLIENTS.md).
* **native zstd for zb-client-ts** — installed in an app, it is picked up by
  zb-client-ts's react-native entry with no code: chain objects inflate in libzb instead
  of fzstd (an iPhone 12 seed, 3M rows: 393.8 → 309.8 s, NOTES §10ja).

```ts
import { Libzb } from 'zb-react-native';

const zb = await Libzb.connect({ natsUrl, creds, principal, tables: ['orders'], dbPath });
await zb.sync();                                   // schema, seed, positions
const { rows } = await zb.query('SELECT count(*) FROM orders');
```

## Build

    scripts/build-ios.sh      # both iOS slices → ios/ZbCore.xcframework, ABI checked first

The xcframework is a **copy** of libzb. After any libzb change, run the script again and
rebuild the app. The first call compares the copy's `zb_abi_version()` with `src/abi.ts`
and refuses a mismatch with the fix — a stale copy once sat in `connect` for minutes.
The version lives in `libzb/abi.json`; `libzb/python/abi_check.py` (the offline battery's
`abi`) fails when libzb's exported functions or connect options change without a bump,
and when this package's pin disagrees.

⚠️ Each slice is **prelinked** (`ld -r -exported_symbol '_zb_*'`): libzb's SQLite, zstd
and nats stay private to it, so expo-sqlite's own SQLite can live in the same app.

iOS only for now; Android is the NDK build of `examples/06-large-table/flutter/tool`.
