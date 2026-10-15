# Release smoke test

Checks a published release as a user gets it, at one version, once every registry has it
(after the npm approval):

```sh
smoke/run.sh 0.2.2                               # here (OrbStack or Docker)
gh workflow run smoke.yml -f version=0.2.2       # on GitHub, Linux x86_64
```

| what | how | checked |
| -- | -- | -- |
| the bridge | APT, with the signing key | starts, enrolls the clients |
| Node `@zebridge/client` | npm | enroll, seed, write → `applied`, a live row from PostgreSQL |
| Python `zebridge` | PyPI | the same |
| Dart `zebridge` | pub.dev, on Linux | the same |
| Android `eu.zebridge:zebridge-android` | Maven Central | the AAR holds libzb for arm64-v8a, armeabi-v7a, x86_64 |
| React Native `@zebridge/react-native` | npm | holds libzb for iOS (device, simulator) and the three Android ABIs |

Each client runs in its own container. The "live row" is one `run.sh` inserts in
PostgreSQL after the client is ready: only the live CDC tail can deliver it. The stack is
removed at the end.

## By hand: the phones

The packages above are only opened, not run, on a phone. One app runs them: the airports
app in `examples/10-airports/flutter`, on pub.dev's `zebridge` at exactly the release
(`scripts/version.py` moves it). It covers libzb on iOS and on Android.

1. Two invites in one tenant, on the hub's PostgreSQL (the Supabase SQL editor):

   ```sql
   INSERT INTO zebridge_invites (principal, tenant_id) VALUES ('smoke-iphone', 'acme'), ('smoke-android', 'acme') RETURNING code;
   ```

2. Start each phone fresh, or it reconnects with the identity it already has and ignores
   the invite: delete the app on the iPhone; on Android,
   `adb -s ZE22324B2T shell pm clear dev.zebridge.airports`.

3. From `examples/10-airports/flutter`, after `flutter pub get`:

   ```sh
   flutter run --release -d <iphone udid> --dart-define=ZB_INVITE=<code 1>
   flutter run --release -d ZE22324B2T    --dart-define=ZB_INVITE=<code 2>
   ```

   The bridge defaults to `https://bridge.zebridge.eu`. Building and installing without
   `flutter run` is in [the airports README](../examples/10-airports/README.md), section 7.

4. Passes when: both apps open and show the airports around the map; a departure set on one
   phone appears on the other, and an arrival set on the other comes back.

The React Native package and the Maven AAR are not run on a phone: the RN example and
`zb-android/scripts/device-test.sh` use this repository's build, not the published one.
They carry the same libzb as the Flutter package, which step 4 runs.
