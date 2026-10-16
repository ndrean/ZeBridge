# Release smoke test

Checks a published release as a user gets it, at one version, once every registry has it (after the npm approval):

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

Each client runs in its own container. The "live row" is one `run.sh` inserts in PostgreSQL after the client is ready: only the live CDC tail can deliver it. The stack is removed at the end.

## One client against a real server

The same check against a deployed bridge, for example the compose in `deploy/compose/`.
On the server, the table (`smoke.sql`, with the deployment's publication) and one invite
per run:

```sh
psql … -v pub=<publication> -f smoke.sql
psql … -tAc "INSERT INTO zebridge_invites (principal, tenant_id) VALUES ('t-python-1', 'smoke') RETURNING code"
```

Here, the client:

```sh
docker build -q smoke/clients/python --build-arg ZB_VERSION=0.2.2 -t zb-smoke-python
docker run --rm -e ZB_SMOKE_NAME=python-1 -e ZB_SMOKE_LIVE_WAIT=300 \
  -e ZB_BRIDGE_URL=https://bridge.example.com -e ZB_INVITE=<code> zb-smoke-python
```

When it prints `ready: insert live-python-1 within 300 s`, insert that row on the server:

```sh
psql … -c "INSERT INTO smoke_items (id, tenant_id, note) VALUES ('live-python-1', 'smoke', 'by hand')"
```

`ZB_SMOKE_NAME` names the client's rows (`from-<name>`, `live-<name>`): give each run a
new one, since a second run finds the first one's rows at seeding. `ZB_SMOKE_LIVE_WAIT` is
how long it waits for the live row (30 s by default, enough for `run.sh`). An invite is
spent at enrollment, failed run or not: each run needs a new code.

## By hand: the phones

The packages above are only opened, not run, on a phone. One app runs them: the airports app in `examples/10-airports/flutter`, on pub.dev's `zebridge` at exactly the release (`scripts/version.py` moves it). It covers libzb on iOS and on Android.

1. Two invites in one tenant, on the hub's PostgreSQL (the Supabase SQL editor):

   ```sql
   INSERT INTO zebridge_invites (principal, tenant_id) VALUES ('smoke-iphone', 'acme'), ('smoke-android', 'acme') RETURNING code;
   ```

2. Start each phone fresh, or it reconnects with the identity it already has and ignores the invite: delete the app on the iPhone; on Android,
   `adb -s ZE22324B2T shell pm clear dev.zebridge.airports`.

3. From `examples/10-airports/flutter`, after `flutter pub get`, and `flutter devices` (grab your device "moto" and "iphone" ID):

   ```sh
   flutter run --release -d <iphone udid> --dart-define=ZB_INVITE=<code 1>
   flutter run --release -d ZE22324B2T    --dart-define=ZB_INVITE=<code 2>
   ```

   The bridge defaults to `https://bridge.zebridge.eu`. Building and installing without
   `flutter run` is in [the airports README](../examples/10-airports/README.md), section 7.

4. Passes when: both apps open and show the airports around the map; a departure set on one
   phone appears on the other, and an arrival set on the other comes back.

**What the phones don't cover**: The React Native package (npm) and the Android package (Maven) are never run on a phone as published: the RN example uses the repository's `zb-react-native/` folder, and `zb-android/scripts/device-test.sh` builds the AAR from the source. What they run is libzb, the same as the Flutter package, at the same version: step 4 runs it on both phones, and the compose checks that both packages carry it for every target. What stays untested is each package's own thin layer as published: the Kotlin classes in the AAR, the RN JavaScript and Swift glue.
