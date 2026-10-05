# 10-airports in React Native

The airports example on a phone through zb-client-ts, without a map: five cities, the
airports within 100 km of the one chosen (asked of the DuckDB service), and the shared
flight, whose departure and arrival any airport in the list can set. The flight is the same
row the web page and the Flutter app write, with the same registers.

The app id is `dev.zebridge.airports.rn`, so it installs beside the Flutter app.

## Android

Needs JDK 17 (React Native 0.76's Gradle does not run on newer ones; Android Studio's
bundled JDK is too new) and the Android SDK.

```sh
pnpm install
npx expo prebuild --platform android --clean

export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export ANDROID_HOME=$HOME/Library/Android/sdk
EXPO_PUBLIC_ZB_INVITE=fl-… npx expo run:android --variant release --no-bundler
```

Build with `expo run:android`, not Gradle alone: its first step writes the autolinking
for Expo's modules, without which the Java compile cannot find `ExpoModulesPackage`.

The settings are read at build time:

* `EXPO_PUBLIC_ZB_INVITE`: the invite, for the first run. The identity is kept on the phone
  afterwards.
* `EXPO_PUBLIC_ZB_NATS_URL`: another NATS address than the one the bridge names, such as a
  leaf node (`wss://leaf.example.com:8443`). The replica and identity are kept per NATS
  host, so a leaf build needs its own invite.
* `EXPO_PUBLIC_ZB_BRIDGE_URL`: the bridge, `https://bridge.zebridge.eu` by default.
