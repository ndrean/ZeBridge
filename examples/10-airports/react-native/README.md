# 10-airports in React Native

The airports example on a phone, without a map, through libzb, the C client, behind
zb-react-native's Expo module, on iOS and Android: five cities, the
airports within 100 km of the one chosen (asked of the DuckDB service), and the shared
flight, whose departure and arrival any airport in the list can set. The flight is the same
row the web page and the Flutter app write, with the same registers.

The app id is `dev.zebridge.airports.rn`, so it installs beside the Flutter app.

## Android

Needs JDK 17 (React Native 0.76's Gradle does not run on newer ones; Android Studio's
bundled JDK is too new) and the Android SDK.

```sh
../../../zb-react-native/scripts/build-android.sh   # libzb for the module
pnpm install
npx expo prebuild --platform android --clean

export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export ANDROID_HOME=$HOME/Library/Android/sdk
EXPO_PUBLIC_ZB_INVITE=fl-… npx expo run:android --variant release --no-bundler
```

Build with `expo run:android`, not Gradle alone: its first step writes the autolinking
for Expo's modules, without which the Java compile cannot find `ExpoModulesPackage`.

The settings are read at build time, and two caches ignore them: Gradle reuses the
JavaScript bundle when no file changed, and Metro reuses each file's transformed code, where
Expo wrote the old values. After changing one, clear both:

```sh
find "$TMPDIR" -maxdepth 1 -name 'metro-*' -exec rm -rf {} +
rm -rf android/app/build/generated/assets/createBundleReleaseJsAndAssets
```

The settings:

* `EXPO_PUBLIC_ZB_INVITE`: the invite, for the first run. The identity is kept on the phone
  afterwards.
* `EXPO_PUBLIC_ZB_NATS_URL`: another NATS address than the one the bridge names, such as a
  leaf node (`tls://leaf.example.com:4222`). The replica and identity are kept per NATS
  host, so a leaf build needs its own invite.
* `EXPO_PUBLIC_ZB_BRIDGE_URL`: the bridge, `https://bridge.zebridge.eu` by default.

## iOS

libzb is built into the module first, then the app. A new bundle id needs Xcode to make
its signing profile, which `expo run:ios` does not allow, so the build is `xcodebuild`'s:

```sh
../../../zb-react-native/scripts/build-ios.sh     # libzb for the module
pnpm install
npx expo prebuild --platform ios --clean

EXPO_PUBLIC_ZB_INVITE=fl-… EXPO_PUBLIC_ZB_NATS_URL=tls://leaf1.example.com:4222 \
  xcodebuild -workspace ios/Airports.xcworkspace -scheme Airports -configuration Release \
  -destination id=<the iPhone's UDID> -allowProvisioningUpdates DEVELOPMENT_TEAM=<team> \
  -derivedDataPath ios/build build
xcrun devicectl device install app --device <device> ios/build/Build/Products/Release-iphoneos/Airports.app
```

`xcrun xctrace list devices` gives the UDID. On iOS libzb speaks TLS over TCP, so a leaf is
`tls://…:4222`, not the websocket. libzb carries its own TLS roots: the app passes no
`caFile`.

