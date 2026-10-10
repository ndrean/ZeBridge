# zb-android

libzb for Android apps written in Kotlin or Java: an AAR holding `libzb.so` for
arm64-v8a, armeabi-v7a and x86_64 (the emulator), the JNI layer, and one class,
`dev.zebridge.ZeBridge`. Android 10 (API 29) and newer. Which library for which host:
[DISTRIBUTION.md](../DISTRIBUTION.md).

```kotlin
dependencies {
    implementation("eu.zebridge:zebridge-android:0.1.0")   // Maven Central
}
```

## Use

```kotlin
val zb = ZeBridge(
    mapOf(
        "bridgeUrl" to "https://zb.example.com",
        "invite" to code,                // first run only: what your backend handed the device
        "tables" to listOf("orders"),
    ),
    context,                             // the replica and its identity go in the app's private storage
) { result -> /* rows changed or writes settled: re-query, then update the UI on its thread */ }

val open = zb.query("SELECT * FROM orders WHERE status = ?", "open")   // List<Map<String, Any?>>
zb.mutate("orders", "UPDATE", mapOf("id" to 7), mapOf("status" to "done"))
zb.close()
```

The options are the ones every ZeBridge client reads (CLIENTS.md). Everything else is
the library's job:

- **The thread.** libzb drives a client from one thread. `ZeBridge` owns that thread
  and polls on it between calls, so any thread may call it. Calls block until they
  have run (at most one poll, 250 ms by default): call from a background thread or
  `Dispatchers.IO`, not the main thread.
- **Changes.** The listener hears every poll that applied rows, settled writes, or
  brought requests to answer. It runs on the client's thread.
- **Errors.** A refused call throws `ZeBridgeException` with libzb's own words.
- **Revocation.** After the operator revokes the principal, `revoked` is true and the
  loop stops. The rows stay; `wipe()` removes them.

Enrollment is the library's: the first construction redeems `invite` at `bridgeUrl` and
keeps the identity beside the replica (mode 0600, app-private); later ones need neither,
and the JWT renews itself before it expires. `ZeBridge.createUser()` and
`ZeBridge.credsFileText(jwt, seed)` remain for an app that manages identities itself.

## Build

```sh
zb-android/scripts/build.sh        # → build/outputs/aar/zb-android-release.aar
```

Needs zig, the Android NDK and SDK, and a JDK (Android Studio's is used when `JAVA_HOME`
is not set). Per CPU, `zig build lib` makes libzb's static archive, and the NDK's clang
links it with `src/main/cpp/zb_jni.c` into one `libzb.so` that exports only the JNI
functions.

## Publish to Maven Central

The coordinates are `eu.zebridge:zebridge-android` (namespace `eu.zebridge`, verified on
central.sonatype.com by a DNS record on zebridge.eu). Once, on the release machine:

- a Central Portal user token in `~/.gradle/gradle.properties` (mode 0600), as
  `mavenCentralUsername` and `mavenCentralPassword`;
- the signing key: the GPG key "ZeBridge packages" signs every file through the `gpg`
  command (`signing { useGpgCmd() }`), and its public half is on a keyserver
  (`gpg --keyserver keyserver.ubuntu.com --send-keys <fingerprint>`).

Then, per release (the version is in `build.gradle.kts`):

```sh
zb-android/scripts/build.sh                                   # libzb.so per CPU, then the AAR
cd zb-android && gradle publishToMavenCentral                  # upload: the Portal validates it
```

The deployment then waits on central.sonatype.com (Deployments) until you press Publish.
`gradle publishAndReleaseToMavenCentral` does both at once.

## Test on a device

```sh
zb-android/scripts/device-test.sh
```

Runs `ZeBridgeDeviceTest` on the connected device against the dev stack, as `omar`,
through `adb reverse tcp:4222`: seed, query, a UTF-8 round trip, a write echoed back
from PostgreSQL, a delete, both verdicts, and the message a phone gives for the DuckDB
engine.
