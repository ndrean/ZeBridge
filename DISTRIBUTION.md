# Distribution — which libzb for which host

ZeBridge has two client libraries. **zb-client-ts** is TypeScript source: browsers
and Node import it, with nothing native to build. It ships libzb's core as a prebuilt
WebAssembly file (`wasm/zb_core.wasm`); after a change to that core, `pnpm wasm` rebuilds
it (it needs Zig). **libzb** is a native
library with a C ABI. It has to be built per platform, and it can carry up to three
local storage engines. This page says which build a host needs, and why.

## What goes into libzb

| part | what it is | how it gets in |
| --- | --- | --- |
| core | the protocol, the NATS client, the C ABI | always compiled in |
| SQLite + zstd | the default engine, and chain decompression | compiled in from pinned sources (`-Dvendor=true`), or linked from the machine |
| TLS roots | Mozilla's CA bundle, for `https://` and `tls://` on iOS and Android, where Zig reads no trust store | compiled into iOS and Android builds only (about 190 KB); desktop builds read the system's store |
| PostgreSQL engine | a replica in a PostgreSQL database (`dbUrl`) | compiled in; libpq opened at run time |
| DuckDB engine | an analytical replica in a `.duckdb` file (`engine: "duckdb"`) | compiled in; libduckdb (~47 MB) opened at run time |

The rule: **compile in what you cannot count on, open at run time what only some hosts
install.** Every build carries all three engines. libduckdb and libpq are opened the
first time a client asks for that engine, so a SQLite-only client loads on any machine,
and a missing library is a message at connect:

    zb_client_connect failed: engine 'duckdb' asked for, but its library is not
    installed (looked for libduckdb.dylib); install it, or set ZB_DUCKDB_LIB to its path

libzb looks in `ZB_DUCKDB_LIB` / `ZB_LIBPQ_LIB` first, then by name, then in the usual
install places (Homebrew, `/usr/local/lib`, the distro's lib directory). A library that
opens but lacks a function libzb calls is named too: it is too old, or not that
library.

## The artifacts

| artifact | targets | SQLite / zstd | PostgreSQL | DuckDB | form | used from |
| --- | --- | --- | --- | --- | --- | --- |
| **libzb-ios** | iPhone arm64, simulator arm64 | compiled in | – | – | xcframework (static) | Swift, Flutter (dart:ffi), React Native (Expo module) |
| **libzb-android** | arm64-v8a, armeabi-v7a, x86_64 | compiled in | – | – | AAR (`zb-android`), or a `.so` per CPU | Kotlin/Java (`dev.zebridge.ZeBridge`), Flutter (dart:ffi), React Native (Expo module) |
| **libzb-desktop** | macOS, Linux, Windows | compiled in | if libpq is installed | if libduckdb is installed | shared library | apps and services: Python (ctypes), Dart, JVM (JNA, FFM), .NET (P/Invoke) |

On top of these, one package per language gives libzb its idioms — `zb-python`,
`zb-android` (the AAR), `zb-dart`, `zb-react-native` (an Expo module) — each a thin binding with no behavior of its own
([CLIENTS.md](CLIENTS.md#bindings-the-rule)); zb-client-ts covers JavaScript without libzb.

A phone never has libpq or libduckdb, so on a phone those two engines answer with the
message above. The desktop library is the same file for an app and for a service; what
differs is what is installed next to it.

Built today: iOS (both slices), the Android AAR (all three CPUs), macOS, and Linux
(x86_64 or aarch64, glibc 2.35), which also ships as release packages
([Linux packages](#linux-packages)). Not yet: Windows release builds (on Windows the two
optional engines do not load yet), and packages for the other platforms (see
[What is missing](#what-is-missing)).

## How each host uses it

**iOS (Swift, Flutter, React Native).** Swift and React Native link a static library,
packed as an xcframework; React Native wraps it in an Expo module
(`zb-react-native/scripts/build-ios.sh`). Flutter and Dart get a dynamic library from the
`zebridge` package's build hook, which Flutter embeds as a framework.

**Android.** A Kotlin or Java app adds the AAR ([zb-android](zb-android/README.md)): the
JNI layer, a `ZeBridge` class that owns the client's thread, and `libzb.so` per CPU. React
Native's Expo module calls the same JNI layer (`zb-react-native/scripts/build-android.sh`).
Flutter gets `libzbcore.so` from the `zebridge` package's build hook. SQLite is compiled in either
way: Android's own SQLite is not reachable from native code (the NDK does not expose it).

**Desktop.** A shared library that the host loads at run time. Python uses ctypes (`pip install zebridge`: the wheels for Linux and macOS carry the library, built by `zb-python/scripts/build-wheels.sh`),
Dart uses dart:ffi, a desktop JVM can use JNA or Java 22's FFM (no glue code needed),
.NET uses P/Invoke.

**Browsers, Node.** zb-client-ts (`npm install @zebridge/client`), not libzb. It needs no native build: libzb's core comes with it as WebAssembly.

## DuckDB: why it is not compiled in

libduckdb is 47 MB and most hosts never use it, so libzb opens the installed one instead
of embedding it. Three things a service must know:

1. **The machine must have libduckdb** for the DuckDB engine. Nothing else needs it.
2. **Versions must agree.** libzb writes the `.duckdb` file with its DuckDB version. An
   older DuckDB cannot open a file written by a newer one, so a service that also uses
   DuckDB directly (for example Python's `duckdb` package) should use the same version
   or a newer one.
3. **One writer per file.** DuckDB lets one process write a file at a time, and libzb
   holds it while running. Read through libzb's `query` (09-event does), or open the
   file yourself after libzb closes it.

**A Python service that also uses the `duckdb` package** can make libzb use the
package's own DuckDB, so both sides read and write the same file version. The wheel's
native module exports DuckDB's C functions; point `ZB_DUCKDB_LIB` at it before the first
DuckDB client opens:

```python
import glob, os, duckdb

site = os.path.dirname(os.path.dirname(duckdb.__file__))
os.environ["ZB_DUCKDB_LIB"] = glob.glob(os.path.join(site, "_duckdb*.so"))[0]
# … zb_client_connect({"engine": "duckdb", "dbPath": "replica.duckdb", …}) …
```

Checked with duckdb 1.5.5 on macOS: libzb seeded a replica through the wheel's copy,
and `duckdb.connect(path, read_only=True)` read it after libzb closed it. Point 3 still
holds: while libzb has the file open, read through `zb_client_query`.

Compiling DuckDB in is possible (`libduckdb_static.a` ships with it): one
self-contained file of about 50 MB with the DuckDB version pinned. That suits a sealed
appliance; it is not the default.

## Sizes

Measured on macOS arm64:

| build | size |
| --- | --- |
| shared, SQLite and zstd from the machine | 2.3 MB |
| shared, SQLite and zstd compiled in, all three engines | 4.2 MB, depends on the system library only |
| static archive | 22.5 MB |

The archive is not what an app pays: it keeps every symbol, and the linker takes only
what the app uses. Compare linked libraries with linked libraries. The DuckDB and
PostgreSQL engines add about 150 KB of code; the libraries themselves stay on the
machine that runs the service.

## Building

    # desktop, apps and services alike, in libzb/ with Zig 0.16.0: nothing needed on the machine to build or load
    zig build lib -Doptimize=ReleaseFast -Dvendor=true

    # phones
    zb-react-native/scripts/build-ios.sh
    zb-react-native/scripts/build-android.sh
    zb-dart/scripts/build-prebuilt.sh  # Dart and Flutter: every target, in zb-dart/prebuilt/
    zb-android/scripts/build.sh        # the AAR

    # Linux servers, from a Mac: bridge, bridge_sweeper, libzbcore.so and zb-respond
    deploy/build-linux.sh              # x86_64; `aarch64` for an ARM server

    # the C header, after any change to the exported functions
    python3 libzb/scripts/gen-header.py

The engines' headers are in `libzb/include-engines/`, so building needs neither DuckDB
nor PostgreSQL installed.

**The TLS roots, before a release.** iOS and Android builds carry Mozilla's CA bundle from
`libzb/src/roots/`. It is refreshed by hand:

    libzb/scripts/refresh-roots.sh

The script downloads the bundle curl publishes (`curl.se/ca/cacert.pem`), checks it against
curl's published SHA-256, and records the date of Mozilla's data. Review the diff and commit
it like any change. An iOS or Android build prints a warning once the bundle is more than six
months old. Users get the new roots with the next build of the app.

Every build embeds `grammar.json` and reports its hash (`zb_grammar_hash`). A client
whose hash differs from the bridge's (`X-Grammar-Hash` on `/grammar`) refuses to
connect: rebuild every native client when `grammar.json` changes.

## Linux packages

For Debian 12+ and Ubuntu 22.04+, amd64 and arm64, from the APT repository at
`packages.zebridge.eu`:

    curl -fsSL https://packages.zebridge.eu/zebridge.gpg | sudo tee /usr/share/keyrings/zebridge.gpg >/dev/null
    echo "deb [signed-by=/usr/share/keyrings/zebridge.gpg] https://packages.zebridge.eu stable main" \
      | sudo tee /etc/apt/sources.list.d/zebridge.list
    sudo apt update
    sudo apt install zebridge libzb

The repository is signed: apt refuses it without that key. The key's fingerprint is on
the repository's page, `https://packages.zebridge.eu`. `sudo apt upgrade` installs later releases.

**A release**, from a Mac. The version is the one in `build.zig.zon`:

    deploy/package-linux.sh x86_64     # build and package, in dist/<version>/
    deploy/package-linux.sh aarch64 --no-build  # or build both: run without --no-build
    deploy/test-linux.sh               # install them on clean Debian 12, 13, Ubuntu 22.04, 24.04
    deploy/apt-repo.sh                 # add them to dist/apt/, index and sign it

then upload the whole `dist/apt/` folder to the static host (a Cloudflare Pages project).
`dist/apt/` keeps every release, so it is uploaded whole each time. It is signed with the GPG key
"ZeBridge packages" (Ed25519), which lives on the release machine only; keep a copy of it
(`gpg --export-secret-keys --armor "ZeBridge packages"`) somewhere safe.

In `dist/<version>/`:

| file | holds | needs |
| --- | --- | --- |
| `zebridge_<version>_<arch>.deb` | `bridge` and `bridge_sweeper` in `/usr/bin`; example systemd units in `/usr/share/doc/zebridge/examples` | `libpq5` (14 or later), `libzstd1`: apt installs them |
| `libzb_<version>_<arch>.deb` | `libzbcore.so`, `/usr/include/zb.h`, `zb-respond` | glibc only; `libpq5` is suggested, for the PostgreSQL engine |
| `zebridge-<version>-linux-<arch>.tar.gz`, `libzb-<version>-linux-<arch>.tar.gz` | the same files, to unpack anywhere | the same, installed by hand |
| `SHA256SUMS` | the checksum of every file | |

A downloaded package installs the same way: `sudo apt install ./zebridge_0.1.0_amd64.deb`.

Every binary prints its version with `--version`. The binaries keep their debug
information, so a crash prints the file and line it happened at.

`libzb/include/zb.h` declares the C ABI. `libzb/scripts/gen-header.py` writes it from
the exported functions in `src/capi.zig`, and `libzb/python/abi_check.py` fails when it is
out of date. A C program builds with `cc app.c -lzbcore`.

## What is missing

- **Windows loading** for the two optional engines (`LoadLibrary`); SQLite works there.
- **Publishing the AAR** to a Maven repository; today it is built from source.
- **Release packaging for the other platforms:** an xcframework zip, the AAR, and
  tarballs for macOS and Windows. Linux has its packages
  ([Linux packages](#linux-packages)).
