#!/bin/sh
# Run the AAR's instrumented test on a connected device against the dev stack.
#
#   zb-android/scripts/build.sh && zb-android/scripts/device-test.sh
#
# The device reaches the Mac's NATS as 127.0.0.1:4222 through `adb reverse`; omar's
# dev creds go into the TEST apk only (build/zbtest-assets). The expected live-row
# count and the bridge's grammar hash are read here and passed as arguments.
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
root=$(cd "$here/.." && pwd)
adb=${ANDROID_HOME:-$HOME/Library/Android/sdk}/platform-tools/adb

$adb reverse tcp:4222 tcp:4222 >/dev/null
mkdir -p "$here/build/zbtest-assets"
cp "$root/scripts/native/creds/omar.creds" "$here/build/zbtest-assets/omar.creds"

set -a; . "$root/.env.admin"; set +a
live=$(psql "$ADMIN_DATABASE_URL" -XAtc "SELECT count(*) FROM test_types WHERE tenant_id = 'kilo' AND deleted_at IS NULL")
port=$(ps -axo command | sed -n 's/.*zig-out\/bin\/bridge .*--port \([0-9]*\).*/\1/p' | head -1)
hash=$(curl -s -D - -o /dev/null "http://127.0.0.1:${port:-9090}/grammar" | tr -d '\r' | sed -n 's/^x-grammar-hash: //Ip')

export JAVA_HOME="${JAVA_HOME:-/Applications/Android Studio.app/Contents/jbr/Contents/Home}"
cd "$here" && gradle --quiet connectedAndroidTest \
  -Pandroid.testInstrumentationRunnerArguments.expectedLive="$live" \
  ${hash:+-Pandroid.testInstrumentationRunnerArguments.grammarHash="$hash"}
echo "ok: device test passed (live rows $live, grammar ${hash:-not checked})"
