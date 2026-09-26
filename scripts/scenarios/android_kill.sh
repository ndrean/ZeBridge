#!/bin/bash
# §10jc on Android: scenario B with the app killed (`am force-stop`, immediate) at +20, +60,
# +110 and +170 s after the load starts and relaunched at once; then PostgreSQL's count and
# sum to compare with the app's "check" button. A release build's files cannot be copied
# out (not debuggable), so the verdict is the check, not a per-batch diff: missing rows
# show in the count, missing updates in the sum.
#
#   [RATE=5000] [LABEL=android-kill] [ZB_APP=dev.zebridge.zebridge_large_table] \
#     scripts/scenarios/android_kill.sh
#
# Needs: fire_types set up (phone_live.py setup), the 06-large-table Flutter app on the
# phone and live on the table, adb authorized, nothing else writing.
set -u
R=$(cd "$(dirname "$0")/../.." && pwd)
D=${ZB_PHONE_OUT:-/tmp/zb-android}; mkdir -p "$D"
ADB=${ADB:-$HOME/Library/Android/sdk/platform-tools/adb}
APP=${ZB_APP:-dev.zebridge.zebridge_large_table}
cd "$R"; set -a; . ./.env.admin; . ./.env.bridge; set +a
LOG=$D/kill-run.log; : > "$LOG"
ZB_PHONE_OUT=$D caffeinate -i scripts/scenarios/.venv/bin/python scripts/scenarios/phone_live.py run \
  --rate "${RATE:-5000}" --seconds 90 --burst-seconds 5 --label "${LABEL:-android-kill}" >> "$LOG" 2>&1 &
RUN=$!
until grep -q "^burst:" "$LOG"; do sleep 1; kill -0 $RUN 2>/dev/null || { echo "run ended early"; cat "$LOG"; exit 1; }; done
t0=$(date +%s)
for at in 20 60 110 170; do
  while [ $(( $(date +%s) - t0 )) -lt $at ]; do sleep 1; done
  pid=$(timeout 10 "$ADB" shell pidof "$APP" 2>/dev/null)
  timeout 20 "$ADB" shell am force-stop "$APP" > /dev/null 2>&1
  echo "$(date +%T) +${at}s: force-stop (pid ${pid:-none})" | tee -a "$D/kills.txt"
  sleep 2
  # monkey exits non-zero even when it launched the app: report the new pid instead.
  timeout 20 "$ADB" shell monkey -p "$APP" -c android.intent.category.LAUNCHER 1 > /dev/null 2>&1; sleep 2
  echo "$(date +%T)   relaunched (pid $(timeout 10 "$ADB" shell pidof "$APP" 2>/dev/null || echo none))" | tee -a "$D/kills.txt"
done
wait $RUN
grep -E "^RESULT|press" "$LOG"
