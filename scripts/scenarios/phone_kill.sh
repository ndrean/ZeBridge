#!/bin/bash
# §10jc: scenario B on a phone with the app SIGKILLed (what jetsam does) at +20, +60, +110
# and +170 s after the load starts, relaunched at once; then the app is stopped, its
# replica copied and compared with PostgreSQL batch by batch.
#
#   ZB_UDID=<device udid> [RATE=20000] [LABEL=libzb-kill] \
#   [REPLICA=Documents/zebridge_bob_libzb.sqlite3 | Documents/SQLite/zebridge_bob.sqlite3] \
#     scripts/scenarios/phone_kill.sh
#
# Needs: fire_types set up (phone_live.py setup), the 06-large-table app installed and live
# on the table (its default engine is the one killed and relaunched), nothing else writing.
# `xcrun devicectl list devices` gives the udid.
set -u
R=$(cd "$(dirname "$0")/../.." && pwd)
D=${ZB_PHONE_OUT:-/tmp/zb-phone}; mkdir -p "$D"
UDID=${ZB_UDID:?set ZB_UDID to the phone (xcrun devicectl list devices)}
APP=dev.zebridge.largetable
cd $R; set -a; . ./.env.admin; . ./.env.bridge; set +a
LOG=$D/kill-run.log; : > $LOG
caffeinate -i scripts/scenarios/.venv/bin/python scripts/scenarios/phone_live.py run --rate ${RATE:-20000} --seconds 90 --burst-seconds 5 --label ${LABEL:-libzb-kill} >> $LOG 2>&1 &
RUN=$!
until grep -q "^burst:" $LOG; do sleep 1; kill -0 $RUN 2>/dev/null || { echo "run ended early"; cat $LOG; exit 1; }; done
t0=$(date +%s)
for at in 20 60 110 170; do
  while [ $(( $(date +%s) - t0 )) -lt $at ]; do sleep 1; done
  pid=$(xcrun devicectl device info processes --device $UDID 2>/dev/null | awk '/ZeLargeTable.app\/ZeLargeTable/ {print $1; exit}')
  echo "$(date +%T) +${at}s: SIGKILL pid ${pid:-none}" | tee -a $D/kills.txt
  [ -n "$pid" ] && xcrun devicectl device process signal --device $UDID --pid $pid --signal SIGKILL > /dev/null 2>&1
  sleep 2
  xcrun devicectl device process launch --device $UDID $APP > /dev/null 2>&1 && echo "$(date +%T)   relaunched" | tee -a $D/kills.txt
done
wait $RUN
grep -E "^RESULT|press" $LOG
# The verdict: the phone's replica against PostgreSQL, batch by batch.
rm -f $D/phone.sqlite3 $D/phone.sqlite3-wal $D/libzb-stderr.log
# REPLICA: Documents/zebridge_bob_libzb.sqlite3 (libzb) or Documents/SQLite/zebridge_bob.sqlite3 (zb-client-ts)
REPLICA=${REPLICA:-Documents/zebridge_bob_libzb.sqlite3}
# Stop the app first: a replica copied while it writes can tear (a main file without its
# WAL read as "malformed"); a killed process leaves the file and its WAL consistent.
pid=$(xcrun devicectl device info processes --device $UDID 2>/dev/null | awk '/ZeLargeTable.app\/ZeLargeTable/ {print $1; exit}')
[ -n "$pid" ] && xcrun devicectl device process signal --device $UDID --pid $pid --signal SIGKILL > /dev/null 2>&1; sleep 2
for sfx in "" -wal; do
  xcrun devicectl device copy from --device $UDID --domain-type appDataContainer --domain-identifier $APP --source "$REPLICA$sfx" --destination "$D/phone.sqlite3$sfx" > /dev/null 2>&1
done
xcrun devicectl device copy from --device $UDID --domain-type appDataContainer --domain-identifier $APP --source Documents/libzb-stderr.log --destination $D/libzb-stderr.log > /dev/null 2>&1
psql "$ADMIN_DATABASE_URL" -XtA -F'|' -c "select batch, count(*), sum(age), min(updated_at), max(updated_at) from fire_types group by batch order by batch" > $D/pg.txt
sqlite3 -separator '|' $D/phone.sqlite3 "select batch, count(*), sum(age) from fire_types group by batch order by batch" > $D/ph.txt
python3 - $D <<'EOF'
import sys
D = sys.argv[1]
pg = {int(l.split('|')[0]): l.strip().split('|') for l in open(D + '/pg.txt') if l.strip()}
ph = {int(l.split('|')[0]): l.strip().split('|') for l in open(D + '/ph.txt') if l.strip()}
bad = [(b, pg[b], ph.get(b)) for b in sorted(pg) if ph.get(b) is None or ph[b][1:3] != pg[b][1:3]]
extra = [b for b in ph if b not in pg]
print(f"VERDICT batches {len(pg)}, wrong {len(bad)}, extra {len(extra)}, rows pg {sum(int(p[1]) for p in pg.values())} phone {sum(int(q[1]) for q in ph.values())}, "
      f"sum pg {sum(int(p[2]) for p in pg.values())} phone {sum(int(q[2]) for q in ph.values())}")
for b, p, q in bad[:30]:
    print(' ', b, 'pg', p[1], p[2], '| phone', q[1] if q else None, q[2] if q else None, '| updated', p[3][11:23], '..', p[4][11:23])
EOF
