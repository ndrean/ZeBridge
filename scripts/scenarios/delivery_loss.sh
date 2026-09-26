#!/bin/bash
# §10jc: a follower that loses deliveries in transit and is killed mid-stream must still
# end EXACT. The dev stack, table fire_types (phone_live.py's), a libzb `zb` follower on
# the Mac in place of a phone; the load is phone_live.py's scenario B.
#
#   scripts/scenarios/delivery_loss.sh [zb-binary | ts] [drop-every]
#     zb-binary   default libzb/zig-out/rf/bin/zb (the fixed one; a pre-fix build is the control)
#     ts          zb-client-ts instead: examples/04-node-consumer/follow-worker.ts
#     drop-every  ZB_TEST_DROP_DELIVERY: every Nth delivery discarded as lost (0 = kills only)
#
# The follower is SIGKILLed every 15 s during the load and restarted on the same replica.
# PASS: every batch's count and sum(age) equal PostgreSQL's.
set -u
R=$(cd "$(dirname "$0")/../.." && pwd); cd "$R"
ZB=${1:-libzb/zig-out/rf/bin/zb}
DROP=${2:-41}
W=${ZB_RUN_DIR:-/tmp/zb-delivery-loss}; rm -rf "$W"; mkdir -p "$W"
set -a; . ./.env.admin; . ./.env.bridge; set +a
PY=scripts/scenarios/.venv/bin/python
$PY scripts/scenarios/phone_live.py setup --preload 200000 | tail -1
follow() {
  if [ "$ZB" = ts ]; then
    (cd examples/04-node-consumer && ZB_TEST_DROP_DELIVERY=$DROP ZB_DB="$W/replica.sqlite3" ZB_CREDS="$R/scripts/native/creds/bob.creds" \
      ZB_PRINCIPAL=bob ZB_TABLES=fire_types NATS_URL=nats://127.0.0.1:4222 \
      exec node --experimental-strip-types follow-worker.ts < /dev/null >> "$W/zb.log" 2>&1) &
  else
    ZB_TEST_DROP_DELIVERY=$DROP ZB_TAIL_TRACE=1 "$ZB" sync --creds scripts/native/creds/bob.creds --principal bob \
      --tables fire_types --db "$W/replica.sqlite3" --stream --poll-ms 200 >> "$W/zb.log" 2>&1 &
  fi
  echo $! > "$W/zb.pid"
}
follow
ZB_PHONE_OUT=$W $PY scripts/scenarios/phone_live.py run --rate 10000 --seconds 60 --burst-seconds 5 --label mac > "$W/run.log" 2>&1 &
RUN=$!
until grep -q "^burst:" "$W/run.log"; do sleep 1; kill -0 $RUN 2>/dev/null || { cat "$W/run.log"; exit 1; }; done
for i in 1 2 3 4; do
  sleep 15
  kill -9 "$(cat "$W/zb.pid")" 2>/dev/null; echo "$(date +%T) kill $i" | tee -a "$W/kills.txt"
  sleep 1; follow
done
wait $RUN
grep -E "^RESULT" "$W/run.log" | cut -c1-300
kill "$(cat "$W/zb.pid")" 2>/dev/null; sleep 2
psql "$ADMIN_DATABASE_URL" -XtA -F'|' -c "select batch, count(*), sum(age) from fire_types group by batch order by batch" > "$W/pg.txt"
sqlite3 -separator '|' "$W/replica.sqlite3" "select batch, count(*), sum(age) from fire_types group by batch order by batch" > "$W/zb.txt"
echo "follower log: lost-in-transit $(grep -c 'discarded as lost' "$W/zb.log"), gaps seen $(grep -c 'delivery lost in transit' "$W/zb.log"), recreated $(grep -cE 'recreating the drain|reopening the tail from position|drain idle with deliveries|tail recreated' "$W/zb.log"), duplicates skipped $(grep -o '[0-9]* duplicate(s) acked' "$W/zb.log" | awk '{s+=$1} END {print s+0}')"
python3 - "$W" <<'EOF'
import sys
W = sys.argv[1]
rd = lambda f: {int(l.split('|')[0]): l.strip().split('|')[1:] for l in open(f) if l.strip()}
pg, zb = rd(W + '/pg.txt'), rd(W + '/zb.txt')
bad = [b for b in pg if zb.get(b) != pg[b]] + [b for b in zb if b not in pg]
rows = lambda d: sum(int(v[0]) for v in d.values()); ages = lambda d: sum(int(v[1]) for v in d.values())
print(f"{'PASS' if not bad else 'FAIL'}: {len(pg)} batches, {len(bad)} wrong; rows pg {rows(pg):,} replica {rows(zb):,}; sum(age) pg {ages(pg):,} replica {ages(zb):,}")
for b in sorted(bad)[:10]:
    print('  batch', b, 'pg', pg.get(b), 'replica', zb.get(b))
sys.exit(1 if bad else 0)
EOF
