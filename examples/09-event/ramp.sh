#!/bin/bash
# 09-event: one step of a load ramp — N sensors for S seconds, measured end to end.
#
#   examples/09-event/ramp.sh 200        # 200 sensors × 50 readings/s = 10,000/s, 30 s
#   examples/09-event/ramp.sh 400 60
#
# Runs one sensors.py per 100 sensors (disjoint sensor ids) and ask.py watch, samples the
# MUTATIONS backlog (the bridge's ingress consumer) every 2 s, then prints one summary:
# readings sent, verdicts, publish refusals, ack p50/p99 over EVERY write (merged across
# processes), the newest reading's age, and the largest backlog seen.
#
# Needs the dev stack, the table (provision.py) and event_service.py running. The bridge's
# ingress lanes are set where it starts: ZB_INGRESS_LANES=1..8 (default 1).
#
# PRINCIPALS spreads the sensor processes round-robin over writers, principal:tenant pairs
# (default bob:globex). With several, each principal has its own MUTATIONS subject — its own
# 5,000-write queue cap — and each tenant its own CDC stream; the run ends with a per-tenant
# check, the service's copy against PostgreSQL, asked as a principal of that tenant:
#   PRINCIPALS=bob:globex,mary:globex,alice:acme,nina:tango,omar:kilo examples/09-event/ramp.sh 500
set -u
R=$(cd "$(dirname "$0")/../.." && pwd); cd "$R"
N=${1:?usage: ramp.sh SENSORS [SECONDS]}; S=${2:-30}
OUT=${RAMP_OUT:-/tmp/zb-ramp}/s$N; rm -rf "$OUT"; mkdir -p "$OUT"
PY=scripts/scenarios/.venv/bin/python
NATS=(nats --creds scripts/native/creds/bridge.creds --inbox-prefix _INBOX.bridge -s "${NATS_URL:-nats://127.0.0.1:4222}")

IFS=, read -r -a PAIRS <<< "${PRINCIPALS:-bob:globex}"
procs=$(( (N + 99) / 100 ))
for ((p = 0; p < procs; p++)); do
  n=$(( N - p * 100 )); [ "$n" -gt 100 ] && n=100
  pair=${PAIRS[$(( p % ${#PAIRS[@]} ))]}
  $PY -u examples/09-event/sensors.py --sensors "$n" --first-id $(( p * 100 )) --seconds "$S" \
      --principal "${pair%%:*}" --tenant "${pair##*:}" --ack-file "$OUT/ack$p.txt" > "$OUT/sensors$p.log" 2>&1 &
done
$PY -u examples/09-event/ask.py watch --seconds $(( S + 3 )) > "$OUT/watch.log" 2>&1 &

end=$(( $(date +%s) + S + 15 ))
while [ "$(date +%s)" -lt "$end" ]; do
  "${NATS[@]}" consumer info MUTATIONS bridge_mutations_worker --json 2>/dev/null |
    python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("num_pending",0), d.get("num_ack_pending",0))' >> "$OUT/backlog.txt" 2>/dev/null
  sleep 2
done
wait

python3 - "$OUT" "$N" "$S" <<'EOF'
import glob, re, sys
out, n, s = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
sent = errors = unverdicted = 0
verdicts = {}
for f in sorted(glob.glob(f"{out}/sensors*.log")):
    text = open(f).read()
    done = [l for l in text.splitlines() if l.startswith("done")]
    if not done:
        print(f"⚠️ {f}: no summary line"); continue
    l = done[-1]
    sent += int(re.search(r"sent ([\d,]+)", l).group(1).replace(",", ""))
    errors += int(re.search(r"publish errors (\d+)", l).group(1))
    for k, v in re.findall(r"(\w+) ([\d,]+)", re.search(r"verdicts: (.*?)  ack", l).group(1)):
        verdicts[k] = verdicts.get(k, 0) + int(v.replace(",", ""))
    m = re.search(r"([\d,]+) writes without a verdict", text)
    unverdicted += int(m.group(1).replace(",", "")) if m else 0
acks = sorted(float(x) for f in glob.glob(f"{out}/ack*.txt") for x in open(f).read().split())
p = lambda q: acks[min(len(acks) - 1, int(q * len(acks)))] if acks else float("nan")
age = [l for l in open(f"{out}/watch.log").read().splitlines() if "age over" in l]
back = [tuple(map(int, l.split())) for l in open(f"{out}/backlog.txt") if l.strip()] if glob.glob(f"{out}/backlog.txt") else []
refusal = sorted({m for f in glob.glob(f"{out}/sensors*.log") for m in re.findall(r"publish failed: (.{0,90})", open(f).read())})
print(f"{n} sensors, {n * 50:,} readings/s asked, {s:g} s")
print(f"  sent {sent:,} ({sent / s:,.0f}/s)  verdicts {verdicts}  publish refused {errors:,}  no verdict yet {unverdicted:,}")
print(f"  ack p50 {p(.5):.1f} ms  p90 {p(.9):.1f} ms  p99 {p(.99):.1f} ms  max {acks[-1] if acks else float('nan'):.0f} ms  ({len(acks):,} acks)")
print(f"  MUTATIONS backlog max {max((b[0] for b in back), default=0):,} pending, {max((b[1] for b in back), default=0):,} in flight")
print("  " + (age[-1] if age else "no watch summary"))
for r in refusal[:3]:
    print(f"  refusal: {r}")
EOF

# Per tenant: the service's copy against PostgreSQL (count and sum), asked as the first
# principal of each tenant — a question can only be asked for the asker's own tenant.
sleep 3
seen=" "
for pair in "${PAIRS[@]}"; do
  pr=${pair%%:*}; tn=${pair##*:}
  case "$seen" in *" $tn "*) continue ;; esac
  seen="$seen$tn "
  pg=$(psql "${ADMIN_DATABASE_URL:?load .env.admin}" -XtA -F' ' -c \
       "SELECT count(*), round(sum(value)::numeric, 3) FROM sensor_events WHERE tenant_id = '$tn' AND deleted_at IS NULL")
  svc=$($PY examples/09-event/ask.py freshness --principal "$pr" --tenant "$tn" --db "/tmp/events-asker-$pr.sqlite3" 2>/dev/null |
        python3 -c 'import sys,json; j=json.load(sys.stdin); print(j.get("rows"), j.get("sum_value"))' 2>/dev/null)
  read -r pc ps <<< "$pg"; read -r sc ss <<< "$svc"
  if [ "$pc" = "$sc" ] && python3 -c "import sys; sys.exit(0 if abs(float('$ps') - float('$ss')) < 0.01 else 1)" 2>/dev/null; then v=exact; else v=DIFFERS; fi
  echo "  tenant $tn: PostgreSQL $pc rows, sum $ps | service $sc rows, sum $ss — $v"
done
