#!/bin/bash
# A phone's Wi-Fi on the Mac, for ONE client (NOTES §10jc/§10je). Shapes the traffic between
# NATS (port 4222) and whatever connects to the Mac's LAN address — a follower started with
# `--url nats://<LAN>:4222` — while the bridge and the local services, on 127.0.0.1, stay
# unshaped: slowing them would measure a slower bridge, not a slower client.
#
#   sudo scripts/scenarios/slowlink.sh start [mbit] [delay_ms] [loss]   # default 20 30 0.005
#   sudo scripts/scenarios/slowlink.sh status
#   sudo scripts/scenarios/slowlink.sh stop
#
# `delay_ms` applies each way (30 → a 60 ms round trip); `loss` is the share of NATS→client
# packets dropped (0.005 = 0.5%), which TCP retransmits.
#
# It never touches the main pf ruleset. One pair of dummynet rules goes into the sub-anchor
# com.apple/zb-shape, which the default /etc/pf.conf already evaluates (`dummynet-anchor
# "com.apple/*"`); two dnctl pipes; pf enabled BY REFERENCE (`pfctl -E`) and released with
# the same token (`pfctl -X`), so pf ends in the state it started in. `stop` removes all three.
set -eu
[ "$(id -u)" = 0 ] || { echo "run it with sudo"; exit 1; }
ANCHOR=com.apple/zb-shape
TOKEN_FILE=/var/tmp/zb-slowlink.token
DOWN=7101; UP=7102
# The address of the interface the default route uses (Wi-Fi or Ethernet, whichever is
# active), not a fixed en0. `sudo` drops the caller's environment, so an override goes on
# sudo's own command line: sudo ZB_LAN_IP=192.168.1.11 scripts/scenarios/slowlink.sh start
IFACE=$(/sbin/route -n get default 2>/dev/null | awk '/interface:/ {print $2; exit}')
LAN=${ZB_LAN_IP:-$( [ -n "$IFACE" ] && /usr/sbin/ipconfig getifaddr "$IFACE" 2>/dev/null || true)}
[ -n "$LAN" ] || { echo "no LAN address (default route via '${IFACE:-none}'): sudo ZB_LAN_IP=<address> $0 $*"; exit 1; }

case "${1:-}" in
start)
  BW=${2:-20}; DELAY=${3:-30}; LOSS=${4:-0.005}
  dnctl pipe $DOWN config bw "${BW}Mbit/s" delay "$DELAY" plr "$LOSS"   # NATS → client
  dnctl pipe $UP config bw "${BW}Mbit/s" delay "$DELAY"                  # client → NATS
  printf '%s\n' \
    "dummynet in quick on lo0 proto tcp from $LAN port 4222 to $LAN pipe $DOWN" \
    "dummynet in quick on lo0 proto tcp from $LAN to $LAN port 4222 pipe $UP" |
    pfctl -q -a "$ANCHOR" -f -
  if [ ! -s "$TOKEN_FILE" ]; then
    pfctl -E 2>&1 | sed -n 's/.*[Tt]oken *: *\([0-9][0-9]*\).*/\1/p' > "$TOKEN_FILE"
  fi
  echo "shaping NATS ↔ $LAN (${IFACE:-set by ZB_LAN_IP}): ${BW} Mbit/s, ${DELAY} ms each way, ${LOSS} loss toward the client"
  echo "connect the client to nats://$LAN:4222; 127.0.0.1 is untouched. Stop: sudo $0 stop"
  ;;
status)
  echo "── rules in $ANCHOR"; pfctl -a "$ANCHOR" -s dummynet 2>/dev/null || true
  echo "── pipes";            dnctl list 2>/dev/null | grep -E "^0?($DOWN|$UP):" -A1 || echo "(none)"
  echo "── pf";               pfctl -s info 2>/dev/null | head -1
  echo "── token";            cat "$TOKEN_FILE" 2>/dev/null || echo "(none)"
  ;;
stop)
  pfctl -q -a "$ANCHOR" -F all 2>/dev/null || true
  dnctl pipe delete $DOWN 2>/dev/null || true
  dnctl pipe delete $UP 2>/dev/null || true
  if [ -s "$TOKEN_FILE" ]; then pfctl -X "$(cat "$TOKEN_FILE")" 2>/dev/null || true; fi
  rm -f "$TOKEN_FILE"
  echo "shaping removed; pf released to its previous state"
  ;;
*)
  sed -n '2,12p' "$0"; exit 2 ;;
esac
