#!/usr/bin/env bash
# The C-side leak check for libzb's FFI boundary, in BOTH build modes.
#
# Why this exists: `std.testing.allocator` catches Zig allocations only. A leaked
# `sqlite3*` is malloc'd by C and sails straight past it — which is exactly the handle
# libzb's init used to leave open on a failed connect. macOS `leaks` sees the malloc
# zones, so it sees what the Zig tests cannot.
#
# ⚠️ Attaches to a LIVE pid rather than using `leaks --atExit`, which crashes on this
# macOS for any ctypes-loaded dylib (reproduced with /usr/lib/libsqlite3.dylib —
# "Assertion failed: previous_refcount >= refs … uniquing_table_mutator.c"). Same
# method leaksoak.py uses.
#
# Both modes, because they differ: Debug keeps safety checks and its own bookkeeping,
# ReleaseFast strips them — a leak masked by one can be visible in the other.
set -uo pipefail
cd "$(dirname "$0")/.."
FAILED=0

for MODE in Debug ReleaseFast; do
  echo "── $MODE ──"
  ( cd libzb && zig build -Doptimize="$MODE" >/dev/null 2>&1 ) || { echo "  build failed"; FAILED=1; continue; }

  ( cd libzb && python3 python/abuse.py --hold >/tmp/zb-abuse-$MODE.log 2>&1 ) &
  DRIVER=$!
  for _ in $(seq 1 60); do grep -q READY /tmp/zb-abuse-$MODE.log 2>/dev/null && break; sleep 0.5; done

  PID=$(pgrep -f "python/abuse.py --hold" | head -1)
  if [ -z "$PID" ]; then echo "  harness did not start"; FAILED=1; kill $DRIVER 2>/dev/null; continue; fi

  REPORT=$(leaks --nocontext "$PID" 2>/dev/null | grep -E "leaks for [0-9]+ total leaked bytes")
  echo "  $REPORT"
  grep -qE "^\s*PASS" /tmp/zb-abuse-$MODE.log || { echo "  abuse suite FAILED — see /tmp/zb-abuse-$MODE.log"; FAILED=1; }
  echo "$REPORT" | grep -qE " 0 leaks for 0 total leaked bytes" || { echo "  ✗ leaked"; FAILED=1; }

  kill "$PID" 2>/dev/null; wait $DRIVER 2>/dev/null
done

[ "$FAILED" = 0 ] && echo "✓ no C-side leaks in either mode" || echo "✗ leak check failed"
exit $FAILED
