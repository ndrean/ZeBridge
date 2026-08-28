#!/usr/bin/env bash
# Verify a zb-soak run: every uid it wrote must be tombstoned or reaped, never alive.
#
#   scripts/soak-check.sh libzb/soak-uids.txt
#
# The property: a delete is never lost. Each cycle INSERTs a row and DELETEs it, so
# afterwards each uid is in exactly one of two acceptable states —
#
#   tombstoned   the row is there with deleted_at set (the sweeper has not reached it)
#   reaped       the row is gone (the sweeper passed GC_THRESHOLD_MS and removed it)
#
# and one unacceptable one:
#
#   ALIVE        present with deleted_at IS NULL — a DELETE was dropped, or a row was
#                resurrected. This is the failure the GC watermark exists to prevent.
set -euo pipefail
LEDGER="${1:-libzb/soak-uids.txt}"
URL="${ADMIN_DATABASE_URL:-postgres://postgres:postgres_password@127.0.0.1:5432/postgres}"
TABLE="${ZB_SOAK_TABLE:-test_types}"

[ -s "$LEDGER" ] || { echo "no ledger at $LEDGER — did the soak run?"; exit 1; }
TOTAL=$(grep -c . "$LEDGER")

# One round trip: the ledger becomes a VALUES list joined against the table.
SQL=$(printf "WITH ledger(uid) AS (VALUES %s)\n" \
        "$(sed "s/.*/('&'::uuid),/" "$LEDGER" | tr -d '\n' | sed 's/,$//')")
SQL+="
SELECT
  (SELECT count(*) FROM ledger)                                              AS ledger_rows,
  (SELECT count(*) FROM ledger l JOIN $TABLE t USING (uid))                  AS still_present,
  (SELECT count(*) FROM ledger l JOIN $TABLE t USING (uid)
     WHERE t.deleted_at IS NOT NULL)                                         AS tombstoned,
  (SELECT count(*) FROM ledger l JOIN $TABLE t USING (uid)
     WHERE t.deleted_at IS NULL)                                             AS alive_BAD,
  (SELECT count(*) FROM ledger l LEFT JOIN $TABLE t USING (uid)
     WHERE t.uid IS NULL)                                                    AS reaped;"

echo "ledger: $LEDGER ($TOTAL uids)"
psql "$URL" -x -c "$SQL"

ALIVE=$(psql "$URL" -tAc "WITH ledger(uid) AS (VALUES $(sed "s/.*/('&'::uuid),/" "$LEDGER" | tr -d '\n' | sed 's/,$//'))
                          SELECT count(*) FROM ledger l JOIN $TABLE t USING (uid) WHERE t.deleted_at IS NULL")
echo
psql "$URL" -tAc "SELECT '  gc watermark: '||watermark::text||'   reaped so far: '||coalesce(reaped::text,'-') FROM zebridge_gc_watermark"
if [ "$ALIVE" != "0" ]; then
  echo "  ✗ $ALIVE uid(s) ALIVE — a delete was lost or a row resurrected"
  exit 1
fi
echo "  ✓ no delete was lost: every uid is tombstoned or reaped"
