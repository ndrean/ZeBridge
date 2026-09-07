"""Every suspension reason, told the same way everywhere (NOTES §10dj's finding).

A refused table is announced in two places a client or an operator can read: the
registry's projection (`SELECT * FROM zebridge_suspensions`) and the descriptor in
the schemas bucket (`"suspended":true,"reason":…`). The boot-schema publisher used to
write `no_primary_key` for every refusal, whatever the registry said. This scenario
breaks one table per reason, first LIVE under a running bridge, then at BOOT under a
restarted one, and compares the two words each time; then it fixes each table and
watches the suspension lift. A Node client follows all seven and must hear each reason.

  no_primary_key                the pk constraint dropped
  no_cdc_subject                the catalogue row deleted
  no_tenant_column              the catalogue row names a column the table lacks
  tenant_not_in_replica_identity  REPLICA IDENTITY set back to DEFAULT
  unsupported_column_type       a column the decoder cannot pass through
  row_too_large                 a row past 2^BASE_BUF
  too_many_columns              more columns than MAX_COLUMNS
"""
import sys, time
import zb
from clients import Node, fresh_sqlite

LOG, LOG2 = "/tmp/zb_suspension_reasons_bridge.log", "/tmp/zb_suspension_reasons_bridge2.log"
PUB = zb.publication()
COMMON = ("tenant_id varchar(255) NOT NULL, last_writer varchar(255), inserted_at timestamptz NOT NULL DEFAULT now(), "
          "updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz")
ENABLE = ("tenant_col => 'tenant_id', writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at', "
          f"tiebreak_col => 'last_writer', generations => true, publication => '{PUB}', dry_run => false")
# table → (expected reason, the break, the fix)
CASES = {
    "zb_r_nopk":    ("no_primary_key", "ALTER TABLE zb_r_nopk DROP CONSTRAINT zb_r_nopk_pkey CASCADE", "ALTER TABLE zb_r_nopk ADD PRIMARY KEY (uid)"),
    "zb_r_nocdc":   ("no_cdc_subject", "DELETE FROM zebridge_catalogue WHERE tbl = 'zb_r_nocdc'", f"SELECT step FROM zebridge_enable('public.zb_r_nocdc', {ENABLE})"),
    "zb_r_notenant": ("no_tenant_column", "UPDATE zebridge_catalogue SET tenant_col = 'ghost_col' WHERE tbl = 'zb_r_notenant'", "UPDATE zebridge_catalogue SET tenant_col = 'tenant_id' WHERE tbl = 'zb_r_notenant'"),
    "zb_r_noident": ("tenant_not_in_replica_identity", "ALTER TABLE zb_r_noident REPLICA IDENTITY DEFAULT", f"SELECT step FROM zebridge_enable('public.zb_r_noident', {ENABLE})"),
    "zb_r_badtype": ("unsupported_column_type", "ALTER TABLE zb_r_badtype ADD COLUMN geo point; UPDATE zb_r_badtype SET geo = '(1,2)', updated_at = now()", "ALTER TABLE zb_r_badtype DROP COLUMN geo"),
    # session_replication_role = replica: the width guard zebridge_enable installed would
    # refuse the row in PostgreSQL itself, and the bridge would never see it
    "zb_r_toobig":  ("row_too_large", "SET session_replication_role = replica; UPDATE zb_r_toobig SET title = repeat('x', 6000), updated_at = now()", "UPDATE zb_r_toobig SET title = 'small', updated_at = now()"),
    "zb_r_toomany": ("too_many_columns", "ALTER TABLE zb_r_toomany " + ", ".join(f"ADD COLUMN c{i} integer" for i in range(24)), "ALTER TABLE zb_r_toomany " + ", ".join(f"DROP COLUMN c{i}" for i in range(24))),
}


def kv_reason(t):
    raw = zb.kv_get("schemas", t)
    return raw.split('"reason":"')[1].split('"')[0] if '"suspended":true' in raw else None


def pg_reason(t):
    return zb.psql(f"SELECT reason FROM zebridge_suspensions WHERE tbl = '{t}'").strip() or None


def wait(pred, budget=20):
    t0 = time.monotonic()
    while time.monotonic() - t0 < budget:
        if pred(): return round(time.monotonic() - t0, 1)
        time.sleep(0.5)
    return None


def teardown():
    for t in CASES:
        zb.psql(f"DROP TABLE IF EXISTS public.{t}", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_catalogue WHERE tbl = '{t}'", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_generations WHERE tbl = '{t}'", quiet=True)


def main():
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns the only bridge")
    failed = 0
    def check(label, cond):
        nonlocal failed
        label = time.strftime("%H:%M:%S ") + label
        if cond: zb.ok(label)
        else: zb.bad(label); failed += 1
        sys.stdout.flush()

    teardown()
    for t in CASES:
        zb.psql(f"CREATE TABLE public.{t} (uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), title text, {COMMON})")
    fresh_sqlite("/tmp/zb-reasons-node.sqlite3")
    nd = None
    # ⚠️ MAX_COLUMNS and BASE_BUF explicit on both bridges: the sizes the last two
    # reasons are measured against must not move with whatever else the database holds.
    ENV = dict(GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5", MAX_COLUMNS="16", BASE_BUF="12")
    try:
        with zb.Bridge(LOG, **ENV) as bridge:
            if not bridge.wait_for_log("Generation producer started", timeout=30):
                zb.bad("bridge never started its producer"); print(bridge.text()[-1500:]); return 1
            tenant = zb.tenant_of("omar")
            for t in CASES:
                out = zb.psql(f"SELECT string_agg(step || ':' || status, ' ') FROM zebridge_enable('public.{t}', {ENABLE})")
                if "error" in out.lower(): check(f"zebridge_enable({t}): {out[:80]}", False)
                zb.psql(f"INSERT INTO {t} (title, tenant_id) VALUES ('row', '{tenant}')")
            nd = Node("/tmp/zb-reasons-node.sqlite3", "/tmp/zb_suspension_reasons_node.log")
            dt = wait(lambda: all(kv_reason(t) is None and '"columns"' in zb.kv_get("schemas", t) for t in CASES), 30)
            check(f"§0 seven tables enabled, all seven descriptors published unsuspended ({dt} s)", dt is not None)

            # ── 1. break each one LIVE ──
            live = {}
            for t, (want, brk, _fix) in CASES.items():
                zb.psql(brk, quiet=True)
                dt = wait(lambda: kv_reason(t) is not None, 15)
                live[t] = (kv_reason(t), pg_reason(t), dt)
                if kv_reason(t) is None and pg_reason(t) is not None:
                    print(f"  · {t}: raw descriptor after the break: {zb.kv_get('schemas', t)[:140]}")
            for t, (want, _b, _f) in CASES.items():
                kvr, pgr, dt = live[t]
                if kvr is None and pgr is None:
                    check(f"§1 {t}: not suspended live (the check for '{want}' runs at boot)", True)
                else:
                    check(f"§1 {t}: live → descriptor '{kvr}', registry '{pgr}' ({dt} s); expected '{want}'", kvr == pgr == want)

        # ── 2. at BOOT: every reason, both words ──
        with zb.Bridge(LOG2, **ENV) as bridge2:
            if not bridge2.wait_for_log("Generation producer started", timeout=30):
                zb.bad("restarted bridge never started its producer"); print(bridge2.text()[-1500:]); return 1
            time.sleep(3)
            for t, (want, _b, _f) in CASES.items():
                kvr, pgr = kv_reason(t), pg_reason(t)
                check(f"§2 {t} at boot: descriptor '{kvr}', registry '{pgr}'; expected '{want}'", kvr == pgr == want)
            time.sleep(2)
            heard = open("/tmp/zb_suspension_reasons_node.log").read()
            missing = [want for t, (want, _b, _f) in CASES.items() if f"{t} suspended upstream ({want})" not in heard]
            check(f"§2 the Node client heard every reason by name" + (f"; missing: {missing}" if missing else ""), not missing)

            # ── 3. fix each one, live ──
            for t, (want, _b, fix) in CASES.items():
                zb.psql(fix, quiet=True)
                if want == "row_too_large":
                    # the probe has a 30 s cooldown: the lift comes with the first write that
                    # fits AFTER it — keep touching the row until then
                    t0 = time.monotonic(); dt = None
                    while time.monotonic() - t0 < 60:
                        zb.psql(f"UPDATE {t} SET updated_at = now()", quiet=True)
                        if wait(lambda: kv_reason(t) is None and pg_reason(t) is None, 5) is not None: dt = round(time.monotonic() - t0, 1); break
                else:
                    dt = wait(lambda: kv_reason(t) is None and pg_reason(t) is None, 30)
                check(f"§3 {t}: fixed → lifted live in both places ({dt} s)" if dt is not None else f"§3 {t}: fixed, but still '{kv_reason(t)}' / '{pg_reason(t)}' after the budget", dt is not None)
            nd.close(); nd = None
            teardown()
    finally:
        if nd: nd.close()
        teardown()
    return failed


if __name__ == "__main__":
    sys.exit(main() or 0)
