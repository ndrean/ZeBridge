#!/usr/bin/env python3
"""§10fn: a principal in SEVERAL tenants — the roster as a set, end to end.
    scripts/scenarios/run.py -k membership      (live: the running bridge, its /enroll)

The write side was a set already (`zb_tenant_write` is an IN over the roster); this
proves the rest moved with it:
  1. the JWT carries one `tenant:` tag per membership (a second invite for a known
     principal is a JOIN, and the mint reads the roster, not the invite);
  2. `$KV.tenants.<principal>` is the set, a JSON array, kept by the WAL;
  3. libzb follows every listed tenant: one chain per tenant into the same table,
     one CDC stream per tenant, rows of both present;
  4. a write to each member tenant is accepted, to a stranger refused (RLS), and a
     write that names no tenant is refused rather than guessed (the trigger);
  5. `leave` drops the tenant's rows, watermark and tail; its live changes no longer
     arrive; `join` brings it back, the rows written meanwhile included; a join the
     JWT does not allow is refused before the membership is taken;
  6. one roster row gone is a leave (the set shrinks, no ban); the LAST one gone is
     the revocation it always was (key purged, the client hangs up).
Throwaway principal, `pois` rows tombstoned at exit (never hard-deleted: a reap is
not forwarded, and the map app follows this table as alice).
"""
import base64, json, os, pathlib, re, subprocess, sys, time, urllib.request, uuid

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb
from clients import Lib

BRIDGE = zb.BRIDGE
TABLE = "pois"
TA, TB, STRANGER = "acme", "globex", "tango"
PRINCIPAL = "mm_" + os.urandom(3).hex()
TMP = pathlib.Path(os.environ.get("TMPDIR", "/tmp"))
DB = str(TMP / f"zb_membership_{PRINCIPAL}.sqlite3")


def sql(text: str) -> str:
    return zb.psql(text, quiet=True).strip()


def enrol(tenant: str, tag: str) -> tuple[pathlib.Path, list[str]]:
    """An invite, a fresh nkey, one GET /enroll → creds and the JWT's tenant tags."""
    code = os.urandom(16).hex()
    sql(f"INSERT INTO public.zebridge_invites (code, principal, tenant_id, expires_at) "
        f"VALUES ('{code}', '{PRINCIPAL}', '{tenant}', now() + interval '5 minutes')")
    gen = subprocess.run([str(BRIDGE), "--gen-nkey"], capture_output=True, text=True)
    user_pub = re.search(r"NATS_BRIDGE_NKEY_PUB=(U[A-Z0-9]+)", gen.stdout).group(1)
    user_seed = re.search(r"NATS_BRIDGE_NKEY_SEED=(SU[A-Z0-9]+)", gen.stdout).group(1)
    with urllib.request.urlopen(zb.http_base() + f"/enroll?code={code}&user_pubkey={user_pub}", timeout=10) as r:
        payload = json.loads(r.read().decode())
    jwt = payload["jwt"]
    claims = json.loads(base64.urlsafe_b64decode(jwt.split(".")[1] + "=="))
    tags = sorted(t.split(":", 1)[1] for t in claims.get("nats", {}).get("tags", []) if t.startswith("tenant:"))
    path = TMP / f"zb_{PRINCIPAL}_{tag}.creds"
    path.write_text("-----BEGIN NATS USER JWT-----\n" + jwt + "\n------END NATS USER JWT------\n\n"
                    "-----BEGIN USER NKEY SEED-----\n" + user_seed + "\n------END USER NKEY SEED------\n")
    return path, tags


def wait(pred, seconds=15.0, step=0.3):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(step)
    return pred()


def local_by_tenant(c: Lib) -> dict:
    return {r[0]: r[1] for r in c.q(f"SELECT tenant_id, count(*) FROM {TABLE} WHERE note LIKE 'mm-%' GROUP BY 1")}


def local_has(c: Lib, note: str) -> bool:
    return len(c.q(f"SELECT uid FROM {TABLE} WHERE note = ?", [note])) > 0


def master_insert(tenant: str, note: str) -> str:
    return sql(f"INSERT INTO public.{TABLE} (lat, lng, note, tenant_id) VALUES (47.2, -1.55, '{note}', '{tenant}') RETURNING uid").splitlines()[0]


def cleanup(client):
    if client is not None:
        try: client.close()
        except Exception: pass
    sql(f"UPDATE public.{TABLE} SET deleted_at = now() WHERE note LIKE 'mm-%' AND deleted_at IS NULL")
    sql(f"DELETE FROM public.zebridge_user_tenants WHERE principal = '{PRINCIPAL}'")
    sql(f"DELETE FROM public.zebridge_principal_keys WHERE principal = '{PRINCIPAL}'")
    sql(f"DELETE FROM public.zebridge_invites WHERE principal = '{PRINCIPAL}'")
    zb.nats_cli("kv", "purge", zb.kv_bucket("tenants"), PRINCIPAL, "-f")
    for p in (DB, DB + "-wal", DB + "-shm"):
        pathlib.Path(p).unlink(missing_ok=True)
    for p in TMP.glob(f"zb_{PRINCIPAL}_*.creds"):
        p.unlink(missing_ok=True)


def main() -> int:
    failed = 0
    client = None
    if sql(f"SELECT count(*) FROM public.zebridge_catalogue WHERE tbl = '{TABLE}'") != "1":
        sys.exit(f"{TABLE} is not enabled — this scenario follows the map example's table (examples/08-map)")
    try:
        # ── 1. two invites, one principal: the second is a join ─────────────────
        creds_a, tags_a = enrol(TA, "a")
        if tags_a == [TA] and zb.kv_tenants(PRINCIPAL) == [TA]:
            zb.ok(f"enrolled in {TA}: the JWT carries tenant:{TA}, $KV.tenants.{PRINCIPAL} = ['{TA}']")
        else:
            zb.bad(f"first enrol: tags {tags_a}, kv {zb.kv_tenants(PRINCIPAL)}"); failed += 1
        creds_b, tags_b = enrol(TB, "b")
        kv_ok = wait(lambda: zb.kv_tenants(PRINCIPAL) == [TA, TB])
        if tags_b == [TA, TB] and kv_ok:
            zb.ok(f"a second invite ({TB}) is a JOIN: the JWT carries BOTH tags (minted from the roster, "
                  f"not the invite), and the KV value is the set {zb.kv_tenants(PRINCIPAL)}")
        else:
            zb.bad(f"second enrol: tags {tags_b}, kv {zb.kv_tenants(PRINCIPAL)}"); failed += 1

        # ── 2. rows in both tenants, seeded into ONE local table ────────────────
        master_insert(TA, "mm-seed-a"); master_insert(TB, "mm-seed-b")
        time.sleep(1.0)
        client = c = Lib(DB, [TABLE], client_id="py-membership", principal=PRINCIPAL, creds=creds_b)
        if c.tenants == [TA, TB]:
            zb.ok(f"libzb resolved the set: tenants = {c.tenants} (tenant = '{c.tenant}', the first)")
        else:
            zb.bad(f"libzb tenants = {c.tenants!r}"); failed += 1
        both = wait(lambda: (c.poll(300), local_has(c, "mm-seed-a") and local_has(c, "mm-seed-b"))[1], 30)
        gens = {r[0] for r in c.q("SELECT tenant FROM _zbz_generations WHERE tbl = ?", [TABLE])}
        streams = {r[0] for r in c.q("SELECT stream FROM _zbz_stream_seq")}
        if both and gens == {TA, TB} and {"CDC_" + TA, "CDC_" + TB} <= streams:
            zb.ok(f"both tenants' rows are in {TABLE}: one chain per tenant (watermarks {sorted(gens)}), "
                  f"one CDC stream per tenant ({sorted(s for s in streams if s.startswith('CDC_'))})")
        else:
            zb.bad(f"seed: both={both}, watermarks={gens}, streams={streams}"); failed += 1

        # ── 3. writes: each member accepted, a stranger refused, no tenant refused ──
        def write(tenant, note, with_tenant=True):
            uid = str(uuid.uuid4())
            vals = {"uid": uid, "lat": 47.2, "lng": -1.55, "note": note, "inserted_at": "2026-09-13T12:00:00Z", "updated_at": "2026-09-13T12:00:00Z"}
            if with_tenant:
                vals["tenant_id"] = tenant
            r = c.mutate(TABLE, "INSERT", {"uid": uid}, vals)
            if "error" in r:
                return {"local_refused": r["error"]}
            before = c.flush(0)["verdicts"]
            for _ in range(20):
                v = c.flush(500)["verdicts"]
                if sum(v.values()) > sum(before.values()):
                    return {k: v[k] - before[k] for k in v}
            return {}
        ok_a = write(TA, "mm-write-a").get("accepted") == 1
        ok_b = write(TB, "mm-write-b").get("accepted") == 1
        if ok_a and ok_b:
            zb.ok(f"a write to {TA} and a write to {TB} are both accepted — the row names its tenant, the policy checks the set")
        else:
            zb.bad(f"member writes: {TA} ok={ok_a}, {TB} ok={ok_b}"); failed += 1
        stranger = write(STRANGER, "mm-write-stranger")
        if stranger.get("rejected") == 1:
            zb.ok(f"a write to {STRANGER} (not a member) is REJECTED by row-level security, as before")
        else:
            zb.bad(f"stranger write verdicts: {stranger}"); failed += 1
        no_tenant = write(TA, "mm-write-none", with_tenant=False)
        if no_tenant.get("rejected") == 1 or no_tenant.get("failed") == 1 or "local_refused" in no_tenant:
            zb.ok(f"a write naming NO tenant is refused, not guessed ({no_tenant}) — a member of several must say which")
        else:
            zb.bad(f"no-tenant write: {no_tenant}"); failed += 1
        for _ in range(5): c.poll(200)

        # ── 4. leave: rows, watermark, tail gone; its changes no longer arrive ───
        r = c.leave(TB)
        gone = local_by_tenant(c).get(TB, 0) == 0
        gens = {x[0] for x in c.q("SELECT tenant FROM _zbz_generations WHERE tbl = ?", [TABLE])}
        streams = {x[0] for x in c.q("SELECT stream FROM _zbz_stream_seq")}
        if r.get("tenants") == [TA] and gone and gens == {TA} and ("CDC_" + TB) not in streams and local_has(c, "mm-seed-a"):
            zb.ok(f"leave({TB}): its rows left the table, its watermark and position are forgotten; {TA}'s rows stay")
        else:
            zb.bad(f"leave: {r}, {TB} rows left={not gone}, watermarks={gens}, streams={streams}"); failed += 1
        master_insert(TB, "mm-while-away"); master_insert(TA, "mm-still-here")
        arrived_a = wait(lambda: (c.poll(300), local_has(c, "mm-still-here"))[1], 15)
        for _ in range(6): c.poll(300)
        if arrived_a and not local_has(c, "mm-while-away"):
            zb.ok(f"after the leave, {TA}'s live change arrives and {TB}'s does not")
        else:
            zb.bad(f"after leave: {TA} arrived={arrived_a}, {TB} arrived={local_has(c, 'mm-while-away')}"); failed += 1

        # ── 5. join: seeded afresh, the row written meanwhile included ──────────
        r = c.join(TB)
        back = wait(lambda: (c.poll(300), local_has(c, "mm-while-away") and local_has(c, "mm-seed-b"))[1], 30)
        gens = {x[0] for x in c.q("SELECT tenant FROM _zbz_generations WHERE tbl = ?", [TABLE])}
        if r.get("tenants") == [TA, TB] and back and gens == {TA, TB}:
            zb.ok(f"join({TB}): seeded again from its chain, the row written while away included; watermarks {sorted(gens)}")
        else:
            zb.bad(f"join: {r}, back={back}, watermarks={gens}"); failed += 1

        # ── 5b. a join outside the JWT is refused, and poisons nothing (§10fo) ───
        r = c.join(STRANGER)
        master_insert(TA, "mm-after-refusal")
        still = wait(lambda: (c.poll(300), local_has(c, "mm-after-refusal"))[1], 15)
        if r.get("error") == "JoinRefused" and c.tenants == [TA, TB] and still:
            zb.ok(f"join({STRANGER}), a tenant the JWT does not carry: JoinRefused, the membership is untouched, "
                  f"and the permitted tenants keep tailing")
        else:
            zb.bad(f"join outside the JWT: {r}, tenants={c.tenants}, tail alive={still}"); failed += 1

        # ── 6. the roster shrinks: a leave, not a ban; the last row gone: the ban ──
        sql(f"DELETE FROM public.zebridge_user_tenants WHERE principal = '{PRINCIPAL}' AND tenant_id = '{TB}'")
        shrunk = wait(lambda: zb.kv_tenants(PRINCIPAL) == [TA])
        alive = "error" not in c.poll(500)
        if shrunk and alive:
            zb.ok(f"one roster row deleted: $KV.tenants.{PRINCIPAL} = ['{TA}'], no ban — the client polls on")
        else:
            zb.bad(f"roster shrink: kv={zb.kv_tenants(PRINCIPAL)}, alive={alive}"); failed += 1
        sql(f"DELETE FROM public.zebridge_user_tenants WHERE principal = '{PRINCIPAL}'")
        purged = wait(lambda: zb.kv_get("tenants", PRINCIPAL) == "")
        banned = wait(lambda: c.poll(500).get("error") == "Revoked", 20)
        if purged and banned:
            zb.ok("the LAST roster row deleted: the key is purged and the ban lands — the client hangs up (Revoked)")
        else:
            zb.bad(f"revocation: purged={purged}, banned={banned}"); failed += 1
    finally:
        cleanup(client)
    return failed


if __name__ == "__main__":
    sys.exit(main())
