#!/usr/bin/env python3
"""Both client decisions together: a withheld descriptor refuses a NEW edit, while a stamped
edit already queued stays eligible for the bridge to settle.

Per client (libzb through a relay that cuts its link, zb-client-ts hung up), on a probe bridge
with stamping on (`write_policy.incarnation_stamps`):

  1. the client seeds the table at incarnation n and goes offline;
  2. it queues edit 1 of row K, stamped n;
  3. the policy is made unreadable, the table is dropped and re-created with a row K older
     than the edit (a restore): the descriptor turns withheld (`incarnation: null`,
     `writable: false`); the bridge then stops, so nothing settles yet;
  4. the client comes back: it receives the withheld descriptor and sends edit 1, which waits
     in the admission queue;
  5. edit 2 of row K is refused: the row and the outbox are untouched;
  6. the client restarts: edit 1 is still queued, with its original ID and stamp;
  7. the policy is readable again and the bridge restarts: edit 1 gets exactly one verdict,
     rejected / TableIncarnationChanged, the replacement row is intact, the outbox drains and
     the replica equals PostgreSQL.

Exit status (it starts in `known_red`):
  1  the known defect, on both clients: edit 1 was queued with no stamp, and edit 2 was
     queued although the descriptor withheld writes;
  0  the contract above, on both clients;
  2  setup incomplete (the descriptor never turned withheld, a client never seeded, edit 1
     was not queued offline, the replacement is not older than the edit);
  3  anything else, a partial fix included; each client's result is printed.
"""
import json, sys, time, uuid
import zb
from clients import Lib, Node, fresh_sqlite
from recreate_incarnation import (Relay, Side, until, create, teardown, incarnation, pg_title, receipt,
                                  created_ms, T, REPLACEMENT, RESTORED_AT, RESTORED_MS, NODE_LOG, OBSERVE_S)

LOG = "/tmp/zb_incarnation_clients_bridge.log"
STAMPS = "public.zebridge_incarnation_stamps()"


def policy(on):
    zb.psql(f"UPDATE zebridge_private.write_policy SET incarnation_stamps = {'true' if on else 'false'} WHERE singleton")


REVOKED = []  # roles whose EXECUTE was taken: given back in main's finally, whatever failed


def make_policy_unreadable():
    """Revoke EXECUTE on the switch's reader from every non-superuser that has it; returns them."""
    roles = zb.psql(f"SELECT string_agg(quote_ident(rolname), ',') FROM pg_roles WHERE NOT rolsuper "
                    f"AND has_function_privilege(oid, '{STAMPS}', 'EXECUTE')") or ""
    if roles:
        REVOKED.append(roles)
        zb.psql(f"REVOKE EXECUTE ON FUNCTION {STAMPS} FROM {roles}")
    return roles


def restore_policy_grants():
    while REVOKED:
        zb.psql(f"GRANT EXECUTE ON FUNCTION {STAMPS} TO {REVOKED.pop()}", quiet=True)


def descriptor():
    raw = zb.kv_get("schemas", T)
    try: return json.loads(raw) if raw else None
    except ValueError: return None


def withheld(d):
    return isinstance(d, dict) and "incarnation" in d and d["incarnation"] is None and d.get("writable") is False


def stored(side, mid):
    """(present, stamp) of the outbox entry `mid`: its stored payload's `incarnation`."""
    rows = side.c.q("SELECT payload FROM _zebridge_outbox WHERE msg_id = ?", (mid,))
    if not rows: return False, None
    try: return True, json.loads(rows[0][0]).get("incarnation")
    except (ValueError, AttributeError): return True, None


def run_client(kind, relay, bridge_env):
    """One client through steps 1–7. Returns the facts the classifier reads."""
    f = {}
    side = Side(kind, relay)
    try:
        if not until(side.seeded, 120, 0.3): return {"setup": f"{T} never seeded"}
        n = incarnation()[0]
        uid = str(uuid.uuid4())
        zb.psql(f"INSERT INTO public.{T} (uid, title, tenant_id) VALUES ('{uid}', 'original', '{side.tenant}')")
        if not until(lambda: (side.poll(), side.local_title(uid))[1] == "original", 60): return {"setup": "row K never reached the replica"}
        side.offline()
        edit1 = f"edit 1 by {kind}"
        mid = side.update(uid, edit1)
        present, stamp = stored(side, mid)
        if not present: return {"setup": "edit 1 was not queued offline"}
        f.update(n=n, mid=mid, stamp=stamp, edit1=edit1)
        roles = make_policy_unreadable()
        zb.psql(f"DROP TABLE public.{T}")
        create()
        zb.psql(f"INSERT INTO public.{T} (uid, title, tenant_id, updated_at) VALUES ('{uid}', '{REPLACEMENT}', '{side.tenant}', '{RESTORED_AT}')")
        if not until(lambda: withheld(descriptor()), 60, 0.5): return {"setup": f"the descriptor never turned withheld: {descriptor()}"}
        # The new incarnation's chain first: a client that comes back seeds from it, and with
        # the bridge stopped no producer would build it (zb-client-ts's connect waits for one).
        new_inc = incarnation()[0]
        def chain_ready():
            raw = zb.kv_get("generations", f"{side.tenant}.{T}")
            try: return json.loads(raw).get("incarnation") == new_inc
            except (ValueError, AttributeError): return False
        if not until(chain_ready, 90, 0.5): return {"setup": f"no chain of incarnation {new_inc} for {side.tenant}"}
        bridge_env["stop"]()                     # nothing settles until step 7
        side.online()
        # The client has the withheld descriptor once its replica says the table moved on, or
        # after its reconnect has had time to read the schemas.
        end = time.monotonic() + 15
        while time.monotonic() < end: side.poll(); time.sleep(0.3)
        before_title = side.local_title(uid)
        before_outbox = side.c.q("SELECT count(*) FROM _zebridge_outbox")[0][0]
        try:
            r = side.update(uid, f"edit 2 by {kind}")
            f["edit2"] = ("queued", r)
        except Exception as e:  # the TS worker raises; libzb answers {"error": …}, which update() turns into None
            f["edit2"] = ("refused", str(e)[:120])
        if f["edit2"][0] == "queued" and not f["edit2"][1]: f["edit2"] = ("refused", "no ID returned")
        f["row_untouched"] = side.local_title(uid) == before_title
        f["outbox_after_edit2"] = side.c.q("SELECT count(*) FROM _zebridge_outbox")[0][0]
        f["outbox_before_edit2"] = before_outbox
        # Step 6: a restart.
        side.restart()
        present, stamp_after = stored(side, mid)
        f.update(kept_after_restart=present, stamp_after_restart=stamp_after)
        # Step 7: the policy readable, the bridge back.
        restore_policy_grants()
        bridge_env["start"]()
        until(lambda: (side.poll(), side.queued(mid) == 0)[1], 90, 0.3)
        end = time.monotonic() + OBSERVE_S
        while time.monotonic() < end: side.poll(); time.sleep(0.3)
        f.update(receipt=receipt(mid), outcomes=side.outcomes(uid, mid), pg=pg_title(uid),
                 drained=side.drained(), converged=until(side.matches_pg, 60, 0.3),
                 edit_newer=(created_ms(mid) or 0) > RESTORED_MS)
        print(f"  {kind}: {json.dumps(f, default=str)}", flush=True)
        return f
    finally:
        side.close()


def classify(results):
    for kind in ("libzb", "ts"):
        f = results.get(kind)
        if f is None: print(f"  {kind}: did not run"); return 2
        if "setup" in f: print(f"  {kind}: setup incomplete — {f['setup']}"); return 2
        if not f["edit_newer"]: print(f"  {kind}: setup incomplete — the replacement is not older than edit 1"); return 2

    def contract(f):
        return (f["stamp"] == f["n"] and f["edit2"][0] == "refused" and f["row_untouched"]
                and f["outbox_after_edit2"] == f["outbox_before_edit2"]
                and f["kept_after_restart"] and f["stamp_after_restart"] == f["n"]
                and f["receipt"] == ("rejected", "TableIncarnationChanged")
                and f["outcomes"] == [("rejected", "TableIncarnationChanged")]
                and f["pg"] == REPLACEMENT and f["drained"] and f["converged"])

    def known(f):
        return f["stamp"] is None and f["edit2"][0] == "queued" and f["outbox_after_edit2"] == f["outbox_before_edit2"] + 1

    state = {k: "contract" if contract(f) else "known defect" if known(f) else "unmatched" for k, f in results.items()}
    for k in ("libzb", "ts"): print(f"  {k}: {state[k]}", flush=True)
    if all(s == "contract" for s in state.values()): return 0
    if all(s == "known defect" for s in state.values()):
        print("known defect reproduced: the clients do not stamp their writes and queue new edits under a withheld descriptor")
        return 1
    return 3


def main():
    teardown()
    relay = None
    results = {}
    bridge = {"b": None}
    def start():
        bridge["b"] = zb.Bridge(LOG, GENERATIONS_ENABLED="1", GENERATION_CADENCE_SECONDS="5").__enter__()
        bridge["b"].wait_for_log("Generation producer started", timeout=30)
    def stop():
        if bridge["b"]: bridge["b"].__exit__(None, None, None); bridge["b"] = None
    try:
        create()
        policy(True)
        relay = Relay()
        start()
        for kind in ("libzb", "ts"):
            if bridge["b"] is None: start()
            results[kind] = run_client(kind, relay, {"start": start, "stop": stop})
            zb.psql(f"DROP TABLE IF EXISTS public.{T}"); create()
    finally:
        stop()
        if relay: relay.close()
        restore_policy_grants()
        policy(False)
        teardown()
    return classify(results)


if __name__ == "__main__":
    # An exception is not a verdict: Python's own exit 1 would read as "known defect
    # reproduced" in known_red. It is an unexpected result (3), with its traceback.
    try:
        sys.exit(main())
    except Exception:
        import traceback; traceback.print_exc(); sys.exit(3)
