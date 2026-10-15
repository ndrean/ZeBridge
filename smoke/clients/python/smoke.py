"""Smoke: the published zebridge wheel enrolls with an invite, seeds smoke_items, writes
one row and waits for PostgreSQL's verdict, then prints "ready" and waits for the row
run.sh inserts in PostgreSQL (it can only arrive by the live CDC tail). Exit 0 on both."""
import os, sys, threading, time
from datetime import datetime, timezone
from zebridge import ZeBridge

NAME = "python"
outcomes, got = [], threading.Event()


def on_change(report):
    for o in report.get("outcomes") or []:
        outcomes.append(o)
        got.set()


def fail(msg):
    print(f"✗ {NAME}: {msg}")
    sys.exit(1)


zb = ZeBridge(bridge_url=os.environ["ZB_BRIDGE_URL"], invite=os.environ["ZB_INVITE"],
              tables=["smoke_items"], db_path=f"/tmp/smoke-{NAME}.db", on_change=on_change)
deadline = time.monotonic() + 30
while not zb.query("SELECT 1 FROM smoke_items WHERE id = 'seed'"):
    if time.monotonic() > deadline:
        fail("the seed row never arrived")
    time.sleep(0.5)
now = datetime.now(timezone.utc).isoformat()
zb.mutate("smoke_items", "INSERT", {"id": f"from-{NAME}"},
          {"id": f"from-{NAME}", "tenant_id": zb.tenants[0], "note": f"written by {NAME}", "updated_at": now})
zb.flush(5000)
if not got.wait(30):
    fail("no verdict in 30 s")
if outcomes[0].get("outcome") != "applied":
    fail(f"verdict {outcomes[0]}")
print("ready", flush=True)
deadline = time.monotonic() + 30
while not zb.query("SELECT 1 FROM smoke_items WHERE id = ?", f"live-{NAME}"):
    if time.monotonic() > deadline:
        fail("PostgreSQL's live row never arrived (the CDC tail)")
    time.sleep(0.5)
zb.close()
print(f"✓ {NAME}: enrolled, seeded, write applied, live change received (tenant {zb.tenants[0]})")
