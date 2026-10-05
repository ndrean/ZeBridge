"""Enroll the Worker's principal once, with libzb, and write its creds where wrangler reads
local secrets (.dev.vars, git-ignored): base64 on one line. Prints the principal and sizes,
never a secret.

    PYTHONPATH=../../zb-python/src python3 enroll.py --bridge https://bridge.example.com \\
        --invite <code> --ws wss://ws.example.com

For a deployed Worker, put the same value in a Worker secret instead:
    wrangler secret put ZB_CREDS_B64
"""
import argparse, base64, json, os
from zebridge import ZeBridge

ap = argparse.ArgumentParser()
ap.add_argument("--bridge", required=True)
ap.add_argument("--invite", required=True)
ap.add_argument("--ws", required=True, help="the NATS WebSocket the Worker dials")
a = ap.parse_args()

here = os.path.dirname(os.path.abspath(__file__))
ident = os.path.join(here, "edge-worker.identity")
with ZeBridge(bridge_url=a.bridge, invite=a.invite, identity_path=ident,
              db_path=os.path.join(here, "edge-worker.sqlite3"), tables=[], heartbeat_ms=0):
    pass

id_ = json.load(open(ident))
path = os.path.join(here, ".dev" + ".vars")
with open(path, "w") as f:
    f.write("ZB_CREDS_B64=" + base64.b64encode(id_["creds"].encode()).decode() + "\n")
    f.write("ZB_PRINCIPAL=" + id_["principal"] + "\n")
    f.write("ZB_NATS_WS_URL=" + a.ws + "\n")
os.chmod(path, 0o600)
print(f"enrolled {id_['principal']}: creds in .dev.vars ({len(id_['creds'])} bytes)")
