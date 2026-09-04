"""The permutation matrix: PostgreSQL and NATS failing and returning in every order,
under one live bridge and one live client (NOTES §10cc).

    scripts/scenarios/run.py owns -k matrix      (owns the bridge, the broker AND the cluster)

Every single-component outage has its own scenario; what none of them test is the
ORDERINGS — a double outage and the two possible restore orders, where the bridge is
simultaneously parking its publisher (NATS gone) and retrying its WAL stream (PG gone),
and recovery exercises whichever half wakes first:

  1. NATS↓  PG↓   →  PG↑   NATS↑     (recover the source first)
  2. NATS↓  PG↓   →  NATS↑ PG↑      (recover the sink first)
  3. PG↓   NATS↓  →  PG↑   NATS↑
  4. PG↓   NATS↓  →  NATS↑ PG↑
  5. both down, the bridge is KILLED mid-double-outage; both restored; a fresh boot
     resumes from the slot and converges — the operator's 3 a.m. worst case

Per case: a marker row proves flow, writes are attempted through the outage (those
PostgreSQL acknowledges are the only truth), both components are restored in the
case's order, and the client must converge to EXACTLY PostgreSQL's count — no loss of
anything committed, no resurrection of anything refused. The bridge must survive
cases 1–4 in one process; case 5 must come back from a corpse.
"""
import asyncio
import ctypes
import json
import os
import pathlib
import socket
import subprocess
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import zb  # noqa: E402
from chaos import running_nats, nats_conf_and_log  # noqa: E402
sys.path.insert(0, str(zb.ROOT / "libzb" / "python"))
import _env  # noqa: E402

TABLE = "test_types"
TENANT = "acme"
PRINCIPAL = "alice"
PG_CTL = "/opt/homebrew/opt/postgresql@18/bin/pg_ctl"
DATADIR = str(zb.ROOT / "postgres-data")
PG_OPTS = ("-p 5432 -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10 "
           "-c wal_sender_timeout=300s -c logical_decoding_work_mem=256MB -c wal_buffers=64MB "
           "-c commit_delay=1000 -c commit_siblings=5")
HOLD_S = 4
LOG = pathlib.Path(os.environ.get("TMPDIR", "/tmp")) / "zb_matrix_bridge.log"


def pg_ctl(*args):
    return subprocess.run([PG_CTL, "-D", DATADIR, *args, "-t", "90"],
                          capture_output=True, text=True, timeout=120)


def pg_up() -> bool:
    return subprocess.run([PG_CTL, "-D", DATADIR, "status"], capture_output=True).returncode == 0


def pg_start():
    return pg_ctl("start", "-o", PG_OPTS, "-l", str(zb.ROOT / "scripts" / "native" / "postgres.log"))


def ensure_pg_up() -> None:
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if pg_up():
            return
        pg_start()
        time.sleep(2)
    print("  ⚠️  PostgreSQL still down after 120s — scripts/native/up.sh needed")


def nats_up() -> bool:
    try:
        socket.create_connection(("127.0.0.1", 4222), timeout=1).close()
        return True
    except OSError:
        return False


class Nats:
    """Stop/start the running nats-server with its own argv, like nats_outage does."""

    def __init__(self):
        self.pid, self.argv = running_nats()
        _, self.log = nats_conf_and_log(self.argv)

    def stop(self):
        subprocess.run(["kill", self.pid], check=True)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if subprocess.run(["kill", "-0", self.pid], capture_output=True).returncode != 0:
                return
            time.sleep(0.3)
        raise RuntimeError("nats-server did not die")

    def start(self):
        with open(self.log, "a") as lf:
            subprocess.Popen(self.argv, stdout=lf, stderr=subprocess.STDOUT,
                             cwd=zb.ROOT, start_new_session=True)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if nats_up():
                self.pid, self.argv = running_nats()
                return
            time.sleep(0.3)
        raise RuntimeError("nats-server did not come back")


def ensure_nats_up(n: "Nats") -> None:
    if not nats_up():
        try:
            n.start()
        except Exception as e:  # noqa: BLE001
            print(f"  ⚠️  NATS still down: {e} — scripts/native/up.sh needed")


def slot_name() -> str:
    return zb.BRIDGE_ARGS[zb.BRIDGE_ARGS.index("--slot") + 1] if "--slot" in zb.BRIDGE_ARGS else "zb_probe"


async def main() -> int:
    markers: list[str] = []
    n = Nats()
    try:
        return await run(markers, n)
    finally:
        ensure_pg_up()
        ensure_nats_up(n)
        for m in markers:
            zb.psql(f"UPDATE public.{TABLE} SET deleted_at = now(), updated_at = now() "
                    f"WHERE some_text LIKE 'mx {m}%' AND deleted_at IS NULL", quiet=True)
        zb.psql(f"SELECT pg_drop_replication_slot('{slot_name()}') FROM pg_replication_slots "
                f"WHERE slot_name = '{slot_name()}' AND NOT active", quiet=True)
        zb.psql(f"DELETE FROM public.zebridge_limits WHERE slot = '{slot_name()}'", quiet=True)


def write_row(marker: str, tag: str) -> bool:
    out = zb.psql(f"INSERT INTO public.{TABLE} (uid, some_text, tenant_id, inserted_at, updated_at) "
                  f"VALUES (gen_random_uuid(), 'mx {marker} {tag}', '{TENANT}', now(), now()) RETURNING 1",
                  quiet=True)
    return bool(out.splitlines()) and out.splitlines()[0] == "1"


async def run(markers: list[str], n: Nats) -> int:
    if zb.another_bridge_running():
        sys.exit("another bridge is already running — this scenario owns everything")
    failed = 0

    lib = _env.load_lib()
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_open.restype, lib.zb_client_open.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    for fn, a in (("sync", []), ("poll", [ctypes.c_uint64]), ("query", [ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, "zb_client_" + fn); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + a

    def take(p):
        try: return json.loads(ctypes.string_at(p).decode())
        finally: lib.zb_free(p)

    db = f"/tmp/zb-matrix-{os.getpid()}.sqlite3"
    _env.rm_sqlite(db)
    h = 0

    def client_count(marker: str) -> int:
        take(lib.zb_client_poll(h, 400))
        r = take(lib.zb_client_query(h, f"SELECT count(*) FROM {TABLE} WHERE some_text LIKE ?".encode(),
                                     json.dumps([f"mx {marker}%"]).encode()))
        return r["rows"][0][0] if r.get("rows") else -1

    async def converge(marker: str, deadline_s: float = 60) -> tuple[int, int]:
        truth = int(zb.psql(f"SELECT count(*) FROM public.{TABLE} "
                            f"WHERE some_text LIKE 'mx {marker}%' AND deleted_at IS NULL"))
        deadline = time.monotonic() + deadline_s
        got = -1
        while time.monotonic() < deadline:
            got = client_count(marker)
            if got == truth:
                break
            await asyncio.sleep(0.5)
        return got, truth

    CASES = [
        ("1: NATS↓ PG↓ → PG↑ NATS↑", ["nats_down", "pg_down", "pg_up", "nats_up"]),
        ("2: NATS↓ PG↓ → NATS↑ PG↑", ["nats_down", "pg_down", "nats_up", "pg_up"]),
        ("3: PG↓ NATS↓ → PG↑ NATS↑", ["pg_down", "nats_down", "pg_up", "nats_up"]),
        ("4: PG↓ NATS↓ → NATS↑ PG↑", ["pg_down", "nats_down", "nats_up", "pg_up"]),
    ]

    try:
        with zb.Bridge(LOG) as br:
            if not br.wait_for_log("Replication started successfully", timeout=60):
                zb.bad("probe bridge did not start"); return 1
            h = lib.zb_client_open(json.dumps({
                "url": zb.nats_server(), "credsPath": zb.creds_for(PRINCIPAL),
                "grammarPath": str(zb.ROOT / "src" / "grammar.json"), "dbPath": db,
                "principal": PRINCIPAL, "clientId": "py-matrix", "tables": [TABLE]}).encode())
            if not h:
                zb.bad("libzb client could not open"); return 1
            take(lib.zb_client_sync(h))

            for name, steps in CASES:
                marker = os.urandom(3).hex()
                markers.append(marker)
                write_row(marker, "pre")
                got, truth = await converge(marker, 30)
                if got != truth:
                    zb.bad(f"case {name}: pipe not flowing before the case ({got}/{truth})")
                    failed += 1
                    continue
                for step in steps:
                    if step == "nats_down":
                        n.stop()
                    elif step == "nats_up":
                        n.start()
                    elif step == "pg_down":
                        # ⚠️ `-m fast`, and its completing IS an assertion. A fast stop
                        # waits for the logical client's flush confirmation; in the
                        # NATS-down orderings the bridge honestly cannot give one, and
                        # the first matrix run deadlocked here until aborted (§10cc).
                        # The STEP-ASIDE (§10cd) resolves it: the monitor sees "the
                        # database system is shutting down", the bridge drops its COPY
                        # connection, the walsender exits. The -t 90 budget therefore
                        # bounds monitor-cadence (30 s) + shutdown work — a hang past it
                        # means the step-aside regressed.
                        r = pg_ctl("stop", "-m", "fast")
                        if r.returncode != 0:
                            zb.bad(f"case {name}: FAST stop did not complete — the step-aside "
                                   f"(§10cd) regressed: {r.stderr.strip()[:100]}")
                            failed += 1
                            break
                    elif step == "pg_up":
                        pg_start()
                        deadline = time.monotonic() + 30
                        while time.monotonic() < deadline and not pg_up():
                            await asyncio.sleep(0.5)
                    # a write attempt at every stage: only PostgreSQL's acks count
                    write_row(marker, f"during-{step}")
                    await asyncio.sleep(HOLD_S)
                if br.proc.poll() is not None:
                    zb.bad(f"case {name}: the bridge DIED (a double outage is still just an outage)")
                    return failed + 1
                write_row(marker, "post")
                got, truth = await converge(marker, 90)
                if got == truth and truth >= 2:
                    zb.ok(f"case {name}: bridge survived; client == PostgreSQL == {truth} "
                          "(every acknowledged write delivered, nothing resurrected)")
                else:
                    zb.bad(f"case {name}: diverged — client {got}, PostgreSQL {truth}")
                    failed += 1

            # ── case 5: both down, the bridge is a corpse ────────────────────
            marker = os.urandom(3).hex()
            markers.append(marker)
            write_row(marker, "pre")
            got, truth = await converge(marker, 30)
            n.stop()
            write_row(marker, "nats-gone")     # commits; WAL retained for the slot
            r = pg_ctl("stop", "-m", "immediate")
            await asyncio.sleep(1)
            br.proc.kill()
            zb.ok("case 5: double outage and the bridge killed on top — the 3 a.m. shape")

        pg_start()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and not pg_up():
            await asyncio.sleep(0.5)
        n.start()
        with zb.Bridge(LOG.with_suffix(".2.log")) as br:
            if not br.wait_for_log("Replication started successfully", timeout=90):
                zb.bad("case 5: the fresh bridge did not boot after full restore"); return failed + 1
            got, truth = await converge(marker, 90)
            if got == truth and truth >= 2:
                zb.ok(f"case 5: fresh boot resumed from the slot — client == PostgreSQL == {truth}, "
                      "the write committed during the NATS half of the outage included")
            else:
                zb.bad(f"case 5: diverged — client {got}, PostgreSQL {truth}")
                failed += 1
    finally:
        if h:
            lib.zb_client_close(h)
        _env.rm_sqlite(db)

    print("PASS" if not failed else f"FAIL ({failed})")
    return failed


if __name__ == "__main__":
    zb.run(main)
