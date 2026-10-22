#!/usr/bin/env python3
"""One field client: libzb through its C ABI, on a machine near a leaf, against the real hub.

Run by field_follow.py over ssh, one process per client. The first connect redeems an invite
(`bridgeUrl` + `invite`); the identity is kept beside the replica and reused on every leg.
Each leg: read the catch-up target (CDC_<tenant>'s last sequence, with the client's own
credentials), connect with ZB_MEASURE on, poll until the target is reached, follow until the
leg ends. JSON events on stdout, libzb's log on stderr. After the last leg it waits for the
orchestrator's done file (PostgreSQL's digest) and polls until the replica equals it.

Needs: Python 3.10+, nats-py (for the target), the libzb .so (--lib).
"""
import argparse, asyncio, ctypes, hashlib, json, os, pathlib, sqlite3, sys, tempfile, threading, time

TABLE = "storm_items"


def frame(i, note) -> bytes:
    """gen_follow.frame: `id:N` for a NULL note, `id:S<bytes>:<note>` otherwise."""
    return f"{i}:N".encode() if note is None else f"{i}:S{len(note.encode())}:".encode() + note.encode()


def replica_digest(db, tenant):
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5)
        try:
            h, n = hashlib.sha256(), 0
            for i, note in con.execute(f"SELECT id, note FROM {TABLE} WHERE tenant_id = ? ORDER BY id", (tenant,)):
                h.update(frame(i, note))
                n += 1
            return {"digest": h.hexdigest(), "rows": n}
        finally:
            con.close()
    except sqlite3.Error as e:
        return {"error": str(e)}


async def stream_last(url, creds_text, domain, stream, principal):
    import nats
    with tempfile.NamedTemporaryFile("w", suffix=".creds", delete=False) as f:
        f.write(creds_text)
        path = f.name
    os.chmod(path, 0o600)
    try:
        # A client may only subscribe under `_INBOX.<principal>.>` (its JWT's grant, as
        # libzb's own inbox prefix): nats-py's default `_INBOX.<id>` is refused.
        nc = await nats.connect(url, user_credentials=path, connect_timeout=10, inbox_prefix=f"_INBOX.{principal}")
        try:
            info = await nc.jetstream(domain=domain).stream_info(stream)
            return info.state.last_seq
        finally:
            await nc.close()
    finally:
        os.unlink(path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", help="one client (measured: ZB_MEASURE is per process)")
    ap.add_argument("--batch", help="a JSON file [{name, schedule}, …]: one thread per client, unmeasured; "
                                    "--db/--identity/--invite-file then hold {name}")
    ap.add_argument("--lib", required=True)
    ap.add_argument("--db", required=True)
    ap.add_argument("--identity", required=True, help="kept across runs: the invite is redeemed once")
    ap.add_argument("--bridge", default="https://bridge.zebridge.eu")
    ap.add_argument("--invite-file", required=True, help="the invite code, used on the first connect only")
    ap.add_argument("--nats", required=True, help="the NATS this client dials: the leaf or the hub")
    ap.add_argument("--hub-nats", required=True, help="the hub, for the catch-up target")
    ap.add_argument("--seed-domain", default="")
    ap.add_argument("--tenant", default="storm")
    ap.add_argument("--schedule", help='[[open_s, close_s|null], ...] from --t0')
    ap.add_argument("--enroll-only", action="store_true",
                    help="with --batch: redeem the clients' invites (backing off when refused), keep the identities, connect nothing else")
    ap.add_argument("--enroll-parallel", type=int, default=8, help="with --enroll-only: how many redeem at once")
    ap.add_argument("--t0", type=float, required=True)
    ap.add_argument("--done-file", required=True)
    a = ap.parse_args()

    lib = ctypes.CDLL(a.lib)
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_client_connect.restype, lib.zb_client_connect.argtypes = ctypes.c_uint64, [ctypes.c_char_p]
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    lib.zb_last_error.restype = ctypes.c_char_p
    for n, extra in (("sync", []), ("poll", [ctypes.c_uint64])):
        f = getattr(lib, "zb_client_" + n); f.restype = ctypes.c_void_p; f.argtypes = [ctypes.c_uint64] + extra

    def take(p):
        try:
            return json.loads(ctypes.string_at(p).decode())
        finally:
            lib.zb_free(p)

    lock = threading.Lock()

    if a.batch and a.enroll_only:
        # Like a fleet: up to --enroll-parallel clients redeem at once, each backing off on
        # its own (exponential, jittered) when refused — at the bridge's real limit, not a
        # queue. Every refusal's reason (zb_last_error) is kept: the bridge's 503 "busy" and a
        # proxy's refusal look the same from the outcome alone.
        import random
        clients = json.loads(pathlib.Path(a.batch).read_text())
        gate = threading.Semaphore(a.enroll_parallel)
        out = []
        t_all = time.time()

        def enroll(c):
            ident = pathlib.Path(a.identity.format(name=c["name"]))
            if ident.exists():
                out.append({"client": c["name"], "enroll": "kept"})
                return
            opts = {"dbPath": a.db.format(name=c["name"]), "identityPath": str(ident), "tables": [TABLE], "heartbeatMs": 0,
                    "natsUrl": a.nats, "bridgeUrl": a.bridge,
                    "invite": pathlib.Path(a.invite_file.format(name=c["name"])).read_text().strip()}
            delay, reasons, t0 = 1.0, {}, time.time()
            for attempt in range(15):
                with gate:
                    h = lib.zb_client_connect(json.dumps(opts).encode())
                    why = "" if h else (lib.zb_last_error() or b"").decode(errors="replace")[:160]
                if h:
                    lib.zb_client_close(h)
                    break
                reasons[why] = reasons.get(why, 0) + 1
                time.sleep(delay * random.uniform(0.5, 1.5))
                delay = min(delay * 2, 30)
            ok = ident.exists()
            with lock:
                out.append({"client": c["name"], "enroll": "enrolled" if ok else "failed", "attempts": attempt + 1,
                            "seconds": round(time.time() - t0, 1), **({"refusals": reasons} if reasons else {})})

        threads = [threading.Thread(target=enroll, args=(c,)) for c in clients]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for r in sorted(out, key=lambda r: r["client"]):
            print(json.dumps(r), flush=True)
        print(json.dumps({"summary": True, "seconds": round(time.time() - t_all, 1), "parallel": a.enroll_parallel}), flush=True)
        return 1 if any(r["enroll"] == "failed" for r in out) else 0

    if a.batch:
        threads = []
        for c in json.loads(pathlib.Path(a.batch).read_text()):
            spec = argparse.Namespace(**vars(a))
            spec.name, spec.schedule = c["name"], json.dumps(c["schedule"])
            for k in ("db", "identity", "invite_file"):
                setattr(spec, k, getattr(a, k).format(name=c["name"]))
            t = threading.Thread(target=run_client, args=(spec, lib, take, lock, False), daemon=True)
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        return 0
    return run_client(a, lib, take, lock, True)


def run_client(a, lib, take, lock, measure):
    def say(**kv):
        with lock:
            print(json.dumps({"t": round(time.time() - a.t0, 1), "client": a.name, **kv}), flush=True)

    identity = pathlib.Path(a.identity)
    stream = f"CDC_{a.tenant}"
    for i, (t_open, t_close) in enumerate(json.loads(a.schedule)):
        while time.time() < a.t0 + t_open:
            time.sleep(0.2)
        opts = {"dbPath": a.db, "identityPath": a.identity, "tables": [TABLE], "heartbeatMs": 0, "natsUrl": a.nats, "clientId": f"field-{a.name}"}
        if a.seed_domain:
            opts["seedDomain"] = a.seed_domain
        targets = {stream: 0, "CDC_PUBLIC": 0}
        if identity.exists() and measure:
            ident = json.loads(identity.read_text())
            try:
                targets[stream] = asyncio.run(stream_last(a.hub_nats, ident["creds"], ident.get("js_domain") or "hub", stream, ident["principal"]))
            except Exception as e:
                say(event="target_failed", leg=i, error=str(e)[:200])
        if not identity.exists():
            opts["bridgeUrl"] = a.bridge
            opts["invite"] = pathlib.Path(a.invite_file).read_text().strip()
        if measure:  # process-wide in libzb: only a client alone in its process is measured
            os.environ["ZB_MEASURE"] = "1"
            os.environ["ZB_CATCHUP_TARGETS"] = ",".join(f"{k}={v}" for k, v in targets.items())
        t_c = time.time()
        say(event="leg", leg=i, start_ms=int(t_c * 1000), targets=targets, cold=i == 0)
        h = lib.zb_client_connect(json.dumps(opts).encode())
        while not h:
            why = (lib.zb_last_error() or b"").decode(errors="replace")[:200]
            if time.time() - t_c > 300:
                say(event="open_failed", leg=i, error=why); return 1
            say(event="open_retry", leg=i, error=why)
            time.sleep(5)
            h = lib.zb_client_connect(json.dumps(opts).encode())
        synced = take(lib.zb_client_sync(h))
        connect_s = time.time() - t_c
        poll_errors = 0
        end = (a.t0 + t_close) if t_close is not None else None
        # The leg's work is measured by libzb itself (the catch-up records); here the leg
        # only runs its course: until it closes, or for the last leg until the done file.
        while True:
            p = take(lib.zb_client_poll(h, 500))
            poll_errors += bool(p.get("error"))
            if end is not None and time.time() >= end:
                break
            if end is None and pathlib.Path(a.done_file).exists():
                break
        if t_close is not None:
            say(event="offline", leg=i, connect_s=round(connect_s, 2), sync_error=synced.get("error"), poll_errors=poll_errors)
            lib.zb_client_close(h)
            continue
        truth = None
        while truth is None:  # written atomically by the orchestrator, but never trust a half file
            try:
                truth = json.loads(pathlib.Path(a.done_file).read_text())
            except (json.JSONDecodeError, OSError):
                time.sleep(0.5)
        t_f = time.time()
        have = {}
        while time.time() - t_f < 300:
            take(lib.zb_client_poll(h, 500))
            have = replica_digest(a.db, a.tenant)
            if have.get("digest") == truth["digest"] and have.get("rows") == truth["rows"]:
                break
        say(event="final", leg=i, connect_s=round(connect_s, 2), exact=have.get("digest") == truth["digest"] and have.get("rows") == truth["rows"],
            rows=have.get("rows"), want_rows=truth["rows"], converge_s=round(time.time() - t_f, 1), poll_errors=poll_errors)
        lib.zb_client_close(h)
    return 0


if __name__ == "__main__":
    sys.exit(main())
