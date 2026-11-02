"""The ZeBridge side of the sync crowd (README.md, "A crowd of 500"): light readers, the twin of client/ps_crowd.mjs.

A reader does what a libzb client does on the wire and nothing else: it reads its tenant's chain manifest from the
`generations` KV (a direct get), downloads every object of the chain from `gen-<tenant>` (meta by direct get, chunks
through a named pull consumer, as libzb), then follows CDC_<tenant> from the chain's cut. It neither decompresses nor
applies: that is the client's share of a sync, not the server's, and PowerSync's readers skip it too. Synced is the
moment the last object's last chunk arrived.

    zb_crowd.py sync N MARKERS     N readers, 50 to a process, one shared start; then MARKERS seconds of one marker
                                   row a second, written through ZeBridge's write path (a mutation from p000, as a
                                   client sends it), each timed to every reader

Runs on the comparison host (zebridge.sh crowd), next to the temporary stack: NATS 127.0.0.1:14222, JetStream domain
`cmp`, the creds of `zebridge.sh creds N` in ~/zbcmp/cl/creds (one principal per reader).
"""
import asyncio, base64, datetime, json, multiprocessing as mp, os, pathlib, random, resource, statistics, sys, time

import msgpack
import nats
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

URL, DOMAIN, TENANT, TABLE = "nats://127.0.0.1:14222", "cmp", "cmp", "storm_items"
CREDS = pathlib.Path.home() / "zbcmp/cl/creds"
PER = 50


def pct(v, p):
    s = sorted(v)
    return s[min(len(s) - 1, int(p * len(s)))] if s else float("nan")


async def fetch_object(js, name, cid):
    """An object's bytes, as libzb pulls them: meta by direct get, chunks by a named consumer, 8 per fetch."""
    bucket = f"gen-{TENANT}"
    enc = base64.urlsafe_b64encode(name.encode()).decode()
    meta = json.loads((await js.get_msg(f"OBJ_{bucket}", subject=f"$O.{bucket}.M.{enc}", direct=True)).data)
    if meta.get("deleted"):
        raise RuntimeError(f"object {name} deleted")
    stream, subject = f"OBJ_{bucket}", f"$O.{bucket}.C.{meta['nuid']}"
    cfg = ConsumerConfig(name=cid, durable_name=cid, ack_policy=AckPolicy.NONE, deliver_policy=DeliverPolicy.ALL,
                         filter_subject=subject, inactive_threshold=300)
    sub = await js.pull_subscribe(subject, durable=cid, stream=stream, config=cfg)
    got = size = 0
    while got < meta.get("chunks", 0):
        try:
            for m in await sub.fetch(8, timeout=5):
                got += 1; size += len(m.data)
        except nats.errors.TimeoutError:
            pass
    await sub.unsubscribe()
    return size


async def reader(i, start, st):
    p = f"p{i:03d}"
    nc = await nats.connect(URL, user_credentials=str(CREDS / f"{p}.creds"), inbox_prefix=f"_INBOX.{p}".encode(),
                            max_reconnect_attempts=-1)
    js = nc.jetstream(domain=DOMAIN)
    man = json.loads((await js.get_msg("KV_generations", subject=f"$KV.generations.{TENANT}.{TABLE}", direct=True)).data)
    base = man.get("base") or man.get("full")
    names = [base["object"]] + [c["object"] for c in man.get("checkpoints") or []] + [d["object"] for d in man.get("deltas") or []]
    for k, name in enumerate(names):
        st["bytes"][i] += await fetch_object(js, name, f"zbo{os.getpid()}x{i}k{k}")
    st["synced"][i] = time.time() - start
    # The tail, from the cut: what a seeded client follows.
    cid = f"zbz{os.getpid():x}r{i}"
    cfg = ConsumerConfig(name=cid, durable_name=cid, ack_policy=AckPolicy.NONE, deliver_policy=DeliverPolicy.BY_START_SEQUENCE,
                         opt_start_seq=int(man.get("cutoff_seq") or 0) + 1, filter_subject=f"cdc.{TENANT}.{TABLE}.>", inactive_threshold=300)
    sub = await js.pull_subscribe(f"cdc.{TENANT}.{TABLE}.>", durable=cid, stream=f"CDC_{TENANT}", config=cfg)
    needle, last_seq = st["tag"].encode(), cfg.opt_start_seq - 1
    while not st["stop"]:
        try:
            for m in await sub.fetch(256, timeout=1):
                last_seq = m.metadata.sequence.stream
                at = m.data.find(needle)
                if at >= 0:
                    st["arrivals"].setdefault(m.data[at:at + len(needle) + 4].decode(errors="replace"), []).append(time.time())
        except (nats.errors.TimeoutError, TimeoutError):   # an empty fetch, under load too: not a lost tail
            pass
        except Exception as e:
            # As libzb does with a tail it lost: say so, and reopen it after the last event it had.
            st["tail_errors"].append(f"reader {i}: {type(e).__name__}: {e}")
            await asyncio.sleep(0.5)
            try:
                cfg.opt_start_seq = last_seq + 1
                cfg.name = cfg.durable_name = f"{cid}t{len(st['tail_errors'])}"
                sub = await js.pull_subscribe(f"cdc.{TENANT}.{TABLE}.>", durable=cfg.name, stream=f"CDC_{TENANT}", config=cfg)
            except Exception as e2:
                st["tail_errors"].append(f"reader {i} reopen: {type(e2).__name__}: {e2}")
    await nc.close()


def worker(offset, count, start, tag, q, stop_evt):
    resource.setrlimit(resource.RLIMIT_NOFILE, (65536, resource.getrlimit(resource.RLIMIT_NOFILE)[1]))

    async def main():
        st = {"synced": {}, "bytes": {i: 0 for i in range(offset, offset + count)}, "arrivals": {}, "tag": tag, "stop": False, "errors": [], "tail_errors": [], "retries": 0, "last": ""}
        await asyncio.sleep(max(0, start - time.time()))

        async def one(i):
            # A seed that fails (a request timing out under load) is retried after a second, as libzb retries it and
            # as PowerSync's readers do here; its time to synced keeps counting from the shared start.
            while time.time() - start < 900:
                try:
                    await reader(i, start, st); return
                except Exception as e:
                    if i in st["synced"]:
                        st["tail_errors"].append(f"reader {i}: {type(e).__name__}: {e}"); return
                    st["retries"] += 1; st["last"] = f"reader {i}: {type(e).__name__}: {e}"
                    await asyncio.sleep(1)
            st["errors"].append(f"reader {i}: not synced in 900 s ({st['last']})")
        tasks = [asyncio.create_task(one(i)) for i in range(offset, offset + count)]
        while len(st["synced"]) + len(st["errors"]) < count and time.time() - start < 900:
            await asyncio.sleep(0.2)
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        q.put(("synced", {"synced": list(st["synced"].values()), "bytes": list(st["bytes"].values()), "errors": st["errors"], "rss": rss, "retries": st["retries"], "last": st["last"]}))
        while not stop_evt.is_set():
            await asyncio.sleep(0.2)
        st["stop"] = True
        q.put(("arrivals", {"arrivals": st["arrivals"], "tail_errors": st["tail_errors"], "errors": st["errors"]}))
        await asyncio.wait(tasks, timeout=3)
    try:
        asyncio.run(main())
    except Exception:
        pass   # readers still closing when the process ends


async def write_markers(tag, seconds):
    """One marker a second through ZeBridge's write path, as a client sends it (mutation, zb1 message ID)."""
    sys.path.insert(0, str(pathlib.Path(__file__).parent))
    from ingress_load import zb1_id
    nc = await nats.connect(URL, user_credentials=str(CREDS / "p000.creds"), inbox_prefix=b"_INBOX.p000")
    js = nc.jetstream()
    sent = {}
    for k in range(seconds):
        t, iso = time.time(), datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        mid = f"{tag}{k:04d}"
        row = {"id": mid, "tenant_id": TENANT, "note": "marker", "updated_at": iso}
        body = msgpack.packb({"key": {"id": mid}, "data": row, "version": iso, "client_id": "crowd-writer"})
        await js.publish(f"mutation.p000.{TABLE}.insert", body, headers={"Nats-Msg-Id": zb1_id(int(t * 1000), "crowd-writer", mid, iso)})
        sent[mid] = t
        await asyncio.sleep(max(0, k + 1 - (time.time() - next(iter(sent.values())))))
    await nc.close()
    return sent


async def clear_readers():
    """The consumers earlier runs' readers left (a client cannot delete its own; they expire after 5 min), removed
    with the bridge's credentials, so a run does not carry the last one's. Only the readers' (zbo…, zbz…)."""
    nc = await nats.connect(URL, user_credentials=str(pathlib.Path.home() / "zbcmp/zb/id/creds/bridge.creds"))
    js = nc.jetstream(domain=DOMAIN)
    n = 0
    for stream in (f"CDC_{TENANT}", f"OBJ_gen-{TENANT}"):
        names, offset = [], 0
        while page := await js.consumers_info(stream, offset=offset):   # 256 to a page
            names += [c.name for c in page if c.name.startswith(("zbo", "zbz"))]; offset += len(page)
        for name in names:
            try:
                await js.delete_consumer(stream, name); n += 1
            except nats.js.errors.NotFoundError:
                pass   # expired meanwhile
    await nc.close()
    return n


def sync(n, markers):
    print(f"(cleared {asyncio.run(clear_readers())} consumers left by earlier runs)", flush=True)
    tag = f"m-{random.randbytes(3).hex()}-"
    start = time.time() + 3 + n / 100
    q, stop_evt = mp.Queue(), mp.Event()
    procs = [mp.Process(target=worker, args=(off, min(PER, n - off), start, tag, q, stop_evt)) for off in range(0, n, PER)]
    for p in procs:
        p.start()
    parts = [q.get()[1] for _ in procs]
    synced = [s for p in parts for s in p["synced"]]
    errors = [e for p in parts for e in p["errors"]]
    mb = [b / 1048576 for p in parts for b in p["bytes"]]
    print(f"zebridge crowd sync, {n} readers in {len(procs)} process(es): synced {len(synced)}/{n}, median {statistics.median(synced):.2f} s, "
          f"p95 {pct(synced, .95):.2f} s, slowest {max(synced):.2f} s; {statistics.median(mb):.2f} MB downloaded per reader (compressed); "
          f"{sum(p['rss'] for p in parts) / n:.1f} MB per reader; {sum(p['retries'] for p in parts)} retried seeds" if synced else f"zebridge crowd sync: none synced; {errors[:3]}", flush=True)
    sent = asyncio.run(write_markers(tag, markers)) if markers else {}
    time.sleep(10)   # the last markers reach the slowest readers
    stop_evt.set()
    arrivals, tail_errors = {}, []
    for _ in procs:
        part = q.get()[1]
        tail_errors += part["tail_errors"] + [e for e in part["errors"] if e not in errors]
        for mid, ts in part["arrivals"].items():
            arrivals.setdefault(mid, []).extend(ts)
    for p in procs:
        p.join(10)
    if markers:
        lat = [(a - t) * 1000 for mid, t in sent.items() for a in arrivals.get(mid, [])]
        complete = sum(1 for mid in sent if len(arrivals.get(mid, [])) >= len(synced))
        print(f"zebridge crowd markers: {len(sent)} sent, {complete} reached all {len(synced)} readers; one write to a reader median "
              f"{pct(lat, .5):.0f} ms, p95 {pct(lat, .95):.0f} ms, slowest {max(lat, default=float('nan')):.0f} ms (n={len(lat)})")
        short = {mid: len(arrivals.get(mid, [])) for mid in sent if len(arrivals.get(mid, [])) < len(synced)}
        if short:
            print(f"  markers short of {len(synced)} readers: {short}")
    if errors:
        print(f"errors: {len(errors)}, first: {' | '.join(errors[:3])}")
    if tail_errors:
        print(f"tail errors (each reopened): {len(tail_errors)}, first: {' | '.join(tail_errors[:3])}")


if __name__ == "__main__":
    if sys.argv[1:2] != ["sync"]:
        sys.exit(__doc__)
    sync(int(sys.argv[2]), int(sys.argv[3]) if len(sys.argv) > 3 else 0)
