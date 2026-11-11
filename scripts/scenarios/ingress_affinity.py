#!/usr/bin/env python3
"""The ingress dispatcher under a held lane: full queue, long hold, stop, crash.

One client's ordered chain is held by a trigger in its lane while the chain fills that
lane's queue past its capacity (the rest waits in the dispatcher). The checks: an
unrelated client on another lane still commits; a hold longer than the ack wait causes
no redelivery (the dispatcher renews the deadlines); a clean stop with full queues and a
kill -9 with queued writes both end with every write applied exactly once; one client mixing
ordered and unordered writes keeps the ordered ones on its lane and spreads the rest; under
sustained ordered load on every lane an unordered write is still judged. Raw
publishers through real admission, four lanes, a disposable stack.
"""
import argparse, asyncio, json, os, signal, tempfile, time, uuid
from pathlib import Path
import msgpack, nats
import zb
from recreate_recovery import Stack, BRIDGE
from ingress_ordering import Gates, LOCK_CLASS, PRINCIPAL, TENANT, ordered_id

P, IND, SLOW = 'ia_parent', 'ia_independent', 'ia_slow'
SLOW_ROW_S = 0.005  # ia_slow's trigger: every lane stays saturated at a modest publish rate
ACK_WAIT_S = 10  # config.Nats.mutation_ack_wait_ns
QUEUE_CAPACITY = 512  # LaneQueue.capacity: 2 × mutation_pull_batch

async def until(probe, timeout, what):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if probe(): return
        await asyncio.sleep(.1)
    raise RuntimeError(f'not reached within {timeout}s: {what}')

async def main(args):
    args.log_dir.mkdir(parents=True, exist_ok=True)
    s = Stack(args.log_dir, 'affinity', 120, {'ZB_INGRESS_LANES': '4'}, pg_port=int(os.getenv('ZB_TEST_PG_PORT', '5432')))
    s.log = lambda label: str(args.log_dir / f'{label}.log')
    nc = other = admin = gates = None
    checks = []
    def check(label, condition, **evidence):
        checks.append(dict(check=label, passed=bool(condition), **evidence))
        (zb.ok if condition else zb.bad)(label + (f'  {json.dumps(evidence)}' if evidence else ''))
        (args.log_dir / 'summary.json').write_text(json.dumps(checks, indent=2) + '\n')
        if not condition: raise AssertionError(label)
    try:
        s.start({PRINCIPAL: TENANT, 'iop2': 'iot2'})
        s.env['MUTATION_ADMISSION'] = '1'
        # Small writes only: a 4 KiB message cap lets one principal hold 5000 unacked writes
        # (64 MiB / (64 KiB + 512) ≈ 1016 by default), enough to saturate four lanes in E.
        s.env['MUTATION_MAX_MESSAGE_BYTES'] = '4096'
        common = 'tenant_id text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz'
        s.sql(f'CREATE TABLE {P}(id uuid PRIMARY KEY, {common}); CREATE TABLE {IND}(id uuid PRIMARY KEY, {common}); CREATE TABLE {SLOW}(id uuid PRIMARY KEY, {common});')
        for table in (P, IND, SLOW):
            s.sql(f"SELECT count(*) FROM zebridge_enable('public.{table}'::regclass, writable=>true, tenant_col=>'tenant_id', version_col=>'updated_at', tombstone_col=>'deleted_at', publication=>'rr_pub', dry_run=>false)")
        s.sql(f"""CREATE FUNCTION ia_hold() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
            PERFORM pg_advisory_xact_lock({LOCK_CLASS}, 1); RETURN NEW; END $$;
            CREATE TRIGGER ia_parent_hold AFTER INSERT ON {P} FOR EACH ROW EXECUTE FUNCTION ia_hold();
            CREATE FUNCTION ia_slow() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
            PERFORM pg_sleep({SLOW_ROW_S}); RETURN NEW; END $$;
            CREATE TRIGGER ia_slow_row BEFORE INSERT ON {SLOW} FOR EACH ROW EXECUTE FUNCTION ia_slow();""")
        s.spawn('admission', [str(BRIDGE), '--mutation-ingress'], s.env)
        s.wait_log('admission', 'mutation admission ready')
        label = s.bridge_up()
        s.wait_log(label, 'Mutation dispatcher: pulling for 4 lane(s)')
        nc = await nats.connect(s.url, user_credentials=s.creds[PRINCIPAL], inbox_prefix=f'_INBOX.{PRINCIPAL}'.encode())
        other = await nats.connect(s.url, user_credentials=s.creds['iop2'], inbox_prefix=b'_INBOX.iop2')
        admin = await nats.connect(s.url, user_credentials=s.bridge_creds)
        js = nc.jetstream(domain='hub')
        ack = zb.TOPOLOGY['subjects']['mutation_ack_prefix']
        verdicts = await nc.subscribe(f'{ack}.{PRINCIPAL}.>')
        other_verdicts = await other.subscribe(f'{ack}.iop2.>')
        await nc.flush(); await other.flush()
        version = s.sql("SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')")
        seen = {}  # message id -> [verdicts]
        held = set()
        async def collect(ids, timeout, what):
            end = time.monotonic() + timeout
            while any(i not in seen for i in ids) and time.monotonic() < end:
                try: m = await verdicts.next_msg(timeout=.5)
                except asyncio.TimeoutError: continue
                tail = m.subject[len(f'{ack}.{PRINCIPAL}.'):]
                if tail.endswith('.held'): held.add(tail[:-5]); continue
                seen.setdefault(tail, []).append(zb.decode(m.data))
            missing = [i for i in ids if i not in seen]
            if missing: raise RuntimeError(f'{what}: {len(missing)} write(s) without a verdict')
        async def drain(quiet=1.0):
            """Reads the verdicts already delivered, until the subscription has been quiet."""
            while True:
                try: m = await verdicts.next_msg(timeout=quiet)
                except asyncio.TimeoutError: return
                tail = m.subject[len(f'{ack}.{PRINCIPAL}.'):]
                if tail.endswith('.held'): held.add(tail[:-5]); continue
                seen.setdefault(tail, []).append(zb.decode(m.data))
        async def chain(n):
            """A parent then n-1 ordered successors from one client, each naming the one before."""
            ids, rows, pred = [], [], None
            for _ in range(n):
                mid, rid = ordered_id(P), str(uuid.uuid4())
                data = dict(id=rid, tenant_id=TENANT, updated_at=version)
                payload = dict(key={'id': rid}, data=data, version=version, client_id='one-client', ordering=1, predecessor=pred)
                await js.publish(zb.subject(zb.TOPOLOGY['subjects']['mutations_prefix'], PRINCIPAL, P) + '.insert', msgpack.packb(payload), headers={'Nats-Msg-Id': mid}, timeout=10)
                ids.append(mid); rows.append(rid); pred = mid
            return ids, rows
        async def consumer():
            return await admin.jetstream(domain='hub').consumer_info('MUTATIONS', 'bridge_mutations_worker')
        def queued(metrics): return {k[len('bridge_ingress_queued{lane="'):-2]: int(v) for k, v in metrics.items() if k.startswith('bridge_ingress_queued{')}
        def rows_of(ids_rows): return int(s.sql(f"SELECT count(*) FROM {P} WHERE id IN ({','.join(repr(r) for r in ids_rows)})"))
        async def wait_full(n):
            """The chain's lane holds a full queue and the dispatcher the rest."""
            def probe():
                m = s.metrics(); q = queued(m)
                return max(q.values()) >= QUEUE_CAPACITY and m['bridge_ingress_pending'] >= n - QUEUE_CAPACITY - 256
            await until(probe, 30, 'the held lane filled and the dispatcher holds the rest')
            return s.metrics()

        # ── A: a hold longer than the ack wait, the lane full, unrelated work flowing ──
        gates = Gates(s); await gates.wait(1, granted=True)
        n = QUEUE_CAPACITY + 200
        t0 = time.monotonic()
        ids, rows = await chain(n)
        await gates.wait(1)  # the parent's AFTER INSERT is blocked: the lane is held
        m = await wait_full(n)
        q = queued(m)
        check('the held client fills one lane and the dispatcher keeps the rest pending', max(q.values()) >= QUEUE_CAPACITY and m['bridge_ingress_pending'] > 0 and sum(1 for v in q.values() if v > 0) == 1, queued=q, pending=m['bridge_ingress_pending'])
        oid, orow = str(uuid.uuid7()), str(uuid.uuid4())
        await other.jetstream(domain='hub').publish(zb.subject(zb.TOPOLOGY['subjects']['mutations_prefix'], 'iop2', IND) + '.insert', msgpack.packb(dict(key={'id': orow}, data=dict(id=orow, tenant_id='iot2', updated_at=version), version=version, client_id='other-client')), headers={'Nats-Msg-Id': oid}, timeout=10)
        ov = zb.decode((await other_verdicts.next_msg(timeout=5)).data)
        check('an unrelated client commits on another lane while the held lane is full', ov['status'] == 'accepted' and gates.pids(1) and s.sql(f"SELECT count(*) FROM {IND} WHERE id='{orow}'") == '1', unrelated=ov['status'])
        await asyncio.sleep(max(0, ACK_WAIT_S + 2.5 - (time.monotonic() - t0)))
        ci = await consumer(); m = s.metrics()
        check('a hold longer than the ack wait redelivers nothing: the deadlines were renewed', ci.num_redelivered == 0 and m['bridge_ingress_ack_renewals_total'] >= 1 and gates.pids(1), held_s=round(time.monotonic() - t0, 1), renewals=m['bridge_ingress_ack_renewals_total'], redelivered=ci.num_redelivered)
        gates.release(1)
        await collect(ids, 90, 'A')
        ci = await consumer(); m = s.metrics()
        check('after the release every write of the chain is applied exactly once, none parked, none redelivered', rows_of(rows) == n and all(len(v) == 1 and v[0]['status'] == 'accepted' for v in (seen[i] for i in ids)) and not held and m['bridge_ingress_parked_total'] == 0 and ci.num_redelivered == 0, rows=rows_of(rows), parked=m['bridge_ingress_parked_total'], redelivered=ci.num_redelivered)
        gates.close()

        # ── B: a clean stop with the queues full ──
        gates = Gates(s); await gates.wait(1, granted=True)
        ids, rows = await chain(n)
        await gates.wait(1)
        await wait_full(n)
        s.procs[label].send_signal(signal.SIGTERM)
        await until(lambda: 'left for the next boot' in Path(s.log(label)).read_text(errors='replace'), 20, 'the dispatcher gave the pending writes back')
        check('the stop does not wait behind the held transaction: the pending writes are left for the next boot', gates.pids(1) and s.procs[label].poll() is None)
        gates.release(1)
        await until(lambda: s.procs[label].poll() is not None, 90, 'the bridge exited after the release')
        s.procs.pop(label)
        await drain()
        ci = await consumer()
        before = {i: len(seen.get(i, [])) for i in ids}
        check('what the lanes held was judged before exit; what the dispatcher held is in flight for the next boot', sum(before.values()) > 0 and ci.num_ack_pending == n - sum(before.values()) and ci.num_redelivered == 0, judged_before_exit=sum(before.values()), ack_pending=ci.num_ack_pending)
        label = s.bridge_up()
        await collect(ids, 90, 'B')
        check('after the restart the chain is complete, each write judged exactly once', rows_of(rows) == n and all(len(v) == 1 and v[0]['status'] == 'accepted' for v in (seen[i] for i in ids)), rows=rows_of(rows))
        gates.close()

        # ── C: kill -9 with writes queued, then the boot drain (§10eb) ──
        gates = Gates(s); await gates.wait(1, granted=True)
        n = 300
        ids, rows = await chain(n)
        await gates.wait(1)
        await until(lambda: s.metrics()['bridge_ingress_fetched_total'] >= n, 30, 'the dispatcher fetched the chain')
        s.procs.pop(label).kill()
        gates.release(1); gates.close(); gates = None
        label = s.bridge_up()
        await collect(ids, 120, 'C')
        check('a crash with queued writes loses nothing and applies nothing twice', rows_of(rows) == n and all(len(v) == 1 and v[0]['status'] == 'accepted' for v in (seen[i] for i in ids)) and 'were in flight when the previous instance stopped' in Path(s.log(label)).read_text(errors='replace'), rows=rows_of(rows))

        # ── D: one client, ordered and unordered writes interleaved, nothing held ──
        def lane_writes(metrics): return {k: v for k, v in metrics.items() if k.startswith('bridge_ingress_lane_writes_total{')}
        m0 = s.metrics()
        n = 400
        ids, rows, uids, urows, pred = [], [], [], [], None
        for _ in range(n):
            mid, rid = ordered_id(P), str(uuid.uuid4())
            payload = dict(key={'id': rid}, data=dict(id=rid, tenant_id=TENANT, updated_at=version), version=version, client_id='one-client', ordering=1, predecessor=pred)
            await js.publish(zb.subject(zb.TOPOLOGY['subjects']['mutations_prefix'], PRINCIPAL, P) + '.insert', msgpack.packb(payload), headers={'Nats-Msg-Id': mid}, timeout=10)
            ids.append(mid); rows.append(rid); pred = mid
            uid, urow = str(uuid.uuid7()), str(uuid.uuid4())
            await js.publish(zb.subject(zb.TOPOLOGY['subjects']['mutations_prefix'], PRINCIPAL, IND) + '.insert', msgpack.packb(dict(key={'id': urow}, data=dict(id=urow, tenant_id=TENANT, updated_at=version), version=version, client_id='one-client')), headers={'Nats-Msg-Id': uid}, timeout=10)
            uids.append(uid); urows.append(urow)
        await collect(ids + uids, 90, 'D')
        m1 = s.metrics()
        delta = lambda k: m1.get(k, 0) - m0.get(k, 0)
        spread_lanes = sum(1 for k, v in lane_writes(m1).items() if v - m0.get(k, 0) > 0)
        applied_u = int(s.sql(f"SELECT count(*) FROM {IND} WHERE id IN ({','.join(repr(r) for r in urows)})"))
        check('mixed traffic from one client: ordered writes (the first included) keep its lane and never park, unordered ones use several lanes, all applied once',
              rows_of(rows) == n and applied_u == n and all(len(seen[i]) == 1 and seen[i][0]['status'] == 'accepted' for i in ids + uids)
              and delta('bridge_ingress_parked_total') == 0 and delta('bridge_ingress_routed_ordered_total') == n and delta('bridge_ingress_routed_spread_total') == n and spread_lanes >= 2,
              ordered=rows_of(rows), unordered=applied_u, parked=delta('bridge_ingress_parked_total'), routed_ordered=delta('bridge_ingress_routed_ordered_total'), routed_spread=delta('bridge_ingress_routed_spread_total'), lanes_used=spread_lanes)

        # ── E: sustained ordered load on every lane; an unordered write is still served ──
        # Ordered-first distribution alone refills every slot a lane frees from that lane's
        # pending ordered writes, so the spill would wait until the ordered traffic stopped.
        stop_pub = asyncio.Event()
        e_ids, e_rows = [], []
        # Six clients per lane for principal 'iop' (routeLane's Wyhash, 4 lanes): an even load,
        # or the lanes with fewer clients drain while the dispatcher sits at its budget, and a
        # lane with room always serves the spill. `saturated` fails loudly if routing changes.
        clients = ['c000', 'c001', 'c002', 'c003', 'c004', 'c005', 'c006', 'c007', 'c008', 'c009', 'c010', 'c011',
                   'c012', 'c013', 'c014', 'c015', 'c016', 'c017', 'c018', 'c019', 'c020', 'c021', 'c023', 'c033']
        preds = {c: None for c in clients}
        async def publisher(mine):
            while not stop_pub.is_set():
                for c in mine:
                    mid, rid = ordered_id(SLOW, c), str(uuid.uuid4())
                    payload = dict(key={'id': rid}, data=dict(id=rid, tenant_id=TENANT, updated_at=version), version=version, client_id=c, ordering=1, predecessor=preds[c])
                    while not stop_pub.is_set():
                        try:
                            await js.publish(zb.subject(zb.TOPOLOGY['subjects']['mutations_prefix'], PRINCIPAL, SLOW) + '.insert', msgpack.packb(payload), headers={'Nats-Msg-Id': mid}, timeout=10)
                            break
                        except nats.js.errors.ServiceUnavailableError:  # the principal's backlog is full: back off, as a client does
                            await asyncio.sleep(.05)
                    else: return
                    e_ids.append(mid); e_rows.append(rid); preds[c] = mid
                await asyncio.sleep(.003)  # more than twice what the four slowed lanes apply
        # The durable's max_ack_pending (the server default, 1,000) is below four lanes' queues
        # (4 × 512): the server would never deliver enough to fill them and the dispatcher's
        # buffer too, and the spill could not starve. Raised here, so the condition the rule
        # guards exists.
        ci = await consumer()
        cfg = ci.config; cfg.max_ack_pending = 20000
        await admin.jetstream(domain='hub').add_consumer('MUTATIONS', cfg)
        pubs = [asyncio.create_task(publisher(clients[i::8])) for i in range(8)]
        def saturated():
            m = s.metrics(); q = queued(m)
            return len(q) >= 4 and all(q.get(str(k), 0) >= QUEUE_CAPACITY // 2 for k in range(4)) and m['bridge_ingress_pending'] >= 4 * QUEUE_CAPACITY - 150
        async def read_for(seconds):
            """Reads verdicts for at most `seconds` (drain() waits for a silence that never comes here)."""
            end = time.monotonic() + seconds
            while time.monotonic() < end:
                try: m = await verdicts.next_msg(timeout=max(.01, end - time.monotonic()))
                except asyncio.TimeoutError: return
                tail = m.subject[len(f'{ack}.{PRINCIPAL}.'):]
                if tail.endswith('.held'): held.add(tail[:-5]); continue
                seen.setdefault(tail, []).append(zb.decode(m.data))
        async def waiting_verdicts(probe, timeout, what):
            end = time.monotonic() + timeout
            while time.monotonic() < end:
                if probe(): return
                await read_for(.2)
            m = s.metrics()
            raise RuntimeError(f'not reached within {timeout}s: {what} (queued {queued(m)}, pending {m["bridge_ingress_pending"]}, published {len(e_ids)}, judged {sum(1 for i in e_ids if i in seen)})')
        await waiting_verdicts(saturated, 30, 'every lane saturated by ordered writes, more pending')
        uid, urow = str(uuid.uuid7()), str(uuid.uuid4())
        t_u = time.monotonic()
        # Another principal: 'iop' sits at its admission backlog cap (admission's own
        # per-principal fairness), and this case is about the dispatcher.
        other_seen = {}
        async def other_wait():
            m = await other_verdicts.next_msg(timeout=60)
            other_seen[m.subject[len(f'{ack}.iop2.'):]] = zb.decode(m.data)
        ow = asyncio.create_task(other_wait())
        await other.jetstream(domain='hub').publish(zb.subject(zb.TOPOLOGY['subjects']['mutations_prefix'], 'iop2', IND) + '.insert', msgpack.packb(dict(key={'id': urow}, data=dict(id=urow, tenant_id='iot2', updated_at=version), version=version, client_id='other-client')), headers={'Nats-Msg-Id': uid}, timeout=10)
        await waiting_verdicts(lambda: uid in other_seen, 25, 'the unordered write was judged while ordered traffic continued')
        await ow
        waited = round(time.monotonic() - t_u, 1)
        m = s.metrics()
        still_loaded = not stop_pub.is_set() and m['bridge_ingress_pending'] > 0
        stop_pub.set(); await asyncio.gather(*pubs)
        s.sql(f'DROP TRIGGER ia_slow_row ON {SLOW}')  # the backlog drains at full speed
        check('under sustained ordered load on every lane an unordered write is judged before that load stops', other_seen[uid]['status'] == 'accepted' and still_loaded, waited_s=waited, pending_then=m['bridge_ingress_pending'], queued_then=queued(m))
        await collect(e_ids, 180, 'E')
        applied = int(s.sql(f'SELECT count(*) FROM {SLOW}'))
        check('the ordered load itself is then applied exactly once', applied == len(e_ids) and all(len(seen[i]) == 1 and seen[i][0]['status'] == 'accepted' for i in e_ids), writes=len(e_ids), applied=applied, parked=s.metrics()['bridge_ingress_parked_total'])
        print(f'{len(checks)}/{len(checks)} clean')
        return 0
    finally:
        if gates: gates.close()
        for conn in (nc, other, admin):
            if conn: await conn.close()
        s.stop()

if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--log-dir', type=Path, default=Path(tempfile.mkdtemp(prefix='zb-ingress-affinity-')))
    raise SystemExit(asyncio.run(main(p.parse_args())))
