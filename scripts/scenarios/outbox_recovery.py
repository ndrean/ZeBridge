#!/usr/bin/env python3
"""Automatic replay volume with nonempty durable outboxes and real client restarts.

Disposable PostgreSQL/NATS/admission/bridge. The same 60-client process ceiling as
storm/storm.py; half native, half TS by default. The write tables leave the private
publication after schema discovery, so only verdicts can settle these outboxes.
This isolates recovery traffic from CDC fanout; it is not a CDC capacity result.
The fixture restores realistic queued JSON envelopes directly into SQLite, leaving
their IDs/bytes immutable. Workers never explicitly flush unless the comparison's
ZB_OUTBOX_LEGACY_PUMP is set for both revisions.
"""
import asyncio, ctypes, json, os, pathlib, resource, signal, sqlite3, sys, tempfile, threading, time, uuid
from collections import defaultdict
import msgpack, nats, nkeys
import zb
from hub_fallback_stage2 import BRIDGE, LIBRARY, Stack, write
from rotate_client_key import client_creds


class RecoveryStack(Stack):
    def spawn(self, label, args, env):
        if label == 'nats':
            # Stack replaces the generated listener/JetStream block. Restore the
            # generated bridge configuration's consumer default for fleet runs.
            conf = pathlib.Path(args[args.index('-c') + 1])
            text = conf.read_text()
            if 'default_max_consumers' not in text:
                text = text.replace('jetstream {', 'jetstream { limits { default_max_consumers: 5000 },', 1)
                conf.write_text(text)
        super().spawn(label, args, env)


def native_worker(path):
    options = json.loads(pathlib.Path(path).read_text())
    lib = ctypes.CDLL(str(LIBRARY))
    lib.zb_client_connect.argtypes, lib.zb_client_connect.restype = [ctypes.c_char_p], ctypes.c_uint64
    lib.zb_client_close.argtypes = [ctypes.c_uint64]
    lib.zb_free.argtypes = [ctypes.c_void_p]
    lib.zb_last_error.restype = ctypes.c_char_p
    for fn, args in (('sync', []), ('poll', [ctypes.c_uint64]), ('query', [ctypes.c_char_p, ctypes.c_char_p])):
        f = getattr(lib, 'zb_client_' + fn)
        f.argtypes, f.restype = [ctypes.c_uint64] + args, ctypes.c_void_p
    def take(p):
        if not p: raise RuntimeError('native returned no JSON')
        try: return json.loads(ctypes.string_at(p))
        finally: lib.zb_free(p)
    legacy = os.environ.get('ZB_OUTBOX_LEGACY_PUMP') == '1'
    lib.zb_client_flush_outbox.argtypes, lib.zb_client_flush_outbox.restype = [ctypes.c_uint64], ctypes.c_void_p
    stop = threading.Event()
    pending, errors, ready = [None] * len(options), [], [False] * len(options)
    quick = [0] * len(options)
    idle_tail = os.environ.get('ZB_OUTBOX_IDLE_TAIL') == '1'
    def client(i, opts):
        h = 0
        try:
            h = lib.zb_client_connect(json.dumps(opts).encode())
            if not h: raise RuntimeError((lib.zb_last_error() or b'connect failed').decode())
            result = take(lib.zb_client_sync(h))
            if 'error' in result: raise RuntimeError(str(result))
            if legacy: take(lib.zb_client_flush_outbox(h))
            ready[i] = True
            next_flush = time.monotonic()+5
            while not stop.is_set():
                if legacy and time.monotonic() >= next_flush:
                    take(lib.zb_client_flush_outbox(h))
                    next_flush = time.monotonic()+5
                began = time.monotonic()
                poll = take(lib.zb_client_poll(h, 1000 if idle_tail else 250))
                if idle_tail and time.monotonic()-began < .5 and poll.get('settled',0)>0: quick[i] += 1
                result = take(lib.zb_client_query(h, b'SELECT count(*) FROM _zebridge_outbox', b'[]'))
                pending[i] = result['rows'][0][0]
                stop.wait(.001 if pending[i] else .05)
        except Exception as e: errors.append(str(e))
        finally:
            if h: lib.zb_client_close(h)
    threads = [threading.Thread(target=client, args=(i,o)) for i,o in enumerate(options)]
    try:
        for t in threads: t.start()
        deadline = time.monotonic() + 90
        while not all(ready) and not errors and time.monotonic() < deadline: time.sleep(.05)
        print(json.dumps({'ready': sum(ready), 'errors': errors}), flush=True)
        for line in sys.stdin:
            if json.loads(line) == 'close': break
            print(json.dumps({'pending': pending, 'errors': errors, 'quick_settling_polls':quick}), flush=True)
    finally:
        stop.set()
        for t in threads: t.join()
        if os.environ.get('ZB_CHECK_LEAKS') == '1' and sys.platform == 'darwin':
            import subprocess
            result = subprocess.run(['leaks','--nocontext',str(os.getpid())],capture_output=True,text=True,timeout=45)
            if result.returncode != 0 or '0 leaks for 0 total leaked bytes' not in result.stdout:
                raise RuntimeError('native worker allocation check: '+result.stdout[-1500:]+result.stderr[-500:])
            print('native worker: 0 leaks for 0 total leaked bytes',file=sys.stderr)


class Worker:
    def __init__(self, root, name, mode, options):
        self.root, self.name, self.mode, self.options = root, name, mode, options
        self.proc = None
    async def start(self):
        path = self.root / (self.name + '.json')
        path.write_text(json.dumps(self.options))
        cmd = ([sys.executable, __file__, '--worker', str(path)] if self.mode == 'native' else
               ['node', '--experimental-strip-types', str(zb.ROOT/'scripts/scenarios/outbox_recovery.mts'), str(path)])
        self.log = self.root / (self.name + '.log')
        self.proc = await asyncio.create_subprocess_exec(*cmd, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=open(self.log, 'a'))
        result = await self.receive()
        if result.get('errors') or result.get('ready') != len(self.options): raise RuntimeError(f'{self.name}: {result}\n' + self.log.read_text()[-1500:])
    async def receive(self):
        line = await asyncio.wait_for(self.proc.stdout.readline(), 120)
        if not line: raise RuntimeError(self.name + ': ' + self.log.read_text()[-2000:])
        return json.loads(line)
    async def state(self):
        self.proc.stdin.write(b'"state"\n')
        await self.proc.stdin.drain()
        return await self.receive()
    async def close(self):
        if self.proc and self.proc.returncode is None:
            try:
                self.proc.stdin.write(b'"close"\n')
                await self.proc.stdin.drain()
                # A requested native allocation scan alone allows 45 seconds.
                await asyncio.wait_for(self.proc.wait(), 90)
            except (asyncio.TimeoutError, BrokenPipeError, ConnectionResetError):
                self.proc.kill()
                await self.proc.wait()
        if self.proc and self.proc.returncode != 0:
            print(f'{self.name} exited {self.proc.returncode}:\n{self.log.read_text()[-3000:]}', file=sys.stderr)
        return self.proc is None or self.proc.returncode == 0


def queue(db, who, count):
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE _zebridge_outbox (msg_id TEXT PRIMARY KEY, subject TEXT NOT NULL, payload TEXT NOT NULL, tbl TEXT NOT NULL, row_id TEXT NOT NULL, before TEXT, created_at INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, published INTEGER NOT NULL DEFAULT 1, tenant TEXT, optimistic TEXT)')
        for i in range(count):
            key = str(uuid.uuid4())
            mid, wire = write('s2_row', key, {'v':i}, client=who)
            conn.execute('INSERT INTO _zebridge_outbox(msg_id,subject,payload,tbl,row_id,created_at) VALUES (?,?,?,?,?,?)',
                         (mid, f'mutation.{who}.s2_row.insert', json.dumps(msgpack.unpackb(wire, raw=False)), 's2_row', key, i))


def max_window(events, seconds=4):
    times = sorted(e[0] for e in events)
    start = maximum = 0
    for end, t in enumerate(times):
        while times[start] < t - seconds: start += 1
        maximum = max(maximum, end - start + 1)
    return maximum


async def main():
    count = int(os.environ.get('ZB_OUTBOX_STORM_CLIENTS', '8'))
    pending = int(os.environ.get('ZB_OUTBOX_PENDING', '256'))
    window = 128
    budget = float(os.environ.get('ZB_OUTBOX_DRAIN_SECONDS', '180'))
    cdc = os.environ.get('ZB_OUTBOX_CDC') == '1'
    legacy = os.environ.get('ZB_OUTBOX_LEGACY_PUMP') == '1'
    check_window = not legacy and os.environ.get('ZB_OUTBOX_VALIDATE_WINDOW','1') != '0'
    mode = os.environ.get('ZB_OUTBOX_CLIENTS', 'both')
    if count < 1 or pending < 1: raise RuntimeError('use positive client and pending counts')
    passed = failed = 0
    def check(ok, label):
        nonlocal passed, failed
        (zb.ok if ok else zb.bad)(label)
        passed += bool(ok); failed += not ok
    with tempfile.TemporaryDirectory(prefix='zb-outbox-storm-') as tmp:
        root = pathlib.Path(tmp)
        stack, observer, workers = RecoveryStack(root), None, []
        events = defaultdict(lambda: defaultdict(list))
        all_options, original = [], {}
        try:
            await stack.start()
            ident = root / (stack.name + '-id')
            keys = dict(l.split('=',1) for l in (ident/'operator.store').read_text().splitlines() if '=' in l and not l.startswith('#'))
            account = nkeys.from_seed(keys['ACCOUNT_SEED'].encode()).public_key.decode()
            who = [f'q{i:04d}' for i in range(count)]
            stack.sql('INSERT INTO zebridge_user_tenants VALUES ' + ','.join(f"('{p}','_default')" for p in who))
            # Keep real admission, listener, receipts and verdict delivery, without
            # turning 32,000 writes into millions of unrelated CDC deliveries.
            if not cdc: stack.sql('ALTER PUBLICATION hf2_pub DROP TABLE s2_row, s2_parent, s2_child')
            async def expected_outage(_): pass
            observer = await nats.connect(stack.url, user_credentials=stack.bridge_creds, max_reconnect_attempts=-1, error_cb=expected_outage)
            # The bridge projects membership asynchronously. A fixed sleep can
            # freeze it halfway through the fleet and make later clients time out.
            kv = await observer.jetstream(domain='hub').key_value('tenants')
            deadline = time.monotonic()+60
            for principal in who:
                while True:
                    try:
                        await kv.get(principal)
                        break
                    except nats.js.errors.KeyNotFoundError:
                        if time.monotonic() >= deadline: raise RuntimeError('membership was not projected: '+principal)
                        await asyncio.sleep(.05)
            stack.procs['bridge'].send_signal(signal.SIGSTOP)
            async def traffic(msg):
                t = time.monotonic()
                if msg.subject.startswith('mutation.'):
                    p = msg.subject.split('.')[1]
                    mid = msg.headers.get('Nats-Msg-Id','') if msg.headers else ''
                    events[p]['pub'].append((t,mid))
                elif '.DIRECT.GET.VERDICTS.' in msg.subject:
                    tail = msg.subject.split('.mutation_ack.',1)[-1]
                    p, _, mid = tail.partition('.')
                    if p.startswith('q') and mid.startswith('zb1-'): events[p]['get'].append((t,mid))
            await observer.subscribe('mutation.>', cb=traffic)
            # Domain requests are mapped to $JS.API before reaching subscriptions.
            await observer.subscribe('$JS.>', cb=traffic)
            await observer.flush()
            consumer_peak = {}
            async def consumers(label):
                try:
                    js = observer.jetstream(domain='hub')
                    account_info = await js.account_info()
                    snapshot = {}
                    for stream in ('CDC_PUBLIC', 'KV_schemas'):
                        info = await js.stream_info(stream)
                        consumer_peak[stream] = max(consumer_peak.get(stream,0),info.state.consumer_count)
                        snapshot[stream] = {'count':info.state.consumer_count,'limit':info.config.max_consumers}
                    # Who holds them: libzb names its tails `zbz…`; TS consumers get server names.
                    kinds = {'native': 0, 'ts': 0}
                    for ci in await js.consumers_info('CDC_PUBLIC'):
                        kinds['native' if ci.name.startswith('zbz') else 'ts'] += 1
                    print(json.dumps({'consumer_snapshot':label,'account_limit':account_info.limits.max_consumers,'account_consumers':account_info.consumers,'server_default_max_consumers':5000,'streams':snapshot,'cdc_public_by_kind':kinds}))
                except Exception as e: print(json.dumps({'consumer_snapshot':label,'unavailable':str(e)}))
            await consumers('before clients')
            for i,p in enumerate(who):
                creds = root / (p + '.creds')
                creds.write_text(client_creds(keys['SK_CLIENT_SEED'], account, p, '_default'))
                db = root / (p + '.db')
                queue(db, p, pending)
                with sqlite3.connect(db) as conn: original[p] = conn.execute('SELECT msg_id,hex(payload) FROM _zebridge_outbox ORDER BY msg_id').fetchall()
                all_options.append(dict(natsUrl=stack.url,credsPath=str(creds),principal=p,jsDomain='hub',tables=['s2_parent'] if os.environ.get('ZB_OUTBOX_IDLE_TAIL') == '1' else [],ondemandTables=['s2_row'],dbPath=str(db),heartbeatMs=0))
            kinds = (['native']*count if mode == 'native' else ['ts']*count if mode == 'ts' else ['native']*(count//2)+['ts']*(count-count//2))
            for kind in ('native','ts'):
                subset = [o for o,k in zip(all_options,kinds) if k == kind]
                for off in range(0,len(subset),60): workers.append(Worker(root,kind+str(off),kind,subset[off:off+60]))
            # Establish the existing fleet before testing its simultaneous hub
            # reconnect. Starting every replica and replay pump together also
            # benchmarks initial setup and can starve Node's auth handshake.
            for worker in workers:
                await worker.start()
                print(json.dumps({'worker_ready': worker.name, 'clients': len(worker.options)}))
            check(True, f'{count} clients opened with {pending} pending writes each, in {len(workers)} worker processes')
            await asyncio.sleep(13)
            max_get = max((max_window(e['get']) for e in events.values()), default=0)
            max_pub = max((max_window(e['pub']) for e in events.values()), default=0)
            short = {p: len(set(mid for _,mid in events[p]['pub'])) for p in who if len(set(mid for _,mid in events[p]['pub'])) != min(window,pending)}
            if short: print(json.dumps({'window_short': dict(list(short.items())[:10]), 'clients_short': len(short), 'observed_clients': len(events)}))
            if check_window: check(len(events)==count and all(len(set(mid for _,mid in events[p]['pub'])) == min(window,pending) for p in who),
                  f'every client reserves {min(window,pending)} of at most {window} slots, then admits no new ID without a verdict (gets/4s {max_get}, publishes/4s {max_pub})')
            reservations = {}
            for o in all_options:
                with sqlite3.connect(o['dbPath']) as conn:
                    reservations[o['principal']] = set(r[0] for r in conn.execute('SELECT msg_id FROM _zebridge_outbox' + (' WHERE retry_at>0' if check_window else '')))
            if check_window: check(all(len(r)==min(window,pending) for r in reservations.values()), 'reservations are durable and bounded even after lost-verdict retries')
            preserved = True
            attempts = {}
            for o in all_options:
                with sqlite3.connect(o['dbPath']) as conn:
                    preserved &= conn.execute('SELECT msg_id,hex(payload) FROM _zebridge_outbox ORDER BY msg_id').fetchall() == original[o['principal']]
                    attempts[o['principal']] = dict(conn.execute('SELECT msg_id,attempts FROM _zebridge_outbox'))
            check(preserved, 'all pending IDs and payload bytes remain unchanged through retries')
            if check_window: check(all(sum(v.values())>0 for v in attempts.values()), 'automatic recovery progress is persisted in every outbox attempts column')
            clean_restart = await workers[0].close()
            await workers[0].start()
            durable = clean_restart
            for o in workers[0].options:
                with sqlite3.connect(o['dbPath']) as conn:
                    after = dict(conn.execute('SELECT msg_id,attempts FROM _zebridge_outbox'))
                    durable &= all(after.get(mid,-1)>=n for mid,n in attempts[o['principal']].items())
                    durable &= set(r[0] for r in conn.execute('SELECT msg_id FROM _zebridge_outbox' + (' WHERE retry_at>0' if check_window else ''))) == reservations[o['principal']]
            check(durable, 'a worker restart preserves the pending identities, reservations and retry counters')
            await consumers('before hub crash')
            # Crash instead of draining: pending PubAcks and consumers cannot make
            # the outage wait for a graceful shutdown under the 500-client load.
            stack.procs['nats'].kill(); stack.procs['nats'].wait(timeout=15)
            await asyncio.sleep(1)
            stack.spawn('nats',['nats-server','-c',str(root/(stack.name+'.conf'))],dict(os.environ))
            await stack.wait_log('nats','Server is ready')
            await asyncio.sleep(2)
            stack.procs['bridge'].send_signal(signal.SIGCONT)
            resumed = time.monotonic()
            deadline = resumed+budget
            left = [None]*count
            next_report = resumed+30
            while time.monotonic()<deadline:
                states = await asyncio.gather(*(w.state() for w in workers))
                left = [n for s in states for n in s['pending']]
                if all(n==0 for n in left): break
                if any(n and a.get('closed') for s in states for n,a in zip(s['pending'],s.get('active',[]))):
                    print('A pending TS client has a terminally closed connection; recovery cannot complete.')
                    break
                if time.monotonic() >= next_report:
                    active = [a for s in states for a in s.get('active',[]) if a['busy']]
                    print(json.dumps({'remaining':sum(n or 0 for n in left), 'active_passes':len(active),
                                      'oldest_work_seconds':max((w['seconds'] for a in active for w in a['work']),default=0)}))
                    await consumers('draining')
                    next_report += 30
                await asyncio.sleep(1)
            check(all(n==0 for n in left) and not any(s.get('errors') for s in states), f'all {count} outboxes drain automatically after a real hub restart (remaining {sum(n or 0 for n in left)})')
            if not all(n==0 for n in left):
                print(json.dumps(states))
            if os.environ.get('ZB_OUTBOX_IDLE_TAIL') == '1' and mode != 'ts':
                check(all(n>0 for state in states for n in state.get('quick_settling_polls',[])), 'verdicts wake a one-second idle CDC poll and immediately refill the window')
            await consumers('drain finished')
            result = stack.sql("SELECT (SELECT count(*) FROM s2_row)||'|'||(SELECT count(*) FROM audit WHERE tbl='s2_row')||'|'||(SELECT count(*) FROM zebridge_private.receipts)")
            expected = count*pending
            recorded = {tuple(line.split('|',1)) for line in stack.sql("SELECT principal||'|'||message_id FROM zebridge_private.receipts").splitlines()}
            expected_keys = {(p,mid) for p,rows in original.items() for mid,_ in rows}
            check(result == f'{expected}|{expected}|{expected}' and recorded == expected_keys,
                  f'every original ID commits once with one receipt: rows|executions|receipts {result}')
            print(json.dumps({'clients':count,'pending_per_client':pending,'workers':len(workers),'window':window if check_window else None,'cdc_fanout':cdc,'legacy_host_pump':legacy,'consumer_peak':consumer_peak,'max_gets_per_client_4s':max_get,'max_publishes_per_client_4s':max_pub,
                              'drain_seconds':round(time.monotonic()-resumed,3),
                              'total_gets':sum(len(e['get']) for e in events.values()),'total_publishes':sum(len(e['pub']) for e in events.values())}))
            # Teardown/heap scans of hundreds of local handles are not the fleet
            # reconnect under test. Keep them staged like initial setup.
            exits = [await w.close() for w in workers]
            check(all(exits), 'workers exit cleanly; requested native allocation checks pass')
        except BaseException:
            failed += 1
            raise
        finally:
            await asyncio.gather(*(w.close() for w in workers), return_exceptions=True)
            if observer: await observer.close()
            stack.stop()
            if failed:
                import shutil
                dest = pathlib.Path('/tmp') / ('zb-outbox-failure-' + root.name)
                dest.mkdir(exist_ok=True)
                for log in root.glob('*.log'): shutil.copy2(log, dest / log.name)
                print('Failure logs: ' + str(dest))
    print(f'outbox recovery: {passed} passed, {failed} failed')
    return int(failed>0)


if __name__ == '__main__':
    # Match storm/storm.py's worker allowance, without changing system limits.
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    wanted = 65536 if hard == resource.RLIM_INFINITY else min(hard,65536)
    if soft != resource.RLIM_INFINITY and soft < wanted: resource.setrlimit(resource.RLIMIT_NOFILE,(wanted,hard))
    if len(sys.argv)>1 and sys.argv[1]=='--worker': native_worker(sys.argv[2])
    else: raise SystemExit(asyncio.run(main()))
