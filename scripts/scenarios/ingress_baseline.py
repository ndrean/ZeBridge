#!/usr/bin/env python3
"""Ingress-only baseline: real admission, connected raw NATS publishers, private stacks.

Consumer pause preloads a durable backlog without including connection/startup time.
No client-library CPU, optimistic apply or CDC-tail cost is measured. --trace-bin
selects a diagnostic bridge; the ordinary bridge remains the capacity reference.
"""
import argparse, asyncio, heapq, json, math, os, re, subprocess, tempfile, time, uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
import msgpack, nats
import zb
from recreate_recovery import Stack, BRIDGE

TABLE='ib_rows'

def percentile(values,p):
    values=sorted(values)
    return values[max(0,math.ceil(len(values)*p)-1)] if values else None

def cpu(pid):
    s=subprocess.check_output(['ps','-o','time=','-p',str(pid)],text=True).strip()
    fields=[float(x) for x in s.split(':')]
    return sum(x*60**i for i,x in enumerate(reversed(fields)))

def trace_summary(path, predecessors=None):
    dispatch={}; committed={}; groups=defaultdict(list); sizes=[]; fetches=0; deliveries=0; redeliveries=0
    for line in path.read_text(errors='replace').splitlines():
        if m:=re.search(r'IO_BATCH ([DC]) (\d+) (\d+) (\d+)',line):
            if m[1]=='D':
                fetches+=1
                if int(m[4]):sizes.append(int(m[4]))
        if m:=re.search(r'IO_MSG ([DC]) (\d+) (\d+) (\S+) (\d+)',line):
            phase,stamp,lane,mid,n=m.groups(); stamp=int(stamp)
            if match:=re.search(r'-ib_rows-k1-(b\d+w\d+)-',mid):mid=match.group(1)
            if phase=='D':
                deliveries+=1;redeliveries+=int(n)>1
                dispatch.setdefault(mid,(stamp,lane));groups[(stamp,lane)].append(mid)
            else:committed.setdefault(mid,stamp)
    states=Counter()
    for mid,group in dispatch.items():
        match=re.fullmatch(r'b(\d+)w(\d+)',mid)
        if not match:continue
        c,w=map(int,match.groups());pred=predecessors.get(mid) if predecessors is not None else (f'b{c}w{w-1}' if w else None)
        if pred is None:states['no_predecessor']+=1
        elif pred in groups[group]:
            states['same_batch']+=1
            if groups[group].index(pred)>groups[group].index(mid):states['same_batch_reversed']+=1
        elif pred in committed and committed[pred]<group[0]:states['commit_observed_before_dispatch']+=1
        elif pred in dispatch and dispatch[pred][0]<=group[0]:states['dispatched_elsewhere_commit_not_yet_observed']+=1
        else:states['predecessor_not_yet_dispatched']+=1
    # Immediate predecessor and ANY earlier write are different in today's
    # unordered engine: B may commit while older A remains in flight.
    events=[(g[0],1,g,ids) for g,ids in groups.items()]
    events += [(stamp,0,None,[mid]) for mid,stamp in committed.items()]
    active=defaultdict(set); heaps=defaultdict(list); earlier_elsewhere=0
    for stamp,kind,group,ids in sorted(events,key=lambda e:(e[0],e[1])):
        parsed=[]
        for mid in ids:
            if m:=re.fullmatch(r'b(\d+)w(\d+)',mid):parsed.append(tuple(map(int,m.groups())))
        if kind==0:
            for c,w in parsed:active[c].discard(w)
        else:
            for c,w in parsed:
                while heaps[c] and heaps[c][0] not in active[c]:heapq.heappop(heaps[c])
                earlier_elsewhere += bool(heaps[c] and heaps[c][0]<w)
            for c,w in parsed:
                active[c].add(w);heapq.heappush(heaps[c],w)
    return dict(fetches=fetches,deliveries=deliveries,redeliveries=redeliveries,unique_dispatches=len(dispatch),commits=len(committed),batch_sizes=dict(Counter(sizes)),batch_p50=percentile(sizes,.5),batch_p99=percentile(sizes,.99),predecessor_states=dict(states),any_earlier_write_in_other_batch_commit_unobserved=earlier_elsewhere)

async def run_case(root,label,clients,lanes,trace_bin=None,mixed=False,structured_ids=False):
    tmp=root/label;tmp.mkdir(parents=True,exist_ok=True)
    stack=Stack(tmp,label,120,{'ZB_INGRESS_LANES':str(lanes)},pg_port=int(os.getenv('ZB_TEST_PG_PORT','5432')))
    stack.bridge_bin=Path(os.getenv('ZB_BENCH_BRIDGE_BIN',str(BRIDGE)))
    stack.log=lambda name:str(tmp/f'{name}.log')
    connections=[];admin=None; monitor=None; ack_samples=[]; consumer_config=None
    per_client=int(os.getenv('ZB_BENCH_PER_CLIENT') or max(256,8192//clients))
    counts=[per_client]*clients if not mixed else [8192]+[20]*(clients-1)
    statuses=Counter();seen=set();latencies=defaultdict(list);sent={};received={};pending=defaultdict(set);predecessors={};first=None;last=None;t0=0;duplicates=0
    async def on_verdict(m):
        nonlocal first,last,duplicates
        if m.subject.endswith('.held'):return
        at=time.monotonic();mid=m.subject.rsplit('.',1)[-1]
        if structured_ids:mid=re.search(r'-ib_rows-k1-(b\d+w\d+)-',mid).group(1)
        if mid in seen:duplicates+=1;return
        seen.add(mid);received[mid]=at
        v=json.loads(m.data);statuses[v['status']]+=1
        c=int(mid[1:mid.index('w')]);pending[c].discard(int(mid.split('w')[1]));latencies[c].append((at-sent.get(mid,t0))*1000)
        if first is None:first=at
        last=at
    try:
        stack.start({f'ib{i}':'ibt' for i in range(clients)})
        stack.sql('UPDATE zebridge_private.mutation_park_policy SET principal_rows=20000') # benchmark capacity, matching its raised queue budget
        stack.env.update(MUTATION_ADMISSION='1',GENERATION_CHECKPOINT_SECONDS='0',GENERATIONS_ENABLED='0',MUTATION_BACKLOG_PER_PRINCIPAL='20000',MUTATION_MAX_MESSAGE_BYTES='4096',MUTATION_RATE_PER_PRINCIPAL='0')
        if trace_bin:
            stack.bridge_bin=trace_bin;stack.env['ZB_INGRESS_TRACE']='1'
        stack.sql(f'CREATE TABLE {TABLE}(id text PRIMARY KEY,tenant_id text NOT NULL,note text,updated_at timestamptz NOT NULL DEFAULT now(),deleted_at timestamptz)')
        stack.sql(f"SELECT count(*) FROM zebridge_enable('public.{TABLE}'::regclass, writable=>true,tenant_col=>'tenant_id',version_col=>'updated_at',tombstone_col=>'deleted_at',generations=>false,publication=>'rr_pub',dry_run=>false)")
        stack.spawn('admission',[os.getenv('ZB_BENCH_ADMISSION_BIN',str(BRIDGE)),'--mutation-ingress'],stack.env)
        stack.wait_log('admission','mutation admission ready')
        bridge=stack.bridge_up();bridge_pid=stack.procs[bridge].pid
        deadline=time.monotonic()+30
        while Path(stack.log(bridge)).read_text(errors='replace').count('Ready! Pulling mutations from JetStream') < lanes:
            if time.monotonic()>deadline:raise RuntimeError('not every ingress lane reached readiness')
            await asyncio.sleep(.05)
        admin=await nats.connect(stack.url,user_credentials=stack.bridge_creds)
        js=admin.jetstream(domain='hub')
        deadline=time.monotonic()+30
        while True:
            try:await js.consumer_info('MUTATIONS','bridge_mutations_worker');break
            except nats.js.errors.NotFoundError:
                if time.monotonic()>deadline:raise
                await asyncio.sleep(.1)
        paused=await js.pause_consumer('MUTATIONS','bridge_mutations_worker',(datetime.now(timezone.utc)+timedelta(minutes=15)).isoformat())
        if not paused.paused:raise RuntimeError('consumer pause was not acknowledged')
        # Benchmark-only consumer tuning; preserve the durable and dispatcher budget.
        requested_ack = os.getenv('ZB_BENCH_MAX_ACK_PENDING')
        info = await js.consumer_info('MUTATIONS','bridge_mutations_worker')
        if requested_ack:
            info.config.max_ack_pending = int(requested_ack)
            await js.add_consumer('MUTATIONS', config=info.config)
        consumer_config = (await js.consumer_info('MUTATIONS','bridge_mutations_worker')).config.as_dict()
        if requested_ack and consumer_config['max_ack_pending'] != int(requested_ack):
            raise RuntimeError('max_ack_pending update did not take effect')
        # No publisher has sent yet. Wait beyond any fetch parked before pause.
        await asyncio.sleep(.7)
        for i in range(clients):
            nc=await nats.connect(stack.url,user_credentials=stack.creds[f'ib{i}'],inbox_prefix=f'_INBOX.ib{i}'.encode(),pending_size=8<<20)
            await nc.subscribe(f'mutation_ack.ib{i}.>',cb=on_verdict)
            connections.append(nc)
        await asyncio.gather(*(nc.flush() for nc in connections))
        version=stack.sql("SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')")
        created_ms=int(time.time()*1000)
        escaped_version=''.join(ch if ch.isascii() and (ch.isalnum() or ch in '_:') else f'~{ord(ch):02X}' for ch in version)
        async def publish(c,w,live=False):
            mid=f'b{c}w{w}'
            body=dict(key={'id':mid},data=dict(id=mid,tenant_id='ibt',note='x'*64,updated_at=version),version=version,client_id=f'b{c}')
            predecessors[mid]=f'b{c}w{max(pending[c])}' if pending[c] else None
            # 'mix': even clients ordered, odd unordered, both sustained through one dispatcher.
            if os.getenv('ZB_BENCH_ORDERED')=='1' or (os.getenv('ZB_BENCH_ORDERED')=='mix' and c%2==0):
                if not structured_ids:raise ValueError('ordered benchmark requires --structured-ids')
                pred=predecessors[mid]
                body.update(ordering=1,predecessor=f'zb1-t{created_ms}-insert-b{c}-ib_rows-k1-{pred}-{escaped_version}' if pred else None)
            payload=msgpack.packb(body)
            pending[c].add(w)
            if live:sent[mid]=time.monotonic()
            ack=await connections[c].jetstream(domain='hub').publish(f'mutation.ib{c}.{TABLE}.insert',payload,headers={'Nats-Msg-Id':f'zb1-t{created_ms}-insert-b{c}-ib_rows-k1-{mid}-{escaped_version}' if structured_ids else mid},timeout=15)
            return ack
        async def preload(c):
            for start in range(0,counts[c],16):
                await asyncio.gather(*(publish(c,w) for w in range(start,min(start+16,counts[c]))))
        # Bounds outstanding publisher coroutines even at 500 connections.
        sem=asyncio.Semaphore(16)
        async def bounded(c):
            async with sem:await preload(c)
        admission_before=cpu(stack.procs['admission'].pid); admission_started=time.monotonic()
        await asyncio.gather(*(bounded(c) for c in (range(clients) if not mixed else [0])))
        admission_seconds=time.monotonic()-admission_started
        admission_cpu=cpu(stack.procs['admission'].pid)-admission_before
        queued=(await js.stream_info('MUTATIONS')).state.messages
        expected=sum(counts) if not mixed else counts[0]
        if queued!=expected or seen:raise RuntimeError(f'pause/preload failed: {queued=}, {expected=}, verdicts={len(seen)}')
        # Drain PostgreSQL statistics to a fresh reader snapshot before timed execution.
        before_db=json.loads(stack.sql("SELECT row_to_json(s) FROM (SELECT xact_commit,active_time,blk_read_time,blk_write_time FROM pg_stat_database WHERE datname=current_database()) s"))
        async def sample_ack():
            while True:
                info = await js.consumer_info('MUTATIONS','bridge_mutations_worker')
                ack_samples.append(dict(at=time.monotonic(),ack_pending=info.num_ack_pending,pending=info.num_pending,redelivered=info.num_redelivered))
                await asyncio.sleep(.05)
        monitor=asyncio.create_task(sample_ack())
        before_cpu=cpu(bridge_pid);t0=time.monotonic()
        await js.resume_consumer('MUTATIONS','bridge_mutations_worker')
        async def occasional(c):
            for w in range(counts[c]):
                await publish(c,w,True)
                await asyncio.sleep(.2)
        tasks=[asyncio.create_task(occasional(c)) for c in range(1,clients)] if mixed else []
        deadline=t0+300
        while len(seen)<sum(counts):
            for task in tasks:
                if task.done() and task.exception():raise task.exception()
            if time.monotonic()>deadline:raise RuntimeError(f'drain timed out {len(seen)}/{sum(counts)} {statuses}')
            await asyncio.sleep(.01)
        cpu_seconds=cpu(bridge_pid)-before_cpu
        monitor.cancel()
        await asyncio.gather(monitor, return_exceptions=True)
        (tmp/'ack-samples.json').write_text(json.dumps(ack_samples,indent=2)+'\n')
        if tasks:await asyncio.gather(*tasks)
        if statuses!={'accepted':sum(counts)}:raise RuntimeError(f'verdicts: {statuses}')
        if int(stack.sql(f'SELECT count(*) FROM {TABLE}'))!=sum(counts):raise RuntimeError('PostgreSQL count mismatch')
        # The dispatcher's view of the run: parked, renewed, pending and queue high-water marks.
        ingress={k[len('bridge_ingress_'):]:v for k,v in stack.metrics().items() if k.startswith('bridge_ingress_')}
        # Disconnect listener sessions so their final PostgreSQL statistics flush.
        stack.bridge_down(bridge)
        await asyncio.sleep(.3)
        after_db=json.loads(stack.sql("SELECT row_to_json(s) FROM (SELECT xact_commit,active_time,blk_read_time,blk_write_time FROM pg_stat_database WHERE datname=current_database()) s"))
        consumer=await js.consumer_info('MUTATIONS','bridge_mutations_worker')
        result=dict(consumer_config=consumer_config,ack_pending_peak=max((s["ack_pending"] for s in ack_samples),default=0),ack_samples=len(ack_samples),ack_at_limit=sum(s["ack_pending"]>=consumer_config["max_ack_pending"] for s in ack_samples),admission_preload_seconds=admission_seconds,admission_cpu_seconds=admission_cpu,admission_cpu_us_per_write=admission_cpu*1e6/expected,ordered=os.getenv('ZB_BENCH_ORDERED')=='1',traffic=os.getenv('ZB_BENCH_ORDERED','0'),admission_binary=os.getenv('ZB_BENCH_ADMISSION_BIN',str(BRIDGE)),bridge_binary=str(stack.bridge_bin),message_id_format='zb1' if structured_ids else 'legacy',label=label,clients=clients,lanes=lanes,mixed=mixed,instrumented=bool(trace_bin),per_client_counts=counts,accepted=len(seen),seconds=last-t0,writes_s=len(seen)/(last-t0),first_verdict_seconds=first-t0,bridge_cpu_seconds=cpu_seconds,bridge_cpu_us_per_write=cpu_seconds*1e6/len(seen),duplicate_verdicts=duplicates,consumer_redelivered_at_end=consumer.num_redelivered,ingress=ingress,db_stats_delta={k:after_db[k]-before_db[k] for k in before_db},client_latency_ms={str(c):dict(p50=percentile(v,.5),p99=percentile(v,.99)) for c,v in latencies.items()},client_completion_ms=dict(p50=percentile([max(v) for v in latencies.values()],.5),p99=percentile([max(v) for v in latencies.values()],.99)))
        if mixed:
            result['occasional_latency_ms']=dict(p50=percentile([v for c,vs in latencies.items() if c for v in vs],.5),p99=percentile([v for c,vs in latencies.items() if c for v in vs],.99))
            result['backlog_drain_seconds']=max(at for mid,at in received.items() if mid.startswith('b0w'))-t0
            ends=defaultdict(float)
            for mid,at in received.items():
                c=int(mid[1:mid.index('w')]);ends[c]=max(ends[c],(at-t0)*1000)
            result['client_completion_ms']=dict(p50=percentile(list(ends.values()),.5),p99=percentile(list(ends.values()),.99))
        if mixed:
            result['live_writes_without_pending_predecessor']=sum(predecessors[mid] is None for mid in sent)
            (tmp/'creation-predecessors.json').write_text(json.dumps(predecessors)+'\n')
        if os.getenv('ZB_BENCH_ORDERED')=='mix':
            # One window for both classes (t0 to the last verdict of the run), so the two
            # rates add up to writes_s; each class's own finish time is reported beside it.
            for name,keep in (('ordered',0),('unordered',1)):
                ats=[at for mid,at in received.items() if int(mid[1:mid.index('w')])%2==keep]
                result[name+'_writes']=len(ats)
                result[name+'_writes_s']=len(ats)/(last-t0)
                result[name+'_finish_seconds']=max(ats)-t0 if ats else 0
        if trace_bin:result['trace']=trace_summary(Path(stack.log(bridge)),predecessors if mixed else None)
        (tmp/'result.json').write_text(json.dumps(result,indent=2)+'\n')
        return result
    finally:
        if monitor:
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
        for nc in connections:await nc.close()
        if admin:await admin.close()
        stack.stop()

async def main(a):
    a.log_dir.mkdir(parents=True,exist_ok=True)
    results=json.loads((a.log_dir/'results.json').read_text()) if a.resume and (a.log_dir/'results.json').exists() else []
    modes=[None,a.trace_bin] if a.trace_bin else [None]
    for r in range(1,a.rounds+1):
        for clients in a.clients:
            for lanes in a.lanes:
                for binary in (modes if r%2 else list(reversed(modes))):
                    label=f'r{r}-c{clients}-l{lanes}-'+('trace' if binary else 'plain')
                    if a.mixed:label+='-mixed'
                    if a.structured_ids:label+='-zb1'
                    if any(r['label']==label for r in results):continue
                    old=a.log_dir/label
                    if old.exists():old.rename(a.log_dir/(label+'-discarded-'+str(time.time_ns())))
                    print(f'START {label}',flush=True)
                    try:result=await run_case(a.log_dir,label,clients,lanes,binary,a.mixed,a.structured_ids)
                    except Exception as e:
                        (a.log_dir/'failure.txt').write_text(f'{label}: {type(e).__name__}: {e}\n');raise
                    results.append(result)
                    print(json.dumps({k:v for k,v in result.items() if k not in ('per_client_counts','client_latency_ms')}),flush=True)
                    (a.log_dir/'results.json').write_text(json.dumps(results,indent=2)+'\n')
    return 0

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--clients',nargs='+',type=int,default=[1,10,100,500]);p.add_argument('--lanes',nargs='+',type=int,default=[1,8]);p.add_argument('--rounds',type=int,default=3)
    p.add_argument('--structured-ids',action='store_true');p.add_argument('--resume',action='store_true');p.add_argument('--trace-bin',type=Path);p.add_argument('--mixed',action='store_true');p.add_argument('--log-dir',type=Path,required=True)
    args=p.parse_args()
    if args.rounds<1 or any(n<1 for n in args.clients) or any(n<1 or n>8 for n in args.lanes):p.error('positive client/round counts and 1–8 lanes are required')
    if args.mixed and any(n<2 for n in args.clients):p.error('mixed traffic needs at least two clients')
    raise SystemExit(asyncio.run(main(args)))
