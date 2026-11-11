#!/usr/bin/env python3
"""Admission-refusal and single-lane rate-limit ordering probes, on private stacks.

Both valid writes must apply. The admission regression sends the
successor before its predecessor's same-ID retry. Rate limiting is a probe,
not assumed broken: debt reservations may preserve order in this interleaving.
"""
import argparse, asyncio, json, os, tempfile, time, uuid
from pathlib import Path
import msgpack, nats
import zb
from recreate_recovery import Stack, BRIDGE
from ingress_ordering import P, C, PRINCIPAL, TENANT, verdict, ordered_id

async def run(tmp, case, repeat):
    stack = Stack(tmp, f'arrival-{case}-{repeat}', 120, {'ZB_INGRESS_LANES':'1'}, pg_port=int(os.getenv('ZB_TEST_PG_PORT','5432')))
    stack.bridge_bin=Path(os.getenv('ZB_ORDERING_BRIDGE_BIN',str(BRIDGE)))
    stack.log = lambda label: str(tmp / f'{case}-{repeat}-{label}.log')
    nc = None
    try:
        stack.start({PRINCIPAL:TENANT})
        stack.env.update(MUTATION_ADMISSION='1')
        if case == 'admission': stack.env['MUTATION_BACKLOG_PER_PRINCIPAL']='1'
        else: stack.env.update(MUTATION_RATE_PER_PRINCIPAL='1', MUTATION_RATE_BURST='1')
        common='tenant_id text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz'
        stack.sql(f'CREATE TABLE {P}(id uuid PRIMARY KEY,{common}); CREATE TABLE {C}(id uuid PRIMARY KEY,parent_id uuid NOT NULL REFERENCES {P}(id),{common})')
        for table in (P,C):
            stack.sql(f"SELECT count(*) FROM zebridge_enable('public.{table}'::regclass, writable=>true, tenant_col=>'tenant_id',version_col=>'updated_at',tombstone_col=>'deleted_at',publication=>'rr_pub',dry_run=>false)")
        stack.spawn('admission',[str(BRIDGE),'--mutation-ingress'],stack.env)
        stack.wait_log('admission','mutation admission ready')
        nc=await nats.connect(stack.url,user_credentials=stack.creds[PRINCIPAL],inbox_prefix=f'_INBOX.{PRINCIPAL}'.encode())
        js=nc.jetstream(domain='hub')
        version=stack.sql("SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')")
        rows=[str(uuid.uuid4()) for _ in range(3)]; ids=[ordered_id(P),ordered_id(P),ordered_id(C)]
        subs=[await nc.subscribe(f"{zb.TOPOLOGY['subjects']['mutation_ack_prefix']}.{PRINCIPAL}.{mid}") for mid in ids]
        held=await nc.subscribe(f"{zb.TOPOLOGY['subjects']['mutation_ack_prefix']}.{PRINCIPAL}.{ids[2]}.held")
        await nc.flush()
        async def send(n):
            table=C if n==2 else P
            data=dict(id=rows[n],tenant_id=TENANT,updated_at=version)
            if n==2:data['parent_id']=rows[1]
            return await js.publish(zb.subject(zb.TOPOLOGY['subjects']['mutations_prefix'],PRINCIPAL,table)+'.insert', msgpack.packb(dict(key={'id':rows[n]},data=data,version=version,client_id='one-client',ordering=1,predecessor=ids[1] if n==2 else None)),headers={'Nats-Msg-Id':ids[n]},timeout=5)
        result=dict(case=case,repeat=repeat)
        if case=='admission':
            await send(0) # fill the one-slot queue before any consumer exists
            try: await send(1)
            except nats.js.errors.APIError as e:
                result['parent_admission_refusal']=dict(code=e.code,err_code=e.err_code,description=e.description)
                if 'maximum' not in e.description.lower() and 'limit' not in e.description.lower():raise
            else:raise RuntimeError('parent was not refused by admission')
            label=stack.bridge_up()
            result['filler_verdict']=await verdict(subs[0],ids[0])
            # A verdict precedes the queue ACK slightly: prove capacity is free.
            admin=await nats.connect(stack.url,user_credentials=stack.bridge_creds)
            try:
                deadline=time.monotonic()+5
                while (await admin.jetstream(domain='hub').stream_info('MUTATIONS')).state.messages:
                    if time.monotonic()>deadline:raise RuntimeError('filler not acknowledged')
                    await asyncio.sleep(.02)
            finally:await admin.close()
            child_ack=await send(2)
            notices=[asyncio.create_task(held.next_msg(timeout=10)),asyncio.create_task(subs[2].next_msg(timeout=10))]
            done,pending=await asyncio.wait(notices,return_when=asyncio.FIRST_COMPLETED)
            for task in pending:task.cancel()
            await asyncio.gather(*pending,return_exceptions=True)
            result['child_before_parent_resend']=zb.decode(next(iter(done)).result().data)
            if result['child_before_parent_resend'].get('status')=='held':
                if stack.sql(f"SELECT count(*) FROM {C}")!='0':raise RuntimeError('held child executed')
            else:result['child_verdict']=result['child_before_parent_resend']
            # Wait until ACK removes the child before retrying the parent under the SAME ID.
            await asyncio.sleep(.1)
            parent_ack=await send(1)

        else:
            label=stack.bridge_up()
            await send(0)
            result['filler_verdict']=await verdict(subs[0],ids[0])
            parent_ack=await send(1)
            deadline=time.monotonic()+2
            while 'rate limited' not in Path(stack.log(label)).read_text(errors='replace'):
                if time.monotonic()>deadline:raise RuntimeError('parent did not hit rate limiter')
                await asyncio.sleep(.02)
            result['parent_rate_limited_before_child']=True
            child_ack=await send(2)
        result['parent_verdict']=await verdict(subs[1],ids[1],timeout=15)
        if 'child_verdict' not in result:result['child_verdict']=await verdict(subs[2],ids[2],timeout=15)
        result.update(parent_sequence=parent_ack.seq,child_sequence=child_ack.seq)
        result['parent_rows']=int(stack.sql(f"SELECT count(*) FROM {P} WHERE id='{rows[1]}'"))
        result['child_rows']=int(stack.sql(f"SELECT count(*) FROM {C} WHERE id='{rows[2]}'"))
        result['passed']=result['parent_rows']==result['child_rows']==1 and result['parent_verdict']['status']==result['child_verdict']['status']=='accepted'
        return result
    finally:
        if nc:await nc.close()
        stack.stop()

async def main(a):
    a.log_dir.mkdir(parents=True,exist_ok=True); results=[]
    for r in range(1,a.repeat+1):
        for case in a.cases:
            try:row=await run(a.log_dir,case,r)
            except Exception as e:row=dict(case=case,repeat=r,passed=False,precondition_error=str(e))
            results.append(row); print(json.dumps(row),flush=True)
            (a.log_dir/'summary.json').write_text(json.dumps(results,indent=2)+'\n')
    return verdict_code(results,a.repeat*len(a.cases))

def known_defect(r):
    """The defect this test pins, exactly: the parent refused by the full queue, then accepted on its
    same-ID resend; its child, admitted first, rejected by the foreign key (23503)."""
    child=r.get('child_verdict') or {}
    return (r.get('case')=='admission' and 'parent_admission_refusal' in r and (r.get('parent_verdict') or {}).get('status')=='accepted'
            and r.get('parent_rows')==1 and child.get('status')=='rejected' and child.get('sqlstate')=='23503' and r.get('child_rows')==0)

def verdict_code(results,required):
    """0 fixed; 1 the known defect and nothing else (the rate-limit probe passed every time, every other
    failure is the exact signature); 2 the harness could not set a case up; 3 any other failure."""
    if len(results)!=required or any('precondition_error' in r for r in results):return 2
    if all(r['passed'] for r in results):return 0
    probe=[r for r in results if r.get('case')=='rate-limit']
    if probe and all(r['passed'] for r in probe) and all(r['passed'] or known_defect(r) for r in results):return 1
    return 3

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--cases',nargs='+',choices=['admission','rate-limit'],default=['admission','rate-limit'])
    p.add_argument('--repeat',type=int,default=3)
    p.add_argument('--log-dir',type=Path,default=Path(tempfile.mkdtemp(prefix='zb-arrival-')))
    raise SystemExit(asyncio.run(main(p.parse_args())))
