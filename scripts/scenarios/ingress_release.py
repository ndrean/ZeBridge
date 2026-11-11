#!/usr/bin/env python3
"""Ordered parking/release, restart, rollback and lost-verdict recovery on a private stack.

Raw publishers drive the ordered protocol, including its direct-get/same-ID lease
probe. --fault-bridge additionally interrupts a parked execution AFTER commit,
before verdict publication. No subsequent application write or client reconnect is
used to recover that outcome. This is protocol coverage, not a client poll-loop test.
"""
import argparse
import asyncio
import json
import os
from pathlib import Path
import tempfile
import time
import uuid

import msgpack
import nats
import zb
from recreate_recovery import Stack
from ingress_ordering import ordered_id, verdict, P, C, PRINCIPAL, TENANT

async def until(probe, timeout=15):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        if probe():return
        await asyncio.sleep(.05)
    raise AssertionError('condition not reached')

async def main(args):
    args.log_dir.mkdir(parents=True,exist_ok=True)
    s=Stack(args.log_dir,'ordered_release',120,{'ZB_INGRESS_LANES':'4'},pg_port=int(os.getenv('ZB_TEST_PG_PORT','5432')))
    s.log=lambda label:str(args.log_dir/f'{label}.log')
    nc=None; checks=[]
    def check(label,condition):
        checks.append(dict(check=label,passed=bool(condition)))
        (zb.ok if condition else zb.bad)(label)
        (args.log_dir/'summary.json').write_text(json.dumps(checks,indent=2)+'\n')
        # ZB_CHECK_CONTINUE=1 (a mutant's evidence): record every outcome, fail at the end.
        if not condition and os.getenv('ZB_CHECK_CONTINUE')!='1':raise AssertionError(label)
    try:
        s.start({PRINCIPAL:TENANT})
        s.env['MUTATION_ADMISSION']='1'
        common='tenant_id text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz'
        s.sql(f'CREATE TABLE {P}(id uuid PRIMARY KEY, valid boolean NOT NULL DEFAULT true CHECK(valid),{common}); CREATE TABLE {C}(id uuid PRIMARY KEY,parent_id uuid NOT NULL REFERENCES {P}(id),{common})')
        for table in (P,C):
            s.sql(f"SELECT count(*) FROM zebridge_enable('public.{table}'::regclass,writable=>true,tenant_col=>'tenant_id',version_col=>'updated_at',tombstone_col=>'deleted_at',publication=>'rr_pub',dry_run=>false)")
        s.spawn('admission',[str(s.bridge_bin),'--mutation-ingress'],s.env)
        s.wait_log('admission','mutation admission ready')
        label=s.bridge_up()
        nc=await nats.connect(s.url,user_credentials=s.creds[PRINCIPAL],inbox_prefix=f'_INBOX.{PRINCIPAL}'.encode())
        js=nc.jetstream(domain='hub'); ack=zb.TOPOLOGY['subjects']['mutation_ack_prefix']
        version=s.sql("SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')")
        async def make(table, pred=None, parent=None, valid=True, mid=None):
            rid=str(uuid.uuid4()); mid=mid or ordered_id(table)
            data=dict(id=rid,tenant_id=TENANT,updated_at=version)
            if table==P:data['valid']=valid
            else:data['parent_id']=parent
            wire=msgpack.packb(dict(key={'id':rid},data=data,version=version,client_id='one-client',ordering=1,predecessor=pred))
            subject=zb.subject(zb.TOPOLOGY['subjects']['mutations_prefix'],PRINCIPAL,table)+'.insert'
            sub=await nc.subscribe(f'{ack}.{PRINCIPAL}.{mid}')
            held=await nc.subscribe(f'{ack}.{PRINCIPAL}.{mid}.held')
            await nc.flush()
            return dict(id=mid,row=rid,wire=wire,subject=subject,sub=sub,held=held)
        async def send(w):return await js.publish(w['subject'],w['wire'],headers={'Nats-Msg-Id':w['id']},timeout=10)
        async def outcome(w):return await verdict(w['sub'],w['id'],timeout=20)
        async def hold(w):
            notice=zb.decode((await w['held'].next_msg(timeout=10)).data)
            check('unresolved successor gets a nonterminal held lease',notice['status']=='held' and notice['lease_ms']>0)
            return notice
        async def verdicts_of(w):
            # What the client hears, after `outcome` took the first: VERDICTS keeps one message
            # per subject, so a second copy only replaces the first there and the stream cannot
            # count it. Past two sweeps, the credit's delivery would have landed.
            n,end=1,time.monotonic()+2.5
            while (left:=end-time.monotonic())>0:
                try:await w['sub'].next_msg(timeout=left);n+=1
                except nats.errors.TimeoutError:pass
            return n
        def credit_path(path):
            return s.metrics().get(f'bridge_ingress_credit_verdicts_total{{path="{path}"}}',0)
        def stored(w):return s.sql(f"SELECT count(*) FROM zebridge_private.mutation_parked WHERE message_id='{w['id']}'")=='1'
        async def empty():
            await until(lambda:s.sql('SELECT rows FROM zebridge_private.mutation_park_total')=='0')
            check('park rows, credits and charged total settle to zero',s.sql('SELECT (SELECT count(*) FROM zebridge_private.mutation_parked)+(SELECT count(*) FROM zebridge_private.mutation_park_credit)')=='0')
        # Missing predecessor is durably parked, survives restart, executes exactly once.
        p=await make(P); c=await make(C,p['id'],p['row'])
        await send(c); await hold(c)
        check('parked row contains the immutable original bytes',stored(c) and s.sql(f"SELECT encode(payload,'hex') FROM zebridge_private.mutation_parked WHERE message_id='{c['id']}'")==c['wire'].hex())
        s.bridge_down(label); label=s.bridge_up()
        check('restart keeps the parked successor',stored(c))
        await send(p)
        check('restart releases successor after parent commits',(await outcome(p))['status']=='accepted' and (await outcome(c))['status']=='accepted')
        n=await verdicts_of(c)
        check(f'the released successor gets exactly one verdict, not the live one and its credit\'s (got {n})',n==1)
        replay=await send(c)
        durable=await js.get_msg('VERDICTS',subject=f"{ack}.{PRINCIPAL}.{c['id']}",direct=True)
        check('duplicate-window resend keeps a durable terminal verdict',replay.duplicate and zb.decode(durable.data)['status']=='accepted' and s.sql(f"SELECT count(*) FROM {C} WHERE id='{c['row']}'")=='1')
        await empty()
        # A parent rejected by its own constraint releases the child for normal FK judgment.
        p=await make(P,valid=False); c=await make(C,p['id'],p['row'])
        await send(c); await hold(c); await send(p)
        po,co=await outcome(p),await outcome(c)
        check('rejected parent releases child; 23503 is terminal, not retried',po['status']=='rejected' and co['status']=='rejected' and co['sqlstate']=='23503')
        n=await verdicts_of(c)
        check(f'the released, rejected successor gets exactly one verdict (got {n})',n==1)
        check('both releases ran in the sweeper, and their verdicts came from the credit',credit_path('sweep')>=2)
        await empty()
        # Batch rollback/fallback: reverse arrival; both root and child are in one fetched batch.
        s.bridge_down(label)
        p=await make(P,valid=False); c=await make(C,p['id'],p['row'])
        await send(c); await send(p); label=s.bridge_up()
        po,co=await outcome(p),await outcome(c)
        check('batch rollback and single-write fallback retain predecessor semantics',po['status']=='rejected' and co['status']=='rejected' and co['sqlstate']=='23503')
        check('failure really exercised batch fallback','batch(es) replayed' in Path(s.log(label)).read_text())
        await empty()
        # Expired predecessor is durable non-execution and propagates to the suffix.
        old='zb1-t1-INSERT-one~2Dclient-io_parent-k1-old-v'
        p=await make(P,pred=old); c=await make(C,p['id'],p['row'])
        await send(c); await hold(c); await send(p)
        po,co=await outcome(p),await outcome(c)
        check('PredecessorUnknown propagates without executing either row',po['status']==co['status']=='indeterminate' and po['reason']==co['reason']=='PredecessorUnknown' and s.sql(f"SELECT count(*) FROM {P} WHERE id='{p['row']}'")=='0')
        await empty()
        if args.fault_bridge:
            # The ordinary-batch release: a parked write whose ack was lost is redelivered and
            # meets its committed predecessor in a lane batch, not in the sweeper's replay.
            s.bridge_down(label); s.bridge_bin=args.fault_bridge
            s.env['ZB_TEST_FAULT']='park_before_ack'; label=s.bridge_up()
            p=await make(P); c=await make(C,p['id'],p['row'])
            await send(c)
            await until(lambda:s.procs[label].poll() is not None)
            check('fault fired after the park committed, before its ack',s.procs[label].returncode==70 and 'park_before_ack fired' in Path(s.log(label)).read_text() and stored(c))
            await send(p)
            await asyncio.sleep(11)  # past the 10 s ack wait: both are delivered at the next boot, in one batch
            s.env.pop('ZB_TEST_FAULT'); label=s.bridge_up()
            po,co=await outcome(p),await outcome(c)
            n=await verdicts_of(c)
            check(f'a release in an ordinary lane batch gets exactly one verdict (got {n})',po['status']==co['status']=='accepted' and n==1 and s.sql(f"SELECT count(*) FROM {C} WHERE id='{c['row']}'")=='1')
            check('that release ran in a lane, not the sweeper',credit_path('lane')>=1)
            await empty()
            # Commit a parked child, then crash before publishing its receipt verdict.
            p=await make(P); c=await make(C,p['id'],p['row'])
            await send(c); lease=await hold(c)
            s.bridge_down(label); s.bridge_bin=args.fault_bridge
            s.env['ZB_TEST_FAULT']='ordered_after_commit'; label=s.bridge_up()
            await send(p); await outcome(p)
            await until(lambda:s.procs[label].poll() is not None)
            check('fault fired after parked execution committed',s.procs[label].returncode==70 and 'ordered_after_commit fired' in Path(s.log(label)).read_text())
            check('receipt and business row survived with no parked row',not stored(c) and s.sql(f"SELECT count(*) FROM {C} WHERE id='{c['row']}'")=='1')
            try:await js.get_msg('VERDICTS',subject=f"{ack}.{PRINCIPAL}.{c['id']}",direct=True)
            except nats.js.errors.NotFoundError:missing=True
            else:missing=False
            check('no terminal verdict was published',missing)
            s.env.pop('ZB_TEST_FAULT'); label=s.bridge_up()
            # The publisher remains connected. Wait the lease then do the existing
            # direct-get recovery, with no fresh application write.
            await asyncio.sleep(lease['lease_ms']/1000+.05)
            durable=await js.get_msg('VERDICTS',subject=f"{ack}.{PRINCIPAL}.{c['id']}",direct=True)
            check('lease probe finds recovered verdict without resend or a new write',zb.decode(durable.data)['status']=='accepted')
            await empty()
        failed=[c['check'] for c in checks if not c['passed']]
        if failed:raise SystemExit(f'{len(failed)} of {len(checks)} checks failed: {failed}')
        print(f'{len(checks)}/{len(checks)} clean')
    finally:
        if nc:await nc.close()
        s.stop()

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--log-dir',type=Path,default=Path(tempfile.mkdtemp(prefix='zb-ordered-release-')))
    p.add_argument('--fault-bridge',type=Path)
    asyncio.run(main(p.parse_args()))
