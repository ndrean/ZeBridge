#!/usr/bin/env python3
"""The mutation durable's explicit max_ack_pending (2,048) and its startup contract.

A fresh durable is created with the limit. A durable left at the old default (1,000), with writes
both undelivered and delivered-but-unacked, is raised in place before the first fetch: same
durable (creation time, delivery position), every other setting unchanged, every write settled
once. A restart keeps it and changes nothing. A value that is neither stops the boot and is left
as it is; so does an update the bridge may not make (its account refuses CONSUMER.CREATE on the
stream). Raw publishers through real admission, two lanes, a disposable stack.
"""
import argparse, asyncio, base64, dataclasses, hashlib, json, os, re, subprocess, time, uuid
from pathlib import Path
import msgpack, nats, nkeys
import zb
from recreate_recovery import Stack, BRIDGE
from rotate_client_key import b32, b64, user_key

TABLE, PRINCIPAL, TENANT = 'al_rows', 'alp', 'alt'
DURABLE, STREAM = 'bridge_mutations_worker', 'MUTATIONS'
OURS, OLD = 2048, 1000  # config.Nats.mutation_max_ack_pending, the server's default

def denied_creds(path, store):
    """A bridge user signed by the account key itself (not a scoped key, whose template would
    replace these permissions): everything, except creating or updating a MUTATIONS consumer.
    Its limits are explicit: absent, a user may hold no subscription at all."""
    ak = nkeys.from_seed(store['ACCOUNT_SEED'].encode())
    upub, useed = user_key()
    now = int(time.time())
    deny = ['$JS.API.CONSUMER.CREATE.MUTATIONS.>', '$JS.hub.API.CONSUMER.CREATE.MUTATIONS.>',
            '$JS.API.CONSUMER.CREATE.MUTATIONS', '$JS.hub.API.CONSUMER.CREATE.MUTATIONS']
    claims = dict(iat=now, exp=now + 3600, iss=ak.public_key.decode(), name='bridge-denied', sub=upub,
                  nats=dict(pub=dict(allow=['>'], deny=deny), sub=dict(allow=['>']), subs=-1, data=-1, payload=-1, type='user', version=2))
    body = json.dumps(dict(jti='', **claims), separators=(',', ':'))
    claims = dict(jti=b32(hashlib.sha256(body.encode()).digest()), **claims)
    head = b64(b'{"typ":"JWT","alg":"ed25519-nkey"}') + '.' + b64(json.dumps(claims, separators=(',', ':')).encode())
    jwt = head + '.' + b64(ak.sign(head.encode()))
    path.write_text(f'-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER JWT------\n\n'
                    f'-----BEGIN USER NKEY SEED-----\n{useed}\n------END USER NKEY SEED------\n')
    return str(path)

async def main(args):
    args.log_dir.mkdir(parents=True, exist_ok=True)
    s = Stack(args.log_dir, 'acklimit', 120, {'ZB_INGRESS_LANES': '2'}, pg_port=int(os.getenv('ZB_TEST_PG_PORT', '5432')))
    s.log = lambda label: str(args.log_dir / f'{label}.log')
    nc = admin = None
    checks = []
    def check(label, condition, **evidence):
        checks.append(dict(check=label, passed=bool(condition), **evidence))
        (zb.ok if condition else zb.bad)(label + (f'  {json.dumps(evidence)}' if evidence else ''))
        (args.log_dir / 'summary.json').write_text(json.dumps(checks, indent=2) + '\n')
        if not condition: raise AssertionError(label)
    def log_of(label): return open(s.log(label), errors='replace').read()
    def exited(label, timeout=30):
        p = s.procs[label]
        try: p.wait(timeout=timeout)
        except subprocess.TimeoutExpired: return None
        s.procs.pop(label)
        return p.returncode
    try:
        s.start({PRINCIPAL: TENANT})
        s.env['MUTATION_ADMISSION'] = '1'
        s.sql(f'CREATE TABLE {TABLE}(id uuid PRIMARY KEY, tenant_id text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz);')
        s.sql(f"SELECT count(*) FROM zebridge_enable('public.{TABLE}'::regclass, writable=>true, tenant_col=>'tenant_id', version_col=>'updated_at', tombstone_col=>'deleted_at', publication=>'rr_pub', dry_run=>false)")
        s.spawn('admission', [str(BRIDGE), '--mutation-ingress'], s.env)
        s.wait_log('admission', 'mutation admission ready')
        nc = await nats.connect(s.url, user_credentials=s.creds[PRINCIPAL], inbox_prefix=f'_INBOX.{PRINCIPAL}'.encode())
        admin = await nats.connect(s.url, user_credentials=s.bridge_creds)
        ajs = admin.jetstream(domain='hub')
        js = nc.jetstream(domain='hub')
        ack = zb.TOPOLOGY['subjects']['mutation_ack_prefix']
        verdicts = await nc.subscribe(f'{ack}.{PRINCIPAL}.>')
        await nc.flush()
        version = s.sql("SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')")
        seen = {}
        async def publish(n):
            ids, rows = [], []
            for _ in range(n):
                mid, rid = str(uuid.uuid7()), str(uuid.uuid4())
                body = dict(key={'id': rid}, data=dict(id=rid, tenant_id=TENANT, updated_at=version), version=version, client_id='al-client')
                await js.publish(zb.subject(zb.TOPOLOGY['subjects']['mutations_prefix'], PRINCIPAL, TABLE) + '.insert', msgpack.packb(body), headers={'Nats-Msg-Id': mid}, timeout=10)
                ids.append(mid); rows.append(rid)
            return ids, rows
        async def collect(ids, timeout):
            end = time.monotonic() + timeout
            while any(i not in seen for i in ids) and time.monotonic() < end:
                try: m = await verdicts.next_msg(timeout=.5)
                except asyncio.TimeoutError: continue
                tail = m.subject[len(f'{ack}.{PRINCIPAL}.'):]
                if tail.endswith('.held'): continue
                seen.setdefault(tail, []).append(zb.decode(m.data))
            await asyncio.sleep(1.5)  # a duplicate verdict would arrive here
            while True:
                try: m = await verdicts.next_msg(timeout=.3)
                except asyncio.TimeoutError: break
                tail = m.subject[len(f'{ack}.{PRINCIPAL}.'):]
                if not tail.endswith('.held'): seen.setdefault(tail, []).append(zb.decode(m.data))
        def settled_once(ids, rows):
            return all(len(seen.get(i, [])) == 1 and seen[i][0]['status'] == 'accepted' for i in ids) and \
                int(s.sql(f"SELECT count(*) FROM {TABLE} WHERE id IN ({','.join(repr(r) for r in rows)})")) == len(rows)
        async def info(): return await ajs.consumer_info(STREAM, DURABLE)
        def others(cfg):
            d = cfg.as_dict(); d.pop('max_ack_pending', None); return d

        # ── A: a fresh durable is created with the explicit limit ──
        label = s.bridge_up()
        s.wait_log(label, 'Mutation dispatcher: pulling for 2 lane(s)')
        ci = await info()
        ids, rows = await publish(5); await collect(ids, 30)
        check('a fresh durable is created with max_ack_pending 2048, no update made', ci.config.max_ack_pending == OURS and 'raising it' not in log_of(label) and settled_once(ids, rows), max_ack_pending=ci.config.max_ack_pending)
        s.bridge_down(label)

        # ── B: a durable at the old default, with undelivered and delivered-but-unacked writes ──
        old = dataclasses.replace(ci.config, max_ack_pending=OLD)
        await ajs.delete_consumer(STREAM, DURABLE)
        await ajs.add_consumer(STREAM, old)  # what a bridge before this change left behind
        ids, rows = await publish(50)
        taker = await ajs.pull_subscribe_bind(DURABLE, stream=STREAM)
        taken = await taker.fetch(10, timeout=5)  # delivered, never acked: an instance that died
        await taker.unsubscribe()
        before = await info()
        check('staged: the durable is at 1000 with writes pending and unacked', before.config.max_ack_pending == OLD and before.num_ack_pending == len(taken) == 10 and before.num_pending == 40,
              ack_pending=before.num_ack_pending, pending=before.num_pending)
        label = s.bridge_up()
        s.wait_log(label, f"durable '{DURABLE}' now has max_ack_pending {OURS}")
        after = await info()
        check('the boot raised it in place: same durable, every other setting unchanged',
              after.config.max_ack_pending == OURS and after.created == before.created and others(after.config) == others(before.config) and
              'raising it to 2048 in place' in log_of(label),
              created_same=after.created == before.created, changed=sorted(k for k in set(others(before.config)) | set(others(after.config)) if others(before.config).get(k) != others(after.config).get(k)))
        await collect(ids, 60)
        check('every staged write settles exactly once (the 10 unacked by redelivery, the 40 fetched fresh)', settled_once(ids, rows),
              settled=sum(1 for i in ids if len(seen.get(i, [])) == 1), duplicates=sum(1 for i in ids if len(seen.get(i, [])) > 1))
        s.bridge_down(label)

        # ── C: a restart after the update keeps it and changes nothing ──
        label = s.bridge_up()
        s.wait_log(label, 'Mutation dispatcher: pulling for 2 lane(s)')
        ids, rows = await publish(5); await collect(ids, 30)
        ci = await info()
        check('a restart keeps 2048 and makes no update', ci.config.max_ack_pending == OURS and 'raising it' not in log_of(label) and ci.created == before.created and settled_once(ids, rows))
        s.bridge_down(label)

        # ── D: an operator's value is neither overwritten nor run ──
        await ajs.add_consumer(STREAM, dataclasses.replace(ci.config, max_ack_pending=4096))
        label = s.bridge_up(ready="has max_ack_pending 4096, not the old default")
        rc = exited(label)
        ci = await info()
        check('a value of 4096 stops the boot (exit code not 0) with a named mismatch and is left as it is', rc not in (None, 0) and ci.config.max_ack_pending == 4096, exit=rc)

        # ── E: an update the bridge may not make stops the boot; the durable is unchanged ──
        await ajs.add_consumer(STREAM, dataclasses.replace(ci.config, max_ack_pending=OLD))
        before = await info()
        store = dict(l.split('=', 1) for l in (s.tmp / f'{s.name}-id' / 'operator.store').read_text().splitlines() if '=' in l and not l.startswith('#'))
        saved = s.env['NATS_CREDS']
        s.env['NATS_CREDS'] = denied_creds(s.tmp / f'{s.name}-denied.creds', store)
        try:
            label = s.bridge_up(ready='could not raise max_ack_pending')
        finally:
            s.env['NATS_CREDS'] = saved
        rc = exited(label)
        after = await info()
        check('a refused update stops the boot (exit code not 0) and leaves the durable as it was', rc not in (None, 0) and after.config.max_ack_pending == OLD and after.created == before.created and others(after.config) == others(before.config), exit=rc)
        return 0
    finally:
        for c in (nc, admin):
            if c: await c.close()
        s.stop()

if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--log-dir', type=Path, default=Path(os.getenv('TMPDIR', '/tmp')) / 'zb-ingress-ack-limit')
    raise SystemExit(asyncio.run(main(p.parse_args())))
