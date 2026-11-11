#!/usr/bin/env python3
"""Parent-before-child commit ordering through admission and parallel ingress lanes.

A single principal publishes the parent first and waits for its durable PubAck. An
AFTER INSERT trigger parks that transaction before commit. Only then is the child
published and acknowledged. With multiple lanes a BEFORE INSERT trigger proves the
child reached a different PostgreSQL session; releasing only that trigger lets the
foreign key run while the parent is uncommitted. No sleep chooses the interleaving.

The safety assertion is that both valid writes eventually apply. The ordered envelope must pass with one or several lanes. The original unprotected
bridge rejects the child with SQLSTATE 23503; -- via ZB_ORDERING_BRIDGE_BIN it remains
a negative control, not a test that blesses the rejection. No client library is
involved: this isolates admission, queue delivery and database execution ordering.

Uses disposable databases, slots and local NATS servers; needs a PostgreSQL admin,
nats-server and the ordinary bridge binary. Logs and summary stay in --log-dir.
"""
import argparse
import asyncio
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid

import msgpack
import nats
import zb
from recreate_recovery import Stack, BRIDGE

P, C = 'io_parent', 'io_child'
PRINCIPAL, TENANT = 'iop', 'iot'
LOCK_CLASS = 1938001

def ordered_id(table, client='one~2Dclient'):
    return f'zb1-t{int(time.time()*1000)}-INSERT-{client}-{table}-k1-{uuid.uuid4().hex}-v'



class Gates:
    def __init__(self, stack):
        self.stack = stack
        self.p = subprocess.Popen([*stack.psql, '-d', stack.name], stdin=subprocess.PIPE,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
        self.p.stdin.write(f'SELECT pg_advisory_lock({LOCK_CLASS},1); SELECT pg_advisory_lock({LOCK_CLASS},2);\n')
        self.p.stdin.flush()

    def pids(self, which, granted=False):
        value = self.stack.sql("SELECT pid FROM pg_locks WHERE locktype='advisory' "
                               f"AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) "
                               f"AND classid={LOCK_CLASS} AND objsubid=2 AND objid={which} "
                               f"AND granted={'true' if granted else 'false'} ORDER BY pid")
        return [int(v) for v in value.splitlines() if v]

    async def wait(self, which, granted=False, timeout=5, required=True):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.p.poll() is not None:
                raise RuntimeError('gate owner exited')
            pids = self.pids(which, granted)
            if pids:
                return pids
            await asyncio.sleep(.025)
        if required:
            raise RuntimeError(f'gate {which} was not reached (granted={granted}); precondition failed')
        return []

    def release(self, which):
        self.p.stdin.write(f'SELECT pg_advisory_unlock({LOCK_CLASS},{which});\n')
        self.p.stdin.flush()

    def close(self):
        if self.p.poll() is None:
            self.p.terminate()
        try:
            self.p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.p.kill()
            self.p.wait()
        self.p.stdin.close()


async def verdict(sub, message_id, timeout=5):
    message = await sub.next_msg(timeout=timeout)
    if message.subject.rsplit('.', 1)[-1] != message_id:
        raise RuntimeError('unexpected verdict identity')
    return zb.decode(message.data)


async def run_case(tmp, lanes, iteration):
    stack = Stack(tmp, f'io{lanes}_{iteration}', 120, {'ZB_INGRESS_LANES': str(lanes)}, pg_port=int(os.getenv('ZB_TEST_PG_PORT','5432')))
    stack.bridge_bin=Path(os.getenv('ZB_ORDERING_BRIDGE_BIN',str(BRIDGE)))
    # Keep ALL private stack evidence inside this run's directory.
    stack.log = lambda label: str(tmp / f'lanes-{lanes}-run-{iteration}-{label}.log')
    nc = other = gates = None
    result = dict(lanes=lanes, iteration=iteration)
    try:
        stack.start({PRINCIPAL: TENANT, 'iop2': 'iot2'})
        stack.env['MUTATION_ADMISSION'] = '1'
        common = 'tenant_id text NOT NULL, last_writer text, updated_at timestamptz NOT NULL DEFAULT now(), deleted_at timestamptz'
        stack.sql(f'CREATE TABLE {P} (id uuid PRIMARY KEY, {common}); '
                  f'CREATE TABLE {C} (id uuid PRIMARY KEY, parent_id uuid NOT NULL REFERENCES {P}(id), {common});')
        for table in (P, C):
            stack.sql(f"SELECT count(*) FROM zebridge_enable('public.{table}'::regclass, writable=>true, tenant_col=>'tenant_id', "
                      "version_col=>'updated_at', tombstone_col=>'deleted_at', tiebreak_col=>'last_writer', publication=>'rr_pub', dry_run=>false)")
        stack.sql(f'CREATE TABLE io_independent (LIKE {P} INCLUDING ALL)')
        stack.sql("SELECT count(*) FROM zebridge_enable('public.io_independent'::regclass, writable=>true, tenant_col=>'tenant_id', version_col=>'updated_at', tombstone_col=>'deleted_at', tiebreak_col=>'last_writer', publication=>'rr_pub', dry_run=>false)")
        stack.sql(f"""CREATE FUNCTION io_hold() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
            PERFORM pg_advisory_xact_lock({LOCK_CLASS}, TG_ARGV[0]::integer); RETURN NEW; END $$;
            CREATE TRIGGER io_parent_hold AFTER INSERT ON {P} FOR EACH ROW EXECUTE FUNCTION io_hold('1');
            CREATE TRIGGER io_child_hold BEFORE INSERT ON {C} FOR EACH ROW EXECUTE FUNCTION io_hold('2');""")
        stack.spawn('admission', [str(BRIDGE), '--mutation-ingress'], stack.env)
        stack.wait_log('admission', 'mutation admission ready')
        bridge = stack.bridge_up()
        stack.wait_log(bridge, f'Starting {lanes} mutation listener lane(s)')
        nc = await nats.connect(stack.url, user_credentials=stack.creds[PRINCIPAL],
                                inbox_prefix=f'_INBOX.{PRINCIPAL}'.encode())
        js = nc.jetstream(domain='hub')
        parent_id, child_id = str(uuid.uuid4()), str(uuid.uuid4())
        parent_msg, child_msg = ordered_id(P), ordered_id(C)
        ack = zb.TOPOLOGY['subjects']['mutation_ack_prefix']
        parent_sub = await nc.subscribe(f'{ack}.{PRINCIPAL}.{parent_msg}')
        child_sub = await nc.subscribe(f'{ack}.{PRINCIPAL}.{child_msg}')
        held_sub = await nc.subscribe(f'{ack}.{PRINCIPAL}.{child_msg}.held')
        await nc.flush()
        version = stack.sql("SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')")
        async def publish(table, rid, message_id, extra):
            data = dict(id=rid, tenant_id=TENANT, updated_at=version, last_writer='one-client', **extra)
            payload = dict(key={'id': rid}, data=data, version=version, client_id='one-client', ordering=1, predecessor=parent_msg if table == C else None)
            subject = zb.subject(zb.TOPOLOGY['subjects']['mutations_prefix'], PRINCIPAL, table) + '.insert'
            return await js.publish(subject, msgpack.packb(payload), headers={'Nats-Msg-Id': message_id}, timeout=5)

        gates = Gates(stack)
        await gates.wait(1, granted=True)
        await gates.wait(2, granted=True)
        parent_ack = await publish(P, parent_id, parent_msg, {})
        parent_pids = await gates.wait(1)
        # AFTER INSERT reached; the parent's row is uncommitted and invisible elsewhere.
        if stack.sql(f"SELECT count(*) FROM {P} WHERE id='{parent_id}'") != '0':
            raise RuntimeError('parent unexpectedly visible before release')
        if lanes > 1:
            other = await nats.connect(stack.url, user_credentials=stack.creds['iop2'], inbox_prefix=b'_INBOX.iop2')
            other_id, other_msg = str(uuid.uuid4()), str(uuid.uuid7())
            other_sub = await other.subscribe(f'{ack}.iop2.{other_msg}')
            await other.flush()
            payload = dict(key={'id': other_id}, data=dict(id=other_id, tenant_id='iot2', updated_at=version), version=version, client_id='other-client')
            await other.jetstream(domain='hub').publish(zb.subject(zb.TOPOLOGY['subjects']['mutations_prefix'], 'iop2', 'io_independent') + '.insert', msgpack.packb(payload), headers={'Nats-Msg-Id': other_msg})
            result['unrelated_verdict_while_parent_held'] = await verdict(other_sub, other_msg)
            result['unrelated_rows_while_parent_held'] = int(stack.sql(f"SELECT count(*) FROM io_independent WHERE id='{other_id}'"))
            if not gates.pids(1) or result['unrelated_verdict_while_parent_held'].get('status') != 'accepted' or result['unrelated_rows_while_parent_held'] != 1:
                raise RuntimeError('unrelated client did not commit while parent stayed held')
        child_ack = await publish(C, child_id, child_msg, {'parent_id': parent_id})
        if parent_ack.stream != child_ack.stream or parent_ack.seq >= child_ack.seq:
            raise RuntimeError('admission did not preserve enqueue order')
        result.update(parent_sequence=parent_ack.seq, child_sequence=child_ack.seq, parent_pids=parent_pids)
        if lanes == 1:
            gates.release(1)
        child_pids = await gates.wait(2, timeout=3, required=lanes == 1)
        parallel = lanes > 1 and bool(child_pids)
        if parallel:
            if set(parent_pids) & set(child_pids) or not gates.pids(1):
                raise RuntimeError('parallel execution barrier not established')
        elif lanes > 1:
            # Three ways the child can wait for its held parent: executed in another session
            # (the original defect, above), parked with a held lease (the receipt-protected
            # exception path), or queued behind the parent in the same lane (affinity: the
            # common path). Evidence for the third: the dispatcher fetched it and one lane's
            # queue holds it while the parent is still held.
            try: notice=zb.decode((await held_sub.next_msg(timeout=2)).data)
            except asyncio.TimeoutError: notice=None
            result['child_held']=notice
            result['parked_while_parent_held']=int(stack.sql(f"SELECT count(*) FROM zebridge_private.mutation_parked WHERE message_id='{child_msg}'"))
            if notice is not None:
                if notice.get('status')!='held' or result['parked_while_parent_held']!=1:
                    raise RuntimeError('successor was not durably parked while parent stayed held')
            else:
                metrics=stack.metrics()
                result['queued_while_parent_held']=sum(metrics[k] for k in metrics if k.startswith('bridge_ingress_queued{'))
                result['fetched_while_parent_held']=metrics['bridge_ingress_fetched_total']
                if not gates.pids(1) or result['parked_while_parent_held']!=0 or result['queued_while_parent_held']<1 or stack.sql(f"SELECT count(*) FROM {C}")!='0':
                    raise RuntimeError('successor was neither parked nor queued behind its held parent')
                result['child_queued_behind_parent']=True
            gates.release(1)
            child_pids = await gates.wait(2)
        result['child_pids'] = child_pids
        result['child_executed_while_parent_held'] = parallel
        gates.release(2)
        if parallel:
            # Read the final child verdict while the parent is provably still held.
            # The current permanent rejection arrives promptly. A future implementation
            # may wait/retry, so absence of a verdict is allowed until the parent releases.
            try:
                early = await verdict(child_sub, child_msg, timeout=1)
            except asyncio.TimeoutError:
                early = None
            if not gates.pids(1):
                raise RuntimeError('parent left its barrier before observation')
            result['child_before_parent_commit'] = early
            gates.release(1)
        else:
            early = None
        result['parent_verdict'] = await verdict(parent_sub, parent_msg)
        result['child_verdict'] = early if early is not None else await verdict(child_sub, child_msg, timeout=15)
        result['parent_rows'] = int(stack.sql(f"SELECT count(*) FROM {P} WHERE id='{parent_id}'"))
        result['child_rows'] = int(stack.sql(f"SELECT count(*) FROM {C} WHERE id='{child_id}'"))
        text = Path(stack.log(bridge)).read_text(errors='replace')
        result['foreign_key_23503_logged'] = '23503' in text
        # Confirm a fresh-ID retry succeeds after the parent committed. This is evidence
        # of the ordering defect, never a substitute for the original write's success.
        if result['child_rows'] == 0:
            retry_id = ordered_id(C)
            retry_sub = await nc.subscribe(f'{ack}.{PRINCIPAL}.{retry_id}')
            await nc.flush()
            await publish(C, child_id, retry_id, {'parent_id': parent_id})
            result['fresh_retry_verdict'] = await verdict(retry_sub, retry_id)
            result['fresh_retry_rows'] = int(stack.sql(f"SELECT count(*) FROM {C} WHERE id='{child_id}'"))
        result['passed'] = (result['parent_verdict'].get('status') == 'accepted' and
                            result['child_verdict'].get('status') == 'accepted' and
                            result['parent_rows'] == result['child_rows'] == 1)
        return result
    finally:
        if gates is not None:
            gates.close()
        if other is not None:
            await other.close()
        if nc is not None:
            await nc.close()
        stack.stop()


async def main(args):
    args.log_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for iteration in range(1, args.repeat + 1):
        for lanes in args.lanes:
            try:
                row = await run_case(args.log_dir, lanes, iteration)
            except Exception as error:
                row = dict(lanes=lanes, iteration=iteration, passed=False, precondition_error=str(error))
            results.append(row)
            print(json.dumps(row), flush=True)
            (zb.ok if row['passed'] else zb.bad)(f'{lanes} lane(s), run {iteration}: both valid writes accepted and committed')
            (args.log_dir / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')
    print(f'Logs: {args.log_dir}', flush=True)
    return verdict_code(results, args.repeat * len(args.lanes))


def known_defect(row):
    """The defect this test pins, exactly: parent accepted, the child executed on another lane while
    the parent was held and rejected by the foreign key (23503), an unrelated client committed meanwhile."""
    child = row.get('child_verdict') or {}
    unrelated = row.get('unrelated_verdict_while_parent_held') or {}
    return (row.get('lanes', 1) > 1 and row.get('child_executed_while_parent_held') is True and
            (row.get('parent_verdict') or {}).get('status') == 'accepted' and row.get('parent_rows') == 1 and
            child.get('status') == 'rejected' and child.get('sqlstate') == '23503' and row.get('child_rows') == 0 and
            unrelated.get('status') == 'accepted' and row.get('unrelated_rows_while_parent_held') == 1)


def verdict_code(results, required):
    """0 fixed (every run passed); 1 the known defect and nothing else (the one-lane control passed,
    every other failure is the exact signature); 2 the harness could not set a case up; 3 any other
    failure: a different product problem, which must not read as the known one."""
    if len(results) != required or any('precondition_error' in r for r in results): return 2
    if all(r['passed'] for r in results): return 0
    control = [r for r in results if r.get('lanes') == 1]
    if control and all(r['passed'] for r in control) and all(r['passed'] or known_defect(r) for r in results): return 1
    return 3


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--lanes', type=int, nargs='+', default=[1, 4])
    parser.add_argument('--repeat', type=int, default=1)
    parser.add_argument('--log-dir', type=Path, default=Path(tempfile.mkdtemp(prefix='zb-ingress-order-')))
    args = parser.parse_args()
    if args.repeat < 1 or any(n < 1 or n > 8 for n in args.lanes):
        parser.error('repeat must be positive and lanes must be between 1 and 8')
    raise SystemExit(asyncio.run(main(args)))
