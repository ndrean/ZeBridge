#!/usr/bin/env python3
"""Stage-1 ordered ingress SQL, real PostgreSQL sessions, no NATS or bridge.

Renders the current init templates into a disposable database. Tests run as the
writer, administration/observations as the database owner. Gates are real advisory
locks in an application INSERT trigger; pg_blocking_pids proves the interleaving.
No sleep chooses which worker runs first. --hold-seconds only measures contention
AFTER the blocked sessions have been observed. Logs and JSON stay in --log-dir.
Requires psql and a local PostgreSQL administrator; never changes the shared DB.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import threading
import time
import uuid
from urllib.parse import urlsplit, urlunsplit

ROOT = Path(__file__).resolve().parents[2]


def lit(s):
    return "'" + str(s).replace("'", "''") + "'"


class Session:
    def __init__(self, url, name, logdir, role=None):
        self.name = name
        self.err = (logdir / f'{name}.stderr.log').open('w')
        self.p = subprocess.Popen(['psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', url],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.err, text=True, bufsize=1)
        self.run(f"SET application_name={lit(name)}; SET statement_timeout='20s'; SET lock_timeout='15s';")
        if role:
            self.run(f'SET ROLE "{role}";')

    def run(self, sql):
        marker = 'done_' + uuid.uuid4().hex
        with Path(self.err.name).with_suffix('.sql.log').open('a') as log:
            log.write(sql + '\n')
        self.p.stdin.write(sql + ';\n\\echo ' + marker + '\n')
        self.p.stdin.flush()
        lines = []
        while True:
            line = self.p.stdout.readline()
            if not line:
                raise RuntimeError(f'{self.name}: psql exited; see {self.err.name}')
            if line.strip() == marker:
                return '\n'.join(lines).strip()
            lines.append(line.rstrip())

    def close(self):
        if self.err.closed:
            return
        if self.p.poll() is None:
            self.p.stdin.close()
            try:
                self.p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.p.kill()
                self.p.wait()
        self.p.stdout.close()
        self.err.close()


class Suite:
    def __init__(self, args):
        self.args = args
        self.logdir = Path(args.log_dir)
        self.logdir.mkdir(parents=True, exist_ok=True)
        self.name = 'zb_park_' + uuid.uuid4().hex[:10]
        self.writer, self.reader = self.name + '_w', self.name + '_r'
        parts = urlsplit(args.admin_url)
        self.url = urlunsplit(parts._replace(path='/' + self.name))
        self.sessions = []
        self.checks = []
        self.measure = {}
        self.clock = int(time.time() * 1000) - 10000
        self.admin = self.session(args.admin_url, 'admin')
        self.db = None

    def session(self, url=None, name=None, writer=False):
        s = Session(url or self.url, name or 's' + str(len(self.sessions)), self.logdir, self.writer if writer else None)
        self.sessions.append(s)
        return s

    def check(self, label, condition):
        self.checks.append({'check': label, 'passed': bool(condition)})
        print(('  ✓ ' if condition else '  ✗ ') + label, flush=True)
        if not condition:
            raise AssertionError(label)

    def init(self):
        self.admin.run(f'CREATE DATABASE "{self.name}";')
        self.db = self.session(name='observe')
        values = dict(POSTGRES_READER_USER=self.reader, POSTGRES_READER_PASSWORD='scratch',
                      POSTGRES_WRITER_USER=self.writer, POSTGRES_WRITER_PASSWORD='scratch',
                      MUTATION_MAX_DELIVER=re.search(r'pub const mutation_max_deliver: i32 = ([0-9]+);',(ROOT/'src/config.zig').read_text())[1], TARGET_DB=self.name, MAX_TENANT_BYTES='92', OPEN_TENANT='public', LEAF_PREFIX='lf-', RECEIPT_COMPLETION_GUARD='true')
        source = '\n'.join((ROOT / p).read_text() for p in ('init.core.template.sql', 'init.write.template.sql'))
        unknown = set(re.findall(r'\$\{([A-Z_]+)\}', source)) - values.keys()
        if unknown:
            raise RuntimeError(f'unknown template variables: {unknown}')
        self.rendered = re.sub(r'\$\{([A-Z_]+)\}', lambda m: values[m[1]], source)
        old_stage1 = getattr(self.args, "upgrade_write_sql", None)
        if self.args.upgrade_from_head or old_stage1:
            old = '\n'.join(subprocess.run(['git','show',f'HEAD:{p}'],cwd=ROOT,capture_output=True,text=True,check=True).stdout
                            for p in ('init.core.template.sql','init.write.template.sql'))
            if old_stage1:
                old = (ROOT/'init.core.template.sql').read_text()+'\n'+Path(old_stage1).read_text()
            self.db.run(re.sub(r'\$\{([A-Z_]+)\}',lambda m: values[m[1]],old))
            self.db.run('SELECT * FROM zebridge_private.maintain_receipts();')
            self.db.run('BEGIN;'+self.receipt(self.mid('upgrade_existing'))+'COMMIT;')
            if old_stage1:
                self.db.run(self.park(self.mid('upgrade_parked'),self.mid('upgrade_missing')))
        lookup = "to_regprocedure('zebridge_private.lookup_ordered_receipts(text[],text[],bytea[],bytea[],boolean[],text[],text)')"
        shape = lambda: self.db.run(f"SELECT coalesce((SELECT oid::text||','||('released'=ANY(proargnames))::text FROM pg_proc WHERE oid={lookup}),'absent')")
        before = shape() if old_stage1 else None
        if old_stage1:
            self.check(f'the previous ordered lookup is installed, without released ({before})', before.endswith(',false'))
        self.db.run(self.rendered)
        if old_stage1:
            first = shape()
            self.check('the upgrade replaced it once: a new function with released', first.endswith(',true') and first.split(',')[0] != before.split(',')[0])
            self.db.run(self.rendered)
            self.check('a second apply keeps the same function (not dropped again)', shape() == first)
            self.check('the writer may still call it', self.db.run(f"SELECT has_function_privilege('{self.writer}',{lookup},'execute')") == 't')
        self.db.run('SELECT * FROM zebridge_private.maintain_receipts();')
        if self.args.upgrade_from_head or old_stage1:
            self.check('upgrade preserves the previous receipt and its fingerprint',
                       self.db.run(self.state(self.mid('upgrade_existing'),None))=='duplicate')
        if old_stage1:
            self.check('stage-1 parked bytes and counters survive credits migration',self.db.run(self.state(self.mid('upgrade_parked'),self.mid('upgrade_missing')))=='waiting')
            self.balanced('stage-1 migration preserves charged usage')
            self.db.run('BEGIN;'+self.receipt(self.mid('upgrade_missing'))+self.release(self.mid('upgrade_parked'),self.mid('upgrade_missing'))+self.record(self.mid('upgrade_parked'))+'COMMIT;')
            self.check('migrated parked row releases into a credit',self.db.run('SELECT count(*) FROM zebridge_private.mutation_park_credit')=='1')
            self.deliver_all()
            self.db.run('SELECT zebridge_private.settle_park_credits();')
            self.balanced('migrated credit settles its original charge')
        self.db.run(f'CREATE TABLE public.park_effect(id text PRIMARY KEY); GRANT ALL ON public.park_effect TO "{self.writer}";')
        self.w = self.session(name='writer', writer=True)
        self.check('writer has functions but no direct park-table access', self.db.run(
            f"SELECT has_function_privilege('{self.writer}','zebridge_private.park_mutation(text,text,bytea,bytea,text,jsonb)','execute') "
            f"AND NOT has_table_privilege('{self.writer}','zebridge_private.mutation_parked','insert');") == 't')

    def mid(self, tag, ms=None):
        return f'zb1-t{self.clock if ms is None else ms}-INSERT-client-tab-k1-{tag}-v'

    def state(self, mid, pred, principal='alice', payload='body'):
        return f"SELECT status FROM zebridge_private.ordered_mutation_state({lit(principal)},{lit(mid)},'route'::bytea,{lit(payload)}::bytea,{lit(pred) if pred else 'NULL'});"

    def park(self, mid, pred, principal='alice', payload='body'):
        return f"SELECT zebridge_private.park_mutation({lit(principal)},{lit(mid)},'route'::bytea,{lit(payload)}::bytea,{lit(pred)},'{{}}');"

    def take(self, mid, principal='alice'):
        return f"SELECT status FROM zebridge_private.take_parked_mutation({lit(principal)},{lit(mid)});"

    def release(self, mid, pred, principal='alice', payload='body'):
        """What the bridge runs for one ordered write: its receipt lock, then the ordered
        lookup, which decides, releases a parked row into a credit and claims, in one call."""
        pr, m, data = lit(principal), lit(mid), lit(payload)
        return (f"SELECT zebridge_private.lock_receipts(ARRAY[{pr}],ARRAY[{m}]);"
                f"SELECT status FROM zebridge_private.lookup_ordered_receipts(ARRAY[{pr}],ARRAY[{m}],ARRAY['route'::bytea],ARRAY[{data}::bytea],ARRAY[true],ARRAY[{lit(pred) if pred else 'NULL'}]::text[],'pub');")

    def record(self, mid, principal='alice', status='accepted', reason='', payload='body'):
        """The receipt of a write `release` claimed (no second claim), and the guard closed."""
        sql = self.receipt(mid, principal=principal, status=status, reason=reason, payload=payload)
        return sql[sql.index('SELECT zebridge_private.record_receipts'):]

    def credits_of(self, mid):
        return int(self.db.run(f'SELECT count(*) FROM zebridge_private.mutation_park_credit WHERE message_id={lit(mid)}'))

    def owed_verdict(self, mid):
        """The verdict a credit owes for `mid` (pending_park_verdicts: what the sweeper publishes), or ''."""
        return self.db.run(f"SELECT outcome->>'status' FROM zebridge_private.pending_park_verdicts('pub') WHERE message_id={lit(mid)}")

    def deliver_all(self):
        """The sweeper's PubAck: each owed verdict marked delivered, so its credit may settle."""
        return self.db.run("SELECT count(*) FROM (SELECT zebridge_private.park_verdict_delivered(credit_id) FROM zebridge_private.pending_park_verdicts('pub')) d")  # void: count rows

    def receipt(self, mid, principal='alice', status='accepted', reason='', payload='body'):
        pr, mid, data = lit(principal), lit(mid), lit(payload)
        result = lit(json.dumps({'status': status, 'reason': reason}))
        return (f"SELECT zebridge_private.lock_receipts(ARRAY[{pr}],ARRAY[{mid}]);"
                f"SELECT status FROM zebridge_private.lookup_receipts(ARRAY[{pr}],ARRAY[{mid}],ARRAY['route'::bytea],ARRAY[{data}::bytea]);"
                f"SELECT zebridge_private.record_receipts(ARRAY[{pr}],ARRAY[{mid}],ARRAY[sha256(int4send(5)||'route'::bytea||{data}::bytea)],ARRAY[{result}::jsonb]);"
                "SELECT zebridge_private.close_guard();")

    def balanced(self, label):
        result = self.db.run("WITH charged AS (SELECT principal, charged_bytes FROM zebridge_private.mutation_parked UNION ALL SELECT principal, charged_bytes FROM zebridge_private.mutation_park_credit) SELECT (SELECT rows FROM zebridge_private.mutation_park_total) = count(*) "
            "AND (SELECT bytes FROM zebridge_private.mutation_park_total) = coalesce(sum(charged_bytes),0) "
            "AND NOT EXISTS (SELECT 1 FROM zebridge_private.mutation_park_quota q WHERE "
            "q.rows <> (SELECT count(*) FROM charged p WHERE p.principal=q.principal) OR "
            "q.bytes <> (SELECT coalesce(sum(charged_bytes),0) FROM charged p WHERE p.principal=q.principal)) "
            "FROM charged;")
        self.check(label, result == 't')

    def reset(self):
        # Disposable DB only. Preserve monotone receipt floor; IDs stay ahead of it.
        self.db.run('TRUNCATE zebridge_private.mutation_park_credit, zebridge_private.mutation_parked, zebridge_private.mutation_park_quota, public.park_effect; '
                    'UPDATE zebridge_private.mutation_park_total SET rows=0,bytes=0; '
                    'UPDATE zebridge_private.mutation_park_policy SET max_rows=1000000,max_bytes=1073741824,principal_rows=1000,principal_bytes=67108864;')

    def blocked(self, name, timeout=8):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            r = self.db.run(f"SELECT cardinality(pg_blocking_pids(pid))>0 FROM pg_stat_activity WHERE application_name={lit(name)};")
            if r == 't':
                return True
            time.sleep(.02)
        raise RuntimeError(f'precondition: {name} did not block')

    def sweep_cost_and_election(self):
        self.measure['jit_available']=self.db.run('SELECT pg_jit_available()')=='t'
        # Include the first cold execution, with the caller's JIT thresholds set to
        # PostgreSQL defaults. Function-local settings must shield the internal recursive plan.
        self.w.run('SET jit=on; SET jit_above_cost=100000; SET jit_inline_above_cost=500000; SET jit_optimize_above_cost=500000;')
        self.check('park state has a one-row planner estimate',self.db.run("SELECT prorows=1 FROM pg_proc WHERE oid='zebridge_private.ordered_mutation_state(text,text,bytea,bytea,text)'::regprocedure")=='t')
        self.check('hot park functions disable JIT locally',self.db.run("SELECT bool_and('jit=off'=ANY(proconfig)) FROM pg_proc WHERE pronamespace='zebridge_private'::regnamespace AND proname IN ('parked_mutations_ready','pending_park_verdicts','lookup_ordered_receipts')")=='t')
        times=[]
        # Outer EXPLAIN captures server execution time, excluding psql pipe noise.
        for _ in range(5):
            plan=json.loads(self.w.run("EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM zebridge_private.parked_mutations_ready('empty',256)"))[0]
            times.append(plan['Execution Time'])
        self.measure['empty_sweep_ms']=times
        self.check('empty sweep stays below 50 ms, including cold execution',max(times)<50)
        self.w.run('RESET jit; RESET jit_above_cost; RESET jit_inline_above_cost; RESET jit_optimize_above_cost;')
        self.check('empty table does not elect a sweeper',self.w.run("SELECT zebridge_private.try_park_sweeper('pub')")=='f')
        self.w.run(self.park(self.mid('election'),self.mid('election_pred')))
        one=self.session(name='elected',writer=True); two=self.session(name='contender',writer=True)
        self.check('first lane wins sweeper election',one.run("SELECT zebridge_private.try_park_sweeper('pub')")=='t')
        one.run('BEGIN; COMMIT;')
        self.check('election survives application transaction boundaries',two.run("SELECT zebridge_private.try_park_sweeper('pub')")=='f')
        self.check('another publication elects independently',two.run("SELECT zebridge_private.try_park_sweeper('other')")=='t')
        two.run("SELECT zebridge_private.leave_park_sweeper('other')")
        one.close()
        self.check('connection loss releases sweeper election',two.run("SELECT zebridge_private.try_park_sweeper('pub')")=='t')
        two.run("SELECT zebridge_private.leave_park_sweeper('pub')")
        self.reset()

    def basic(self):
        a, p = self.mid('basic'), self.mid('missing')
        self.check('unresolved predecessor parks', self.w.run(self.park(a, p)) == 'parked')
        self.check('identical resend is idempotent', self.w.run(self.park(a, p)) == 'parked')
        self.check('conflicting payload does not overwrite parked bytes', self.w.run(self.park(a, p, payload='different')) == 'conflict')
        self.check('conflicting predecessor does not overwrite a parked link', self.w.run(self.park(a, self.mid('other'))) == 'conflict')
        self.balanced('one park charged once, exact bytes and principal counters')
        self.check('one durable row after resends', self.db.run('SELECT count(*) FROM zebridge_private.mutation_parked') == '1')
        self.db.run(self.rendered)
        self.balanced('reapplying init SQL preserves nonempty park counters')
        self.w.run('BEGIN;' + self.park(self.mid('rollback'), p) + 'ROLLBACK;')
        self.check('rollback leaves no parked row', self.db.run('SELECT count(*) FROM zebridge_private.mutation_parked') == '1')
        self.w.run('BEGIN;' + self.receipt(p) + 'COMMIT;')
        self.check('committed predecessor releases successor', self.w.run(self.state(a, p)) == 'ready')
        doomed = self.session(name='missing_receipt', writer=True)
        try:
            doomed.run('BEGIN;' + self.release(a, p) + "INSERT INTO park_effect VALUES ('must_rollback'); COMMIT;")
            refused = False
        except RuntimeError:
            refused = True
        self.check('release without receipt cannot commit even with application changes', refused and 'park release committed without its receipt' in Path(doomed.err.name).read_text())
        self.check('failed release rolls business change back', self.db.run('SELECT count(*) FROM park_effect') == '0')
        self.balanced('failed release restores park and all counters')
        self.w.run('BEGIN;' + self.release(a, p) + self.record(a) + "INSERT INTO park_effect VALUES ('rollback'); ROLLBACK;")
        self.check('rollback of receipt and application keeps parked retry', self.w.run(self.state(a, p)) == 'ready')
        self.check('a rolled-back release leaves the write parked with no delivery credit',
                   self.db.run(f'SELECT count(*) FROM zebridge_private.mutation_parked WHERE message_id={lit(a)}') == '1' and self.credits_of(a) == 0)
        self.w.run('BEGIN;' + self.release(a, p) + self.record(a) + "INSERT INTO park_effect VALUES ('committed'); COMMIT;")
        self.check('the committed retry creates exactly one credit, owing the receipt\'s outcome',
                   self.credits_of(a) == 1 and self.owed_verdict(a) == 'accepted')
        self.check('a credit owing its verdict does not settle before delivery', self.w.run('SELECT zebridge_private.settle_park_credits();') == '0')
        self.check('post-commit resend returns duplicate without reparking', self.w.run(self.park(a, p)) == 'duplicate')
        self.check('post-commit conflicting resend stays conflict', self.w.run(self.park(a, p, payload='changed')) == 'conflict')
        self.balanced('successful release decrements quotas atomically')
        # Known non-execution outcome propagates; malformed/unknown receipts do not.
        for tag, status, reason, expected in [('unknown','indeterminate','PredecessorUnknown','predecessor_unknown'),
                                             ('bad','unexpected','','unreadable_predecessor'),
                                             ('refused','rejected','','ready')]:
            pred = self.mid(tag)
            self.w.run('BEGIN;' + self.receipt(pred,status=status,reason=reason) + 'COMMIT;')
            self.check(f'predecessor {status}/{reason} -> {expected}', self.w.run(self.state(self.mid('s'+tag), pred)) == expected)
        old = self.mid('old', 1000)
        self.check('expired predecessor is unknown, never ready', self.w.run(self.state(self.mid('fresh'), old)) == 'predecessor_unknown')
        self.check('expired successor takes precedence', self.w.run(self.state(old, self.mid('bad'))) == 'expired')
        self.check('cross-client predecessor rejected', self.w.run(self.state(self.mid('cross'), p.replace('-client-', '-other-'))) == 'invalid_predecessor')
        self.check('own receipt wins before predecessor validation', self.w.run(self.state(p, p)) == 'duplicate')  # own receipt wins
        self.check('unsettled self dependency rejected', self.w.run(self.state(self.mid('self'), self.mid('self'))) == 'invalid_predecessor')
        # A process dying mid-release leaves BOTH its parked bytes and its quota.
        crash_id, crash_pred = self.mid('crash'), self.mid('crash_pred')
        self.w.run(self.park(crash_id,crash_pred))
        self.w.run('BEGIN;'+self.receipt(crash_pred)+'COMMIT;')
        dying = self.session(name='crash_release',writer=True)
        dying.run('BEGIN;'+self.release(crash_id,crash_pred)+"INSERT INTO park_effect VALUES ('crash');")
        dying.close()
        self.check('connection loss rolls back partial release and business row',
                   self.w.run(self.state(crash_id,crash_pred))=='ready' and self.db.run("SELECT count(*) FROM park_effect WHERE id='crash'")=='0')
        self.balanced('connection loss restores park counters')
        self.check('connection loss leaves no delivery credit', self.credits_of(crash_id) == 0)
        recovered = self.session(name='reopened_release',writer=True)
        recovered.run('BEGIN;'+self.release(crash_id,crash_pred)+self.record(crash_id)+'COMMIT;')
        self.check('new session completes parked write after crash',recovered.run(self.park(crash_id,crash_pred))=='duplicate' and self.credits_of(crash_id)==1 and self.owed_verdict(crash_id)=='accepted')
        self.balanced('reopened release balances counters')
        # Expired predecessor while S is admissible: stage 1 exposes the decision;
        # the caller records the nonexecution outcome through the ordinary guard.
        unknown_id, unknown_pred = self.mid('propagate'), self.mid('never_arrived')
        self.w.run(self.park(unknown_id,unknown_pred))
        self.w.run('BEGIN;'+self.receipt(unknown_pred,status='indeterminate',reason='PredecessorUnknown')+'COMMIT;')
        self.check('parked suffix discovers durable nonexecution',self.w.run(self.state(unknown_id,unknown_pred))=='predecessor_unknown')
        self.check('the ordered lookup itself decides the nonexecution',self.w.run('BEGIN;'+self.release(unknown_id,unknown_pred))=='predecessor_unknown')
        self.w.run(self.record(unknown_id,status='indeterminate',reason='PredecessorUnknown')+'COMMIT;')
        self.check('its credit owes the indeterminate receipt',self.owed_verdict(unknown_id)=='indeterminate')
        self.check('nonexecution receipt propagates to next successor',self.w.run(self.state(self.mid('next'),unknown_id))=='predecessor_unknown')
        self.balanced('nonexecution release balances quotas')
        self.reset()

    def ordered_batch(self):
        def finish(mid, **kwargs):
            sql=self.receipt(mid, **kwargs)
            return sql[sql.index('SELECT zebridge_private.record_receipts'):].replace('SELECT zebridge_private.close_guard();','')
        def gate(ids,preds):
            count=len(ids)
            principals="ARRAY["+','.join("'alice'" for _ in ids)+"]"
            names="ARRAY["+','.join(map(lit,ids))+"]"
            return (f"SELECT zebridge_private.lock_receipts({principals},{names});"
                    f"SELECT string_agg(status,',' ORDER BY ord) FROM zebridge_private.lookup_ordered_receipts({principals},{names},"
                    "ARRAY["+','.join("'route'::bytea" for _ in ids)+"],ARRAY["+','.join("'body'::bytea" for _ in ids)+"],"
                    "ARRAY["+','.join('true' for _ in ids)+"],ARRAY["+','.join(lit(x) if x else 'NULL' for x in preds)+"]::text[]);")
        a,b,c=[self.mid('gate_'+x) for x in ('a','b','c')]
        self.w.run(self.park(b,a)); self.w.run(self.park(c,b))
        statuses=self.w.run('BEGIN;'+gate([a,b,c],[None,a,b])).splitlines()[-1]
        self.check('ordered batch claims a whole parked suffix in predecessor order',statuses=='claimed,claimed,claimed')
        self.check('release creates credits without reducing charged counters',self.db.run('SELECT rows FROM zebridge_private.mutation_park_total')=='2')
        self.w.run('ROLLBACK;')
        self.check('batch rollback restores every parked successor',self.db.run('SELECT count(*) FROM zebridge_private.mutation_parked')=='2')
        self.balanced('ordered batch rollback preserves exact quotas')
        self.w.run('BEGIN;'+gate([a,b,c],[None,a,b])+''.join(finish(x) for x in (a,b,c))+'SELECT zebridge_private.close_guard(); COMMIT;')
        self.check('ordered batch commits all receipts and releases all parks',self.db.run('SELECT count(*) FROM zebridge_private.mutation_parked')=='0')
        self.check('one credit per released successor survives commit',self.db.run('SELECT count(*) FROM zebridge_private.mutation_park_credit')=='2')
        self.check('settlement cannot discard an unpublished terminal verdict',self.w.run('SELECT zebridge_private.settle_park_credits();')=='0')
        self.check('verdict delivery is publication scoped',self.w.run("SELECT count(*) FROM zebridge_private.pending_park_verdicts('different')")=='0')
        self.check('released verdicts are durable',self.w.run("SELECT count(*) FROM zebridge_private.pending_park_verdicts('')")=='2')
        self.w.run("SELECT zebridge_private.park_verdict_delivered(credit_id) FROM zebridge_private.pending_park_verdicts(''); SELECT zebridge_private.settle_park_credits();")
        self.check('acknowledged verdict credits settle exactly once',self.w.run('SELECT zebridge_private.settle_park_credits();')=='0')
        self.balanced('ordered batch settlement balances counters')
        missing,waiting=self.mid('gate_missing'),self.mid('gate_waiting')
        self.w.run('BEGIN;'+gate([waiting],[missing])+'COMMIT;')
        self.check('waiting decision does not claim or create a receipt',self.db.run(f"SELECT count(*) FROM zebridge_private.receipts WHERE message_id={lit(waiting)}")=='0')
        self.w.run(self.park(waiting,missing))
        changed=self.w.run(f"BEGIN; SELECT zebridge_private.lock_receipts(ARRAY['alice'],ARRAY[{lit(waiting)}]); SELECT status FROM zebridge_private.lookup_receipts(ARRAY['alice'],ARRAY[{lit(waiting)}],ARRAY['route'::bytea],ARRAY['changed'::bytea]); ROLLBACK;")
        self.check('legacy envelope cannot overwrite an ordered parked fingerprint',changed.splitlines()[-1]=='conflict')
        old=self.mid('gate_old',1000); u,v=self.mid('gate_unknown'),self.mid('gate_unknown_suffix')
        statuses=self.w.run('BEGIN;'+gate([u,v],[old,u])).splitlines()[-1]
        self.check('unknown predecessor propagates inside the same batch',statuses=='predecessor_unknown,predecessor_unknown')
        self.w.run(finish(u,status='indeterminate',reason='PredecessorUnknown')+finish(v,status='indeterminate',reason='PredecessorUnknown')+'SELECT zebridge_private.close_guard(); COMMIT;')
        self.reset()

    def unordered_plan_count(self):
        # Optional integration regression: requires PostgreSQL's auto_explain module.
        # Count executed nested receipt reads, not source tokens or estimated costs.
        self.db.run("LOAD 'auto_explain'; SET client_min_messages=log; SET auto_explain.log_min_duration=0; SET auto_explain.log_nested_statements=on; SET auto_explain.log_format=json;")
        log=Path(self.db.err.name);start=log.stat().st_size
        ids=','.join(lit(self.mid('plan_'+str(i))) for i in range(256))
        try:
            self.db.run(f"BEGIN; SELECT zebridge_private.lock_receipts(array_fill('alice'::text,ARRAY[256]),ARRAY[{ids}]); SELECT count(*) FROM zebridge_private.lookup_receipts(array_fill('alice'::text,ARRAY[256]),ARRAY[{ids}],array_fill('route'::bytea,ARRAY[256]),array_fill('body'::bytea,ARRAY[256])); ROLLBACK;")
        finally:self.db.run('SET auto_explain.log_min_duration=-1;')
        with log.open('rb') as f:f.seek(start);text=f.read().decode()
        decoder=json.JSONDecoder();pos=0;reads=[]
        while (pos:=text.find('{\n',pos))>=0:
            try:value,n=decoder.raw_decode(text[pos:]);pos+=n
            except ValueError:pos+=1;continue
            query=value.get('Query Text','')
            if 'zebridge_private.receipts r' in query:reads.append(value)
        (self.logdir/'unordered-receipt-plans.json').write_text(json.dumps(reads,indent=2)+'\n')
        self.check('256 unordered claims execute one receipt-reading statement',len(reads)==1)

    def unordered_batch(self):
        import hashlib
        def gate(rows):
            def arr(xs,typ):return 'ARRAY['+','.join('NULL' if x is None else lit(x) for x in xs)+']::'+typ+'[]'
            prs,ids,routes,payloads=map(list,zip(*rows))
            return (f"SELECT zebridge_private.lock_receipts({arr(prs,'text')},{arr(ids,'text')});"
                "SELECT jsonb_agg(jsonb_build_array(ord,status,encode(fingerprint,'hex'),outcome) ORDER BY ord) FROM zebridge_private.lookup_receipts("
                f"{arr(prs,'text')},{arr(ids,'text')},{arr(routes,'bytea')},{arr(payloads,'bytea')});")
        def check_rows(label,rows,expected):
            result=json.loads(self.w.run('BEGIN;'+gate(rows)).splitlines()[-1])
            self.check(label,[r[1] for r in result]==expected and [r[0] for r in result]==list(range(1,len(rows)+1)))
            expected_fp=[None if r is None or p is None else hashlib.sha256(len(r.encode()).to_bytes(4,'big')+r.encode()+p.encode()).hexdigest() for _,_,r,p in rows]
            self.check(label+' fingerprints',[r[2] for r in result]==expected_fp)
            self.w.run('ROLLBACK;')
            return result
        a,b=self.mid('unordered_a'),self.mid('unordered_b')
        row=lambda mid,payload='body',pr='alice',route='route':(pr,mid,route,payload)
        check_rows('unordered repeats and conflicts compare the first input',[row(a),row(a),row(a,'changed')],['claimed','repeat','conflict'])
        check_rows('unordered identities are scoped by principal',[row(a),row(a,pr='bob')],['claimed','claimed'])
        check_rows('invalid input cannot become the repeat basis',[row(a,None),row(a)],['invalid','claimed'])
        check_rows('unordered validates routes and malformed structured IDs',[row(a,route=None),row('zb1-bad')],['invalid','invalid'])
        check_rows('legacy ID and binary framing stay supported',[row('unordered-legacy',route='r'),row(b,'é',route='xy')],['claimed','claimed'])
        future=self.mid('unordered_future',int(time.time()*1000)+120000)
        check_rows('repeat and conflict precede future timing',[row(future),row(future),row(future,'other')],['future','repeat','conflict'])
        self.w.run('BEGIN;'+self.receipt(a,status='rejected',reason='fixture')+'COMMIT;')
        result=check_rows('receipt duplicate wins and later repeats carry no outcome',[row(a),row(a),row(a,'changed')],['duplicate','repeat','conflict'])
        self.check('duplicate returns the exact stored outcome',result[0][3]=={'status':'rejected','reason':'fixture'} and result[1][3] is None and result[2][3] is None)
        self.w.run(self.park(b,self.mid('unordered_missing')))
        check_rows('unordered preserves a conflicting ordered park',[row(b,'changed')],['conflict'])
        check_rows('same parked bytes retain existing claim semantics',[row(b)],['claimed'])
        self.balanced('unordered probes never change park accounting')
        self.reset()
        old=self.mid('unordered_old',self.clock-60000)
        self.w.run('BEGIN;'+self.receipt(old)+'COMMIT;')
        self.db.run(f'UPDATE zebridge_private.receipt_policy SET min_accepted_ms={self.clock-1000};')
        check_rows('durable duplicate and conflict precede own expiry',[row(old),row(old,'changed')],['duplicate','conflict'])
        expired=self.mid('unordered_missing_old',self.clock-60000)
        check_rows('repeat precedence is unchanged for an expired first entry',[row(expired),row(expired)],['expired','repeat'])
        # Completion guard covers exactly first claimed occurrences, not repeats.
        fresh=self.mid('unordered_guard')
        self.w.run('BEGIN;'+gate([row(fresh),row(fresh),row(fresh,'changed')]))
        self.check('unordered guard registers a claimed identity only once',self.w.run(f'RESET ROLE; SELECT cardinality(expected_keys)=1 AND cardinality(expected_fps)=1 FROM zebridge_private.receipt_guards WHERE txid=pg_current_xact_id(); SET ROLE "{self.writer}";')=='t')
        finish=self.receipt(fresh);finish=finish[finish.index('SELECT zebridge_private.record_receipts'):]
        self.w.run(finish+'COMMIT;')
        check_rows('committed bulk claim is a duplicate in a new transaction',[row(fresh)],['duplicate'])
        doomed=self.session(name='unordered_missing_receipt',writer=True)
        try:
            doomed.run('BEGIN;'+gate([row(self.mid('unordered_orphan'))])+"INSERT INTO park_effect VALUES ('unordered_orphan'); COMMIT;")
            refused=False
        except RuntimeError:refused=True
        self.check('unordered missing receipt rolls back application changes',refused and 'undecided writes' in Path(doomed.err.name).read_text() and self.db.run("SELECT count(*) FROM park_effect WHERE id='unordered_orphan'")=='0')
        # Two sessions run the real lock + bulk gate, not a simulated verdict.
        owner=self.session(name='unordered_owner',writer=True)
        other=self.session(name='unordered_duplicate',writer=True)
        race=self.mid('unordered_race')
        owner.run('BEGIN;'+gate([row(race)]))
        finish=self.receipt(race);finish=finish[finish.index('SELECT zebridge_private.record_receipts'):]
        owner.run(finish)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(other.run,'BEGIN;'+gate([row(race)]))
            end=time.monotonic()+5
            while time.monotonic()<end:
                if self.db.run("SELECT count(*) FROM pg_stat_activity WHERE application_name='unordered_duplicate' AND wait_event='advisory'")=='1':break
                time.sleep(.02)
            self.check('unordered racing duplicate waits for original transaction',not future.done() and time.monotonic()<end)
            owner.run('COMMIT;')
            result=json.loads(future.result(timeout=10).splitlines()[-1]);other.run('ROLLBACK;')
            self.check('unordered racing duplicate gets the original outcome',result[0][1]=='duplicate' and result[0][3]['status']=='accepted')

    def set_based_gate(self):
        # NULL flags deliberately bypass the optimization but take the original
        # ordered ELSE branch. This is an internal differential oracle, not wire input.
        def gate(ids,preds,fast=True,principals=None,payloads=None):
            def arr(xs,typ='text'):
                return 'ARRAY['+','.join('NULL' if x is None else lit(x) for x in xs)+']::'+typ+'[]'
            prs=principals or ['alice']*len(ids)
            return (f"SELECT zebridge_private.lock_receipts({arr(prs)},{arr(ids)});"
                f"SELECT jsonb_agg(jsonb_build_array(status,encode(fingerprint,'hex'),outcome) ORDER BY ord) FROM zebridge_private.lookup_ordered_receipts("
                f"{arr(prs)},{arr(ids)},{arr(['route']*len(ids),'bytea')},{arr(payloads or ['body']*len(ids),'bytea')},"
                f"ARRAY[{','.join('true' if fast else 'NULL::boolean' for _ in ids)}],{arr(preds)});")
        def compare(label,ids,preds,**kw):
            answers=[]
            for fast in [True,False]:
                answers.append(self.w.run('BEGIN;'+gate(ids,preds,fast,**kw)).splitlines()[-1])
                self.w.run('ROLLBACK;')
            self.check(label,answers[0]==answers[1])
        a,b,c=[self.mid('set_'+x) for x in 'abc']
        compare('bulk fresh chain matches scalar decisions and fingerprints',[a,b,c],[None,a,b])
        compare('bulk does not release a predecessor arriving later',[b,a],[a,None])
        compare('bulk isolates principals with identical message IDs',[a,b],[None,a],principals=['alice','bob'])
        compare('bulk validates client token even for an in-batch predecessor',[a,b.replace('-client-','-other-')],[None,a])
        compare('bulk refuses self dependency',[a],[a])
        compare('bulk refuses future own ID',[self.mid('set_future',int(time.time()*1000)+120000)],[None])
        compare('bulk expires own ID before considering dependency',[self.mid('set_expired',1000)],[a])
        for outcome in ['accepted','stale','row_deleted','rejected','indeterminate','failed']:
            parent=self.mid('set_parent_'+outcome)
            self.w.run('BEGIN;'+self.receipt(parent,status=outcome)+'COMMIT;')
            compare('bulk classifies committed predecessor '+outcome,[a,b],[parent,a])
        unknown=self.mid('set_unknown')
        self.w.run('BEGIN;'+self.receipt(unknown,status='indeterminate',reason='PredecessorUnknown')+'COMMIT;')
        compare('bulk propagates unknown predecessor through suffix',[a,b],[unknown,a])
        compare('bulk preserves duplicate before dependency validation',[unknown],[unknown])
        compare('bulk preserves conflicting receipt',[unknown],[None],payloads=['changed'])
        self.w.run(self.park(b,a))
        compare('bulk preserves park release and credit path',[a,b,c],[None,a,b])
        compare('bulk preserves conflicting parked fingerprint',[b],[a],payloads=['changed'])
        self.balanced('bulk differential rollbacks leave park accounting intact')
        self.reset()
        # Prove the common path does not hide scalar calls behind a batch wrapper.
        functions=[('ordered_mutation_state','text,text,bytea,bytea,text'),('lookup_receipts','text[],text[],bytea[],bytea[]')]
        definitions=[self.db.run(f"SELECT pg_get_functiondef('zebridge_private.{name}({types})'::regprocedure)") for name,types in functions]
        try:
            for definition in definitions:
                head=definition.split('AS $function$')[0]
                self.db.run(head+"AS $function$ BEGIN RAISE EXCEPTION 'scalar gate called'; END; $function$;")
            ids=[self.mid('set_long_'+str(i)) for i in range(256)]
            result=json.loads(self.w.run('BEGIN;'+gate(ids,[None]+ids[:-1])).splitlines()[-1])
            self.check('256-write ordered batch never calls scalar gates',len(result)==256 and all(r[0]=='claimed' for r in result))
            self.check('bulk registers all 256 guarded fingerprints once',self.w.run(f'RESET ROLE; SELECT cardinality(expected_keys)=256 AND cardinality(expected_fps)=256 FROM zebridge_private.receipt_guards WHERE txid=pg_current_xact_id(); SET ROLE "{self.writer}";')=='t')
            self.w.run('ROLLBACK;')
        finally:
            for definition in definitions:self.db.run(definition)
        doomed=self.session(name='bulk_missing_receipt',writer=True)
        try:
            doomed.run('BEGIN;'+gate([a,b],[None,a])+"INSERT INTO park_effect VALUES ('bulk_orphan'); COMMIT;")
            refused=False
        except RuntimeError:refused=True
        self.check('bulk cannot commit business effects without receipts',refused and 'undecided writes' in Path(doomed.err.name).read_text() and self.db.run("SELECT count(*) FROM park_effect WHERE id='bulk_orphan'")=='0')
        # Same-key duplicate waits for the original transaction, while an unrelated
        # client can claim. A predecessor's uncommitted receipt never releases a child.
        owner=self.session(name='bulk_owner',writer=True)
        duplicate=self.session(name='bulk_duplicate',writer=True)
        child=self.session(name='bulk_child',writer=True)
        owner.run('BEGIN;'+gate([a],[None]))
        finish=self.receipt(a);finish=finish[finish.index('SELECT zebridge_private.record_receipts'):]
        owner.run(finish)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(duplicate.run,'BEGIN;'+gate([a],[None]))
            end=time.monotonic()+5
            while time.monotonic()<end:
                if self.db.run("SELECT count(*) FROM pg_stat_activity WHERE application_name='bulk_duplicate' AND wait_event='advisory'")=='1':break
                time.sleep(.02)
            self.check('bulk duplicate is held by the existing receipt lock',not future.done() and time.monotonic()<end)
            statuses=json.loads(child.run('BEGIN;'+gate([b],[a])).splitlines()[-1])
            self.check('bulk child cannot use an uncommitted predecessor',statuses[0][0]=='waiting')
            child.run('ROLLBACK;')
            statuses=json.loads(child.run('BEGIN;'+gate([c],[None],principals=['bob'])).splitlines()[-1])
            self.check('bulk unrelated principal progresses while owner is held',statuses[0][0]=='claimed')
            child.run('ROLLBACK;')
            owner.run('COMMIT;')
            answer=json.loads(future.result(timeout=10).splitlines()[-1])
            self.check('bulk racing duplicate observes the committed receipt',answer[0][0]=='duplicate')
            duplicate.run('ROLLBACK;')
        statuses=json.loads(child.run('BEGIN;'+gate([b],[a])).splitlines()[-1])
        self.check('bulk child becomes eligible after predecessor commit',statuses[0][0]=='claimed')
        child.run('ROLLBACK;')

    def candidate_window(self):
        self.db.run('CREATE TABLE gate_scope(id int); CREATE PUBLICATION gate_pub FOR TABLE gate_scope;')
        root=self.mid('window0')
        # Children deliberately arrived first. A LIMIT ordered only by parked_at
        # would select descendants and omit their ready ancestor forever.
        prefix=f'zb1-t{self.clock}-INSERT-client-tab-k1-window'
        self.w.run(f"SELECT zebridge_private.park_mutation('alice',{lit(prefix)}||n||'-v','mutation.alice.gate_scope.insert'::bytea,'body'::bytea,{lit(prefix)}||(n-1)||'-v','{{}}') FROM generate_series(270,1,-1) n;")
        self.w.run('BEGIN;'+self.receipt(root)+'COMMIT;')
        selected=self.w.run("SELECT message_id FROM zebridge_private.parked_mutations_ready('gate_pub',256)").splitlines()
        self.check('bounded candidate window contains the ready ancestor first',len(selected)==256 and selected[0]==self.mid('window1'))
        self.check('bounded candidate window contains a runnable dependency prefix',selected==[self.mid('window'+str(i)) for i in range(1,257)])
        self.check('candidate scan does not steal another publication work',self.w.run("SELECT count(*) FROM zebridge_private.parked_mutations_ready('other_pub',256)")=='0')
        self.balanced('candidate scanning changes no quota')
        self.reset()

    def batch_and_cycles(self):
        parent,child = self.mid('batch_parent'),self.mid('batch_child')
        self.w.run('BEGIN;'+f"SELECT zebridge_private.lock_receipts(ARRAY['alice','alice'],ARRAY[{lit(parent)},{lit(child)}]);"+self.receipt(parent))
        self.check('same-transaction predecessor receipt releases child',self.w.run(self.state(child,parent))=='ready')
        self.w.run('ROLLBACK;')
        self.check('batch rollback discards tentative predecessor satisfaction',self.w.run(self.state(child,parent))=='waiting')
        self.w.run('BEGIN;'+self.receipt(parent))
        independent=self.session(name='uncommitted_predecessor',writer=True)
        self.check('uncommitted predecessor parks without waiting for its lock',independent.run(self.park(child,parent))=='parked')
        self.w.run('ROLLBACK;')
        self.check('aborted predecessor leaves its successor parked',independent.run(self.release(child,parent))=='waiting' and self.credits_of(child)==0)
        self.w.run('BEGIN;'+self.receipt(parent)+'COMMIT;')
        independent.run('BEGIN;'+self.release(child,parent)+self.record(child)+'COMMIT;')
        self.balanced('predecessor retry and successor release balance quotas')
        parent=self.mid('other_principal')
        self.w.run('BEGIN;'+self.receipt(parent,principal='bob')+'COMMIT;')
        self.check('another principal receipt cannot release this client',self.w.run(self.state(self.mid('scope'),parent))=='waiting')
        a,b=self.mid('two_a'),self.mid('two_b')
        self.w.run(self.park(a,b))
        self.check('two-cycle refused while first participant stays parked',self.w.run(self.park(b,a))=='invalid_predecessor')
        self.reset()
        a,b,c=[self.mid('cycle_'+v) for v in ('a','b','c')]
        self.check('three-cycle parks without execution',all(self.w.run(self.park(x,y))=='parked' for x,y in ((a,b),(b,c),(c,a))))
        self.db.run(f'UPDATE zebridge_private.receipt_policy SET min_accepted_ms={self.clock+1};')
        self.check('equal-time cycle expires without receipts',all(self.w.run(self.release(x,y))=='expired' for x,y in ((a,b),(b,c),(c,a))))
        self.check('expiry owes no verdict: the bridge answers it',all(self.owed_verdict(x)=='' for x in (a,b,c)))
        self.balanced('cycle expiry releases all counters')
        self.clock+=100
        self.reset()

    def caps(self):
        for global_cap in (False, True):
            self.reset()
            self.db.run('UPDATE zebridge_private.mutation_park_policy SET ' + ('max_rows=3' if global_cap else 'principal_rows=3'))
            gate = threading.Barrier(9)
            def attempt(i):
                s = self.session(name=f'cap_{global_cap}_{i}', writer=True)
                gate.wait(timeout=10)
                return s.run(self.park(self.mid(f'cap{global_cap}{i}'),self.mid('cap_missing'), f'p{i}' if global_cap else 'cap'))
            with ThreadPoolExecutor(max_workers=8) as pool:
                jobs = [pool.submit(attempt,i) for i in range(8)]
                gate.wait(timeout=10)
                results = [f.result(timeout=20) for f in jobs]
            self.check(('global' if global_cap else 'principal') + ' concurrent cap admits exactly 3/8', results.count('parked') == 3 and results.count('backlog_full') == 5)
            self.balanced('concurrent cap keeps both counters exact')
            self.check('quota refusal leaves no empty principal rows',self.db.run('SELECT count(*) FROM zebridge_private.mutation_park_quota WHERE rows=0')=='0')
        self.reset()
        self.db.run('UPDATE zebridge_private.mutation_park_policy SET max_bytes=1;')
        self.check('global byte cap refuses without charging', self.w.run(self.park(self.mid('byte'), self.mid('pred'))) == 'backlog_full')
        self.balanced('byte refusal leaves counters unchanged')
        self.reset()
        self.db.run('UPDATE zebridge_private.mutation_park_policy SET principal_bytes=1;')
        self.check('principal byte cap refuses without charging', self.w.run(self.park(self.mid('byte'), self.mid('pred'))) == 'backlog_full')
        self.reset()

    def helper_primitive(self):
        # take_parked_mutation, which the bridge never calls: only what it does differently.
        a, p = self.mid('helper'), self.mid('helper_pred')
        self.w.run(self.park(a, p))
        self.w.run('BEGIN;' + self.receipt(p) + 'COMMIT;')
        self.check('the helper answers absent for no parked row', self.w.run(self.take(self.mid('helper_none'))) == 'absent')
        self.w.run('BEGIN;' + self.take(a) + self.receipt(a) + 'COMMIT;')
        self.check('the helper releases into a credit that owes no verdict (why the bridge uses the ordered lookup)',
                   self.credits_of(a) == 1 and self.owed_verdict(a) == '')
        self.w.run('SELECT zebridge_private.settle_park_credits();')
        self.balanced('the helper credit settles its charge')
        self.reset()

    def expiry(self):
        a,p = self.mid('expires'), self.mid('expires_pred')
        self.w.run(self.park(a,p))
        self.db.run(f'UPDATE zebridge_private.receipt_policy SET min_accepted_ms={self.clock+1};')
        self.check('equal-time successor/predecessor expire without execution', self.w.run(self.release(a, p)) == 'expired')
        self.check('expiry creates no receipt', self.db.run(f'SELECT count(*) FROM zebridge_private.receipts WHERE message_id={lit(a)}') == '0')
        self.balanced('expiry releases both quotas')
        self.w.run('SELECT zebridge_private.settle_park_credits();')
        self.check('expiry removes empty principal metadata',self.db.run('SELECT count(*) FROM zebridge_private.mutation_park_quota WHERE rows=0')=='0')
        self.clock += 100
        a,p = self.mid('attempt'),self.mid('attempt_pred')
        self.w.run(self.park(a,p))
        for _ in range(3):
            self.w.run(self.park(a,p))
            self.w.run(self.release(a,p))
        self.check('wait/resend never spends execution attempts', self.db.run(f'SELECT attempts FROM zebridge_private.mutation_parked WHERE message_id={lit(a)}') == '0')
        for _ in range(int(self.db.run('SELECT max_attempts FROM zebridge_private.mutation_park_policy'))):
            self.w.run(f"SELECT zebridge_private.fail_parked_mutation('alice',{lit(a)});")
        self.check('execution budget exhausts without terminal receipt', self.w.run(self.release(a, p)) == 'exhausted' and self.owed_verdict(a) == '')
        self.balanced('exhausted attempt releases quota')
        self.reset()

    def credits(self):
        self.reset()
        original_budget=int(self.db.run('SELECT max_attempts FROM zebridge_private.mutation_park_policy'))
        expected=int(re.search(r'pub const mutation_max_deliver: i32 = ([0-9]+);',(ROOT/'src/config.zig').read_text())[1])
        self.check('SQL retry policy matches the bridge source constant',original_budget==expected)
        self.db.run('UPDATE zebridge_private.mutation_park_policy SET max_attempts=2;')
        mid,pred=self.mid('repeat_exhausted'),self.mid('repeat_missing')
        for cycle in range(2):
            self.check(f'exhausted ID can park again, release {cycle+1}',self.w.run(self.park(mid,pred))=='parked')
            self.w.run(f"SELECT zebridge_private.fail_parked_mutation('alice',{lit(mid)});")
            self.check('below shared retry budget still waits',self.w.run(self.release(mid,pred))=='waiting')
            self.w.run(f"SELECT zebridge_private.fail_parked_mutation('alice',{lit(mid)});")
            self.check('shared retry budget controls exhaustion',self.w.run(self.release(mid,pred))=='exhausted')
        self.check('same message has two distinct durable release credits',self.db.run('SELECT count(*)=2 AND count(DISTINCT credit_id)=2 FROM zebridge_private.mutation_park_credit')=='t')
        self.balanced('unsettled credits retain the full charge')
        self.db.run('UPDATE zebridge_private.mutation_park_policy SET max_rows=2;')
        self.check('credits keep global cap closed despite no parked rows',self.w.run(self.park(self.mid('closed'),pred,'bob'))=='backlog_full')
        self.db.run('UPDATE zebridge_private.mutation_park_policy SET max_rows=100,principal_rows=2;')
        self.check('credits keep principal cap closed',self.w.run(self.park(self.mid('closed2'),pred))=='backlog_full')
        charged=int(self.db.run('SELECT bytes FROM zebridge_private.mutation_park_total'))
        self.db.run(f'UPDATE zebridge_private.mutation_park_policy SET principal_rows=100,max_bytes={charged};')
        self.check('credits also keep global byte cap closed',self.w.run(self.park(self.mid('closed_bytes'),pred,'bob'))=='backlog_full')
        self.db.run(f'UPDATE zebridge_private.mutation_park_policy SET max_bytes=1073741824,principal_bytes={charged};')
        self.check('credits also keep principal byte cap closed',self.w.run(self.park(self.mid('closed_principal_bytes'),pred))=='backlog_full')
        self.w.run('BEGIN; SELECT zebridge_private.settle_park_credits(); ROLLBACK;')
        self.check('settlement rollback retains both credits',self.db.run('SELECT count(*) FROM zebridge_private.mutation_park_credit')=='2')
        self.balanced('settlement rollback retains both counters')
        dead_settler=self.session(name='dead_settler',writer=True)
        dead_settler.run('BEGIN; SELECT zebridge_private.settle_park_credits();')
        dead_settler.close()
        self.check('settler crash restores its credits',self.db.run('SELECT count(*) FROM zebridge_private.mutation_park_credit')=='2')
        self.balanced('settler crash restores its counters')
        self.check('bounded settlement processes only one credit',self.w.run('SELECT zebridge_private.settle_park_credits(1);')=='1')
        self.balanced('partial settlement leaves remaining charge exact')
        # An exited releaser never ran settlement; a new process/session can recover it.
        settler=self.session(name='restarted_settler',writer=True)
        self.check('new session recovers outstanding credit',settler.run('SELECT zebridge_private.settle_park_credits();')=='1')
        self.check('settlement replay cannot debit twice',settler.run('SELECT zebridge_private.settle_park_credits();')=='0')
        self.balanced('recovered settlement empties the counters')
        self.db.run(f'UPDATE zebridge_private.mutation_park_policy SET max_attempts={original_budget};')
        self.reset()
        # A terminal receipt is independent of accounting settlement and survives it.
        a,p=self.mid('credit_committed'),self.mid('credit_parent')
        self.w.run(self.park(a,p))
        self.w.run('BEGIN;'+self.receipt(p)+'COMMIT;')
        releaser=self.session(name='credit_crash_after_commit',writer=True)
        releaser.run('BEGIN;'+self.release(a,p)+self.record(a)+"INSERT INTO park_effect VALUES ('credit_committed'); COMMIT;")
        releaser.close()
        self.check('crash after commit leaves credit, receipt and application row',self.db.run("SELECT (SELECT count(*) FROM zebridge_private.mutation_park_credit)=1 AND (SELECT count(*) FROM park_effect)=1")=='t' and self.w.run(self.park(a,p))=='duplicate')
        self.balanced('post-commit crash keeps counters conservative')
        self.check('the credit still owes the committed receipt\'s verdict',self.owed_verdict(a)=='accepted')
        credit=self.db.run(f'SELECT credit_id FROM zebridge_private.mutation_park_credit WHERE message_id={lit(a)}')
        state=lambda:self.db.run(f'SELECT count(*)||\',\'||bool_and(NOT verdict_pending) FROM zebridge_private.mutation_park_credit WHERE credit_id={credit}')
        self.db.run(f'SELECT zebridge_private.park_verdict_delivered({credit})')
        first=state()
        self.check('the first delivery mark takes the credit out of the owed verdicts, the credit kept for settlement',self.owed_verdict(a)=='' and first=='1,true')
        self.db.run(f'SELECT zebridge_private.park_verdict_delivered({credit})')
        self.check('a repeated delivery mark leaves that state unchanged',state()==first and self.owed_verdict(a)=='')
        # Concurrent settlers: one deliberately retains the global lock; the second
        # waits, then observes no remaining credits. Neither takes receipt locks.
        one=self.session(name='settle_one',writer=True)
        two=self.session(name='settle_two',writer=True)
        self.w.run(f"BEGIN; SELECT zebridge_private.lock_receipts(ARRAY['alice'],ARRAY[{lit(a)}]);")
        one.run('BEGIN;')
        self.check('first settler consumes credit inside transaction',one.run('SELECT zebridge_private.settle_park_credits();')=='1')
        with ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(two.run,'SELECT zebridge_private.settle_park_credits();')
            try:
                self.blocked('settle_two')
            finally:
                one.run('COMMIT;')
            self.check('second concurrent settler cannot charge credit again',future.result(timeout=5)=='0')
        self.w.run('ROLLBACK;')
        self.balanced('concurrent settlers debit exactly once')
        self.check('terminal outcome survives settlement',self.w.run(self.park(a,p))=='duplicate')
        self.reset()

    def contention(self):
        a,p = self.mid('held'),self.mid('held_pred')
        self.w.run(self.park(a,p))
        self.w.run('BEGIN;' + self.receipt(p) + 'COMMIT;')
        self.db.run("CREATE FUNCTION public.hold_park_effect() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                    "IF NEW.id='held' THEN PERFORM pg_advisory_xact_lock(1938009,1); END IF; RETURN NEW; END $$; "
                    "CREATE TRIGGER hold_park_effect BEFORE INSERT ON park_effect FOR EACH ROW EXECUTE FUNCTION hold_park_effect();")
        holder = self.session(name='gate')
        holder.run('SELECT pg_advisory_lock(1938009,1);')
        release1 = self.session(name='release1',writer=True)
        release2 = self.session(name='release2',writer=True)
        parker = self.session(name='unrelated_park',writer=True)
        resend = self.session(name='resend',writer=True)
        conflict = self.session(name='conflict',writer=True)
        def gate():
            return (f"SELECT zebridge_private.lock_receipts(ARRAY['alice'],ARRAY[{lit(a)}]);"
                    f"SELECT status FROM zebridge_private.lookup_ordered_receipts(ARRAY['alice'],ARRAY[{lit(a)}],ARRAY['route'::bytea],ARRAY['body'::bytea],ARRAY[true],ARRAY[{lit(p)}]);")
        finish=self.receipt(a)
        finish=finish[finish.index('SELECT zebridge_private.record_receipts'):]
        def measured(s, sql):
            start=time.monotonic()
            result=s.run(sql)
            return result, (time.monotonic()-start)*1000
        with ThreadPoolExecutor(max_workers=5) as pool:
            first=pool.submit(measured,release1,'BEGIN;'+gate()+finish+"INSERT INTO park_effect VALUES ('held'); COMMIT;")
            self.blocked('release1')
            second=pool.submit(measured,release2,'BEGIN;'+gate()+'COMMIT;')
            pending=pool.submit(measured,parker,self.park(self.mid('unrelated'),self.mid('unrelated_pred'),'bob'))
            duplicate=pool.submit(measured,resend,self.park(a,p))
            mismatch=pool.submit(measured,conflict,self.park(a,p,payload='conflict'))
            try:
                for name in ('release2','resend','conflict'):
                    self.blocked(name)
                try:
                    parked_while_held=pending.result(timeout=2)[0]=='parked'
                except TimeoutError:
                    parked_while_held=False
                self.check('unrelated park commits while application gate remains held', parked_while_held and not first.done())
                start_same=time.monotonic()
                self.check('another device of SAME principal can park while application is held', self.w.run(self.park(self.mid('same_principal'),self.mid('same_pred')))== 'parked' and not first.done())
                self.measure['same_principal_park_ms']=(time.monotonic()-start_same)*1000
                self.check('settlement ignores the uncommitted release credit',self.w.run('SELECT zebridge_private.settle_park_credits();')=='0' and not first.done())
                start=time.monotonic()
                self.w.run('BEGIN;'+self.receipt(self.mid('ordinary'),'carol')+"INSERT INTO park_effect VALUES ('ordinary'); COMMIT;")
                self.measure['ordinary_commit_while_release_held_ms']=(time.monotonic()-start)*1000
                self.check('ordinary eligible transaction commits while release is held', self.db.run("SELECT count(*) FROM park_effect WHERE id='ordinary'") == '1' and not first.done())
                time.sleep(self.args.hold_seconds)
            finally:
                holder.run('SELECT pg_advisory_unlock(1938009,1);')
            results=[f.result(timeout=20) for f in (first,second,pending,duplicate,mismatch)]
        self.check('two release workers execute once', results[1][0].splitlines()[-1]=='duplicate' and self.db.run("SELECT count(*) FROM park_effect WHERE id='held'")=='1')
        self.check('racing resend sees committed receipt', results[3][0]=='duplicate')
        self.check('racing conflicting resend cannot repark', results[4][0]=='conflict')
        self.check('unrelated park completed without waiting for release',results[2][0]=='parked')
        self.measure.update(held_release_ms=results[0][1], competing_release_ms=results[1][1], unrelated_park_wait_ms=results[2][1], hold_seconds=self.args.hold_seconds)
        self.balanced('held race preserves exact quotas')
        times=[]
        for i in range(30):
            start=time.monotonic()
            self.w.run(self.park(self.mid(f'bench{i}'),self.mid('bench_pred'),'bench'))
            times.append((time.monotonic()-start)*1000)
        self.measure['uncontended_park_ms']={'median':statistics.median(times),'max':max(times),'samples':len(times)}
        print('  measurements: '+json.dumps(self.measure,sort_keys=True),flush=True)

    def close(self):
        for s in reversed(self.sessions[1:]):
            s.close()
        try:
            self.admin.run(f'DROP DATABASE IF EXISTS "{self.name}" WITH (FORCE); DROP ROLE IF EXISTS "{self.writer}"; DROP ROLE IF EXISTS "{self.reader}";')
        finally:
            self.admin.close()
            (self.logdir/'results.json').write_text(json.dumps(dict(checks=self.checks,measurements=self.measure),indent=2)+'\n')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--admin-url',default=os.environ.get('ADMIN_DATABASE_URL','postgres://postgres@127.0.0.1:5432/postgres'))
    p.add_argument('--log-dir',default='/tmp/zb-ingress-park-sql')
    p.add_argument('--hold-seconds',type=float,default=1.0)
    p.add_argument('--upgrade-write-sql',help='upgrade an actual earlier write-template snapshot, with a parked row')
    p.add_argument('--trace-batch-plans',action='store_true',help='require auto_explain and assert one nested unordered receipt read per batch')
    p.add_argument('--upgrade-from-head',action='store_true',help='first install HEAD templates and retain an existing receipt')
    args=p.parse_args()
    suite=Suite(args)
    try:
        suite.init(); suite.sweep_cost_and_election(); suite.basic(); suite.unordered_batch(); suite.ordered_batch(); suite.set_based_gate(); suite.candidate_window(); suite.batch_and_cycles(); suite.caps(); suite.helper_primitive(); suite.expiry(); suite.credits(); suite.contention()
        if args.trace_batch_plans:suite.unordered_plan_count()
        print(f'{len(suite.checks)}/{len(suite.checks)} clean')
    finally:
        suite.close()


if __name__=='__main__':
    main()
