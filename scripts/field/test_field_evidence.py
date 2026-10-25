"""Offline harness tests: SQLite evidence, frozen truth, native logs and NATS mocks.

Run: python3 -m unittest discover -s scripts/field -p 'test_*.py' -v
No field host, database credentials, libzb binary or running NATS is needed.
Set ZB_FIELD_TEST_PG_BIN to a directory containing initdb, pg_ctl and psql to also
exercise the export on a disposable, Unix-socket-only local PostgreSQL cluster.
"""
import argparse
import asyncio
from contextlib import closing, redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock, patch

import field_evidence as evidence
import field_follow
import field_worker


class Fixture(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db = self.root / "c032.sqlite3"

    def database(self, rows):
        with closing(sqlite3.connect(self.db)) as con:
            con.execute("CREATE TABLE storm_items(id TEXT PRIMARY KEY, tenant_id TEXT, note TEXT, updated_at TEXT)")
            con.executemany("INSERT INTO storm_items VALUES (?, 'storm', ?, ?)", rows)
            con.commit()

    def truth(self, rows):
        digest = hashlib.sha256()
        for key, note, _ in sorted(rows):
            digest.update(evidence.frame(key, note))
        summary = {"kind": "summary", "rows": len(rows), "digest": digest.hexdigest(),
                   "snapshot": "100:100:", "captured_at": "2026-10-25T00:00:00Z"}
        records = [summary] + [{"kind": "row", "id": key, "version": version,
                                "value_digest": hashlib.sha256(evidence.value_frame(note)).hexdigest()}
                               for key, note, version in sorted(rows)]
        path = self.root / "truth.jsonl"
        path.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in records))
        return {**summary, "truth_file": path.name, "truth_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


class TruthTests(Fixture):
    def test_null_empty_and_utf8_have_distinct_byte_frames(self):
        self.assertEqual(evidence.frame("a", None), b"a:N")
        self.assertEqual(evidence.frame("a", ""), b"a:S0:")
        self.assertEqual(evidence.frame("a", "é\n"), "a:S3:é\n".encode())

    def test_single_export_supplies_grading_and_rows_without_another_read(self):
        done = self.truth([("one", None, "v1"), ("two", "é", "v2")])
        reply = subprocess.CompletedProcess([], 0, (self.root / "truth.jsonl").read_text(), "")
        with patch.object(evidence.subprocess, "run", return_value=reply) as run:
            frozen = evidence.freeze_truth({"SB_ADMIN_URL": "postgresql://unused"}, self.root / "frozen.jsonl", "storm_items", "storm")
        self.assertEqual(run.call_count, 1)
        self.assertEqual(frozen["digest"], done["digest"])
        sql = run.call_args.args[0][-1]
        self.assertIn("WITH frozen AS MATERIALIZED", sql)
        self.assertEqual(sql.count('FROM public."storm_items"'), 1)
        self.assertIn("pg_current_snapshot()", sql)
        self.assertIn("'version', version", sql)

    def test_invalid_export_never_replaces_previous_truth(self):
        self.truth([])
        original = (self.root / "truth.jsonl").read_bytes()
        reply = subprocess.CompletedProcess([], 0, '{"kind":"summary","rows":2,"digest":"' + '0' * 64 + '"}\n', "")
        with patch.object(evidence.subprocess, "run", return_value=reply), self.assertRaises(ValueError):
            evidence.freeze_truth({"SB_ADMIN_URL": "unused"}, self.root / "truth.jsonl", "storm_items", "storm")
        self.assertEqual((self.root / "truth.jsonl").read_bytes(), original)

    def test_changed_or_duplicate_truth_is_refused(self):
        done = self.truth([("a", "n", "v")])
        path = self.root / "truth.jsonl"
        path.write_text(path.read_text() + path.read_text().splitlines()[1] + "\n")
        with self.assertRaisesRegex(ValueError, "checksum"):
            evidence.read_truth(path, done["truth_sha256"])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            evidence.read_truth(path)


class ReplicaTests(Fixture):
    def test_version_comparison_normalizes_offsets_and_preserves_microseconds(self):
        self.assertEqual(evidence.version_instant("2026-10-25 00:00:00.123456+00"),
                         evidence.version_instant("2026-10-25T01:30:00.123456+01:30"))
        self.assertNotEqual(evidence.version_instant("2026-10-25T00:00:00.123455Z"),
                            evidence.version_instant("2026-10-25T00:00:00.123456Z"))
        self.assertNotEqual(evidence.version_instant("unknown-a"), evidence.version_instant("unknown-b"))

    def test_version_only_diff_ignores_pg_wire_spelling_but_finds_real_drift(self):
        self.database([("same", "n", "2026-10-25T00:00:00.123456Z"),
                       ("offset", "n", "2026-10-25T01:30:00.123456+01:30"),
                       ("stale", "n", "2026-10-25T00:00:00.123455Z")])
        self.truth([(key, "n", "2026-10-25 00:00:00.123456+00") for key in ("same", "offset", "stale")])
        _, rows = evidence.read_truth(self.root / "truth.jsonl")
        counts = evidence.diff_replica(self.db, "storm", rows, self.root)
        self.assertEqual(counts["version_only"], 1)
        record = json.loads((self.root / "version-only.jsonl").read_text())
        self.assertEqual(record["id"], "stale")
        self.assertEqual(record["actual"]["version"], "2026-10-25T00:00:00.123455Z")

    def test_exact_closed_replica_is_deleted(self):
        rows = [("a", None, "v1"), ("b", "é", "v2")]
        self.database(rows)
        done = self.truth(rows)
        result = evidence.finalize_replica(self.db, "storm", done, "c032")
        self.assertTrue(result["exact"])
        self.assertFalse(self.db.exists())
        self.assertFalse((self.root / "evidence").exists())

    def test_same_count_reports_missing_extra_and_differing_values(self):
        self.database([("a", "wrong", "actual"), ("extra", "n", "v"), ("z", "same", "v-old")])
        done = self.truth([("a", "right", "expected"), ("missing", "n", "v"), ("z", "same", "v-new")])
        result = evidence.finalize_replica(self.db, "storm", done, "c032")
        self.assertFalse(result["exact"])
        self.assertEqual(result["rows"], done["rows"])
        self.assertEqual(result["diff"], {"missing": 1, "extra": 1, "differing": 1, "version_only": 1})
        self.assertTrue(Path(result["replica"]).exists())
        self.assertFalse(self.db.exists())
        differing = json.loads((self.root / "evidence/c032/differing.jsonl").read_text())
        self.assertEqual(differing["expected"]["version"], "expected")
        self.assertEqual(differing["actual"]["version"], "actual")

    def test_checkpoint_error_preserves_original_files(self):
        self.database([("a", "wrong", "v")])
        done = self.truth([("a", "right", "v")])
        original = self.db.read_bytes()
        connect = sqlite3.connect
        def deny_checkpoint(database, **kwargs):
            if database.endswith("?mode=rw"):
                raise sqlite3.OperationalError("checkpoint refused")
            return connect(database, **kwargs)
        with patch.object(evidence.sqlite3, "connect", side_effect=deny_checkpoint):
            result = evidence.finalize_replica(self.db, "storm", done, "c032")
        self.assertIn("checkpoint refused", result["checkpoint"]["error"])
        self.assertEqual(Path(result["replica"]).read_bytes(), original)

    def test_bad_truth_still_keeps_failed_database(self):
        self.database([("a", "wrong", "v")])
        done = self.truth([("a", "right", "v")])
        (self.root / "truth.jsonl").write_text("broken")
        result = evidence.finalize_replica(self.db, "storm", done, "c032")
        self.assertIn("checksum", result["diff_error"])
        self.assertTrue(Path(result["replica"]).exists())

    def test_busy_checkpoint_preserves_real_wal_and_its_committed_rows(self):
        self.database([("a", "old", "v1")])
        done = self.truth([("a", "authoritative", "v2")])
        writer = sqlite3.connect(self.db)
        writer.execute("PRAGMA journal_mode=WAL")
        reader = sqlite3.connect(self.db)
        try:
            reader.execute("BEGIN")
            reader.execute("SELECT * FROM storm_items").fetchall()
            writer.execute("UPDATE storm_items SET note='committed-in-wal', updated_at='v2'")
            writer.commit()
            writer.close()
            connect = sqlite3.connect
            def no_wait(database, **kwargs):
                if database.endswith("?mode=rw"):
                    kwargs["timeout"] = 0
                return connect(database, **kwargs)
            with patch.object(evidence.sqlite3, "connect", side_effect=no_wait):
                result = evidence.finalize_replica(self.db, "storm", done, "c032")
            self.assertEqual(result["checkpoint"]["busy"], 1)
            self.assertTrue(Path(result["replica"] + "-wal").exists())
        finally:
            reader.close()
            writer.close()
        with closing(sqlite3.connect(result["replica"])) as recovered:
            self.assertEqual(recovered.execute("SELECT note FROM storm_items").fetchone()[0], "committed-in-wal")

    def test_archive_keeps_closed_failure_and_never_copies_or_deletes_live_db(self):
        self.database([("a", "wrong", "v")])
        done = self.truth([("a", "right", "v")])
        result = evidence.finalize_replica(self.db, "storm", done, "c032")
        live = self.root / "unfinished.sqlite3"
        live.write_bytes(b"owned by a worker")
        (self.root / "observer.done").write_text("{}")
        archive = evidence.archive_run(self.root)
        with tarfile.open(archive) as saved:
            self.assertIn("evidence/c032/replica.sqlite3", saved.getnames())
            self.assertNotIn("unfinished.sqlite3", saved.getnames())
            self.assertIn("uncollected-replicas.json", saved.getnames())
        self.assertEqual(live.read_bytes(), b"owned by a worker")
        self.assertTrue(Path(result["replica"]).exists())

    def test_verified_collection_removes_source_but_keeps_failed_replica_copy(self):
        self.database([("a", "wrong", "v")])
        done = self.truth([("a", "right", "v")])
        evidence.finalize_replica(self.db, "storm", done, "c032")
        (self.root / "observer.done").write_text("{}")
        archive = evidence.archive_run(self.root)
        with tempfile.TemporaryDirectory() as destination:
            checksum = evidence.file_sha256(archive)
            evidence.extract_collection(archive, destination, checksum)
            result = evidence.cleanup_collected(self.root, checksum)
            self.assertTrue(result["removed"])
            self.assertFalse(self.root.exists())
            self.assertFalse(archive.exists())
            self.assertTrue((Path(destination) / "evidence/c032/replica.sqlite3").exists())

    def test_corrupt_transfer_never_deletes_source(self):
        (self.root / "observer.done").write_text("{}")
        archive = evidence.archive_run(self.root)
        with tempfile.TemporaryDirectory() as destination, self.assertRaisesRegex(ValueError, "checksum"):
            evidence.extract_collection(archive, destination, "0" * 64)
        self.assertTrue(self.root.exists())
        self.assertTrue(archive.exists())

    def test_inconsistent_archive_files_are_refused_before_cleanup(self):
        (self.root / "observer.done").write_text("{}")
        log = self.root / "client.log"
        log.write_text("before")
        archive = evidence.archive_run(self.root)
        # Emulate a file changing between manifest hashing and tar creation.
        log.write_text("after")
        with tarfile.open(archive, "w:gz") as saved:
            for path in self.root.iterdir():
                saved.add(path, arcname=path.name)
        with tempfile.TemporaryDirectory() as destination, self.assertRaisesRegex(ValueError, "client.log"):
            evidence.extract_collection(archive, destination, evidence.file_sha256(archive))
        self.assertTrue(self.root.exists())

    def test_cleanup_retains_changed_or_unfinished_sources(self):
        (self.root / "observer.done").write_text("{}")
        log = self.root / "client.log"
        log.write_text("before")
        archive = evidence.archive_run(self.root)
        checksum = evidence.file_sha256(archive)
        log.write_text("after")
        self.assertFalse(evidence.cleanup_collected(self.root, checksum)["removed"])
        self.assertTrue(log.exists())
        log.write_text("before")
        self.db.write_bytes(b"unfinished replica")
        self.assertFalse(evidence.cleanup_collected(self.root, checksum)["removed"])
        self.assertTrue(self.db.exists())

    def test_missing_worker_exit_marker_prevents_cleanup(self):
        (self.root / "observer.done").write_text("{}")
        (self.root / "run-config.json").write_text(json.dumps({"worker_clients": {"batch2": ["c032"]}}))
        archive = evidence.archive_run(self.root, wait_s=0)
        self.assertFalse(evidence.cleanup_collected(self.root, evidence.file_sha256(archive))["removed"])
        self.assertTrue(self.root.exists())


class LogTests(Fixture):
    def logged(self, client):
        worker = "c032" if client else "batch2"
        command = [sys.executable, "-c", "import sys; print('plain', file=sys.stderr); print('refusing the full (D2)', file=sys.stderr)"]
        args = NS(command=command, worker=worker, client=client, log=str(self.root / "worker.log"), signals=str(self.root / "signals.jsonl"))
        self.assertEqual(evidence.logged(args), 0)
        return (self.root / "worker.log").read_text(), json.loads((self.root / "signals.jsonl").read_text())

    def test_isolated_native_log_and_signal_name_the_client(self):
        text, signal = self.logged("c032")
        self.assertTrue(all("client=c032]" in line for line in text.splitlines()))
        self.assertEqual(signal["client"], "c032")
        self.assertIn("at_ms", signal)

    def test_grouped_native_log_does_not_invent_a_client(self):
        text, signal = self.logged(None)
        self.assertIn("worker=batch2 client=unattributed", text)
        self.assertIsNone(signal["client"])


class SamplerTests(unittest.IsolatedAsyncioTestCase):
    async def test_samples_limits_incarnation_cuts_and_errors_without_faking_empty_state(self):
        info = NS(created="incarnation", state=NS(first_seq=100, last_seq=200, bytes=4096, messages=50),
                  config=NS(max_bytes=8192, max_msgs=1000, max_age=60))
        js = NS(stream_info=AsyncMock(side_effect=[info, PermissionError()]), get_msg=AsyncMock(side_effect=[
            NS(seq=10, headers=None, data=b'{"cutoff_seq":150,"shared_cutoff_seq":25,"seed_epoch":2}'), TimeoutError()]))
        result = await evidence.sample(js, ["CDC_storm", "CDC_PUBLIC"], "generations", ["storm.storm_items", "_default.storm_items"])
        self.assertEqual(result["streams"]["CDC_storm"]["first_seq"], 100)
        self.assertEqual(result["streams"]["CDC_storm"]["created"], "incarnation")
        self.assertEqual(result["streams"]["CDC_storm"]["max_bytes"], 8192)
        self.assertEqual(result["streams"]["CDC_PUBLIC"]["error"], "PermissionError")
        self.assertNotIn("first_seq", result["streams"]["CDC_PUBLIC"])
        self.assertEqual(result["manifests"]["storm.storm_items"]["manifest"]["cutoff_seq"], 150)
        self.assertEqual(result["manifests"]["_default.storm_items"]["error"], "TimeoutError")
        self.assertEqual(js.get_msg.await_args_list[0].args, ("KV_generations",))
        self.assertEqual(js.get_msg.await_args_list[0].kwargs, {"subject": "$KV.generations.storm.storm_items", "direct": True})

    async def test_trigger_samples_between_periodic_ticks_and_cleans_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "run-config.json").write_text(json.dumps({"streams": [], "manifest_bucket": "generations",
                "manifest_keys": [], "worker_clients": {"batch2": ["c032"]}}))
            identity = root / "identity"
            identity.write_text(json.dumps({"creds": "SECRET", "principal": "storm-c032"}))
            nc = NS(jetstream=lambda **_: NS(), close=AsyncMock(), is_closed=False)
            connect = AsyncMock(return_value=nc)
            calls = 0
            async def sample(*_):
                nonlocal calls
                calls += 1
                if calls == 1:
                    (root / "signals.jsonl").write_text(json.dumps({"client": None, "worker": "batch2", "reason": "gap stays open"}) + "\n")
                else:
                    (root / "observer.stop").touch()
                return {"streams": {}, "manifests": {}}
            args = NS(run_dir=str(root), identity=str(identity), nats="nats://unused", js_domain="hub", interval=10, t0=time.time(), triggered=True)
            with patch.dict(sys.modules, {"nats": NS(connect=connect)}), patch.object(evidence, "sample", side_effect=sample):
                try:
                    await asyncio.wait_for(evidence.observe(args), timeout=5)
                except TimeoutError:
                    self.fail((root / "retention.jsonl").read_text())
            records = [json.loads(line) for line in (root / "retention.jsonl").read_text().splitlines()]
            self.assertEqual([r["reason"] for r in records], ["periodic", "client_signal"])
            self.assertIn("c032", records[-1]["replica_positions"])
            self.assertFalse(Path(connect.call_args.kwargs["user_credentials"]).exists())
            self.assertNotIn("SECRET", (root / "retention.jsonl").read_text())
            nc.close.assert_awaited_once()

    async def test_observer_crash_does_not_look_like_a_successful_stop(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "run-config.json").write_text(json.dumps({"streams": [], "manifest_bucket": "generations",
                "manifest_keys": [], "worker_clients": {}}))
            (root / "identity").write_text(json.dumps({"creds": "SECRET", "principal": "storm-c032"}))
            nc = NS(jetstream=lambda **_: NS(), close=AsyncMock(), is_closed=False)
            args = NS(run_dir=str(root), identity=str(root / "identity"), nats="unused", js_domain="", interval=10, t0=0, triggered=False)
            with patch.dict(sys.modules, {"nats": NS(connect=AsyncMock(return_value=nc))}), \
                    patch.object(evidence, "sample", side_effect=RuntimeError("fixture sampler failure")), self.assertRaises(RuntimeError):
                await evidence.observe(args)
            self.assertFalse(json.loads((root / "observer.done").read_text())["clean_stop"])
            nc.close.assert_awaited_once()

    async def test_periodic_mode_does_not_add_queries_during_signal_storm(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "run-config.json").write_text(json.dumps({"streams": [], "manifest_bucket": "generations",
                "manifest_keys": [], "worker_clients": {"batch2": ["c032"]}}))
            (root / "identity").write_text(json.dumps({"creds": "SECRET", "principal": "storm-c032"}))
            nc = NS(jetstream=lambda **_: NS(), close=AsyncMock(), is_closed=False)
            sampled = asyncio.Event()
            async def sample(*_):
                (root / "signals.jsonl").write_text((json.dumps({"client": None, "worker": "batch2", "reason": "D2"}) + "\n") * 500)
                sampled.set()
                return {"streams": {}, "manifests": {}}
            args = NS(run_dir=str(root), identity=str(root / "identity"), nats="unused", js_domain="", interval=10, t0=0, triggered=False)
            with patch.dict(sys.modules, {"nats": NS(connect=AsyncMock(return_value=nc))}), patch.object(evidence, "sample", side_effect=sample) as sampler:
                task = asyncio.create_task(evidence.observe(args))
                try:
                    await asyncio.wait_for(sampled.wait(), timeout=2)
                    # Longer than the old one-second triggered-sample grace.
                    await asyncio.sleep(1.2)
                finally:
                    (root / "observer.stop").touch()
                    await asyncio.wait_for(task, timeout=2)
            self.assertEqual(sampler.await_count, 1)
            stopped = json.loads((root / "observer.done").read_text())
            self.assertEqual(stopped["signals_seen"], 500)
            self.assertEqual(stopped["samples"], 1)

    async def test_closed_connection_is_rebuilt_even_if_first_reconnect_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "run-config.json").write_text(json.dumps({"streams": [], "manifest_bucket": "generations",
                "manifest_keys": [], "worker_clients": {}}))
            (root / "identity").write_text(json.dumps({"creds": "SECRET", "principal": "storm-c032"}))
            first = NS(jetstream=lambda **_: "first-js", close=AsyncMock(), is_closed=False)
            second = NS(jetstream=lambda **_: "second-js", close=AsyncMock(), is_closed=False)
            connect = AsyncMock(side_effect=[first, ConnectionError("hub unavailable"), second])
            sessions = []
            async def sample(js, *_):
                sessions.append(js)
                if js == "first-js":
                    first.is_closed = True
                else:
                    (root / "observer.stop").touch()
                return {"streams": {}, "manifests": {}}
            args = NS(run_dir=str(root), identity=str(root / "identity"), nats="unused", js_domain="", interval=0.01, t0=0, triggered=False)
            with patch.dict(sys.modules, {"nats": NS(connect=connect)}), patch.object(evidence, "sample", side_effect=sample):
                await asyncio.wait_for(evidence.observe(args), timeout=5)
            self.assertEqual(sessions, ["first-js", "second-js"])
            self.assertEqual(connect.await_count, 3)
            first.close.assert_awaited_once()
            second.close.assert_awaited_once()
            for call in connect.await_args_list:
                self.assertFalse(Path(call.kwargs["user_credentials"]).exists())


class WorkerTests(Fixture):
    def test_modes_keep_grouped_baseline_and_isolate_diagnostics(self):
        extra = field_follow.extra_clients(100)
        grouped = field_follow.worker_groups(extra, False)
        isolated = field_follow.worker_groups(extra, True)
        self.assertEqual(len(grouped), 15)
        self.assertEqual(len(isolated), 100)
        self.assertTrue(all(len(group) == 1 for _, group, _ in isolated))
        self.assertEqual(sum(measured for _, _, measured in isolated), 5)
        self.assertEqual({c["name"] for _, g, _ in grouped for c in g}, {c["name"] for _, g, _ in isolated for c in g})

    def test_handle_closes_before_final_evidence_and_result(self):
        rows = [("a", "n", "v")]
        self.database(rows)
        done = self.truth(rows)
        (self.root / "done").write_text(json.dumps(done))
        (self.root / "identity").write_text("{}")
        closed = []
        lib = NS(zb_client_connect=lambda _: 42, zb_client_sync=lambda _: {}, zb_client_poll=lambda *_: {},
                 zb_client_close=lambda h: closed.append(h))
        args = NS(name="c032", identity=str(self.root / "identity"), tenant="storm", schedule="[[0,null]]",
                  t0=0, db=str(self.db), nats="nats://unused", seed_domain="", done_file=str(self.root / "done"))
        finalize = evidence.finalize_replica
        def finish(*a):
            self.assertEqual(closed, [42])
            return finalize(*a)
        output = io.StringIO()
        with patch.object(field_worker, "finalize_replica", side_effect=finish), redirect_stdout(output):
            field_worker.run_client(args, lib, lambda p: p, threading.Lock(), False)
        final = json.loads(output.getvalue().splitlines()[-1])
        self.assertTrue(final["exact"])
        self.assertEqual(closed, [42])

    def test_unexpected_poll_error_closes_handle_without_deleting_evidence(self):
        self.database([("a", "n", "v")])
        (self.root / "identity").write_text("{}")
        lib = NS(zb_client_connect=lambda _: 42, zb_client_sync=lambda _: {},
                 zb_client_poll=lambda *_: (_ for _ in ()).throw(RuntimeError("fixture error")), zb_client_close=lambda _: None)
        args = NS(name="c032", identity=str(self.root / "identity"), tenant="storm", schedule="[[0,null]]",
                  t0=0, db=str(self.db), nats="unused", seed_domain="", done_file=str(self.root / "done"))
        with patch.object(lib, "zb_client_close") as close, redirect_stdout(io.StringIO()), self.assertRaises(RuntimeError):
            field_worker.run_client(args, lib, lambda p: p, threading.Lock(), False)
        close.assert_called_once_with(42)
        self.assertTrue(self.db.exists())


@unittest.skipUnless(os.environ.get("ZB_FIELD_TEST_PG_BIN"), "set ZB_FIELD_TEST_PG_BIN for a private PostgreSQL cluster")
class PostgreSQLTruthTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import getpass
        cls.tmp = tempfile.TemporaryDirectory(prefix="zb-field-truth-")
        cls.addClassCleanup(cls.tmp.cleanup)
        cls.root = Path(cls.tmp.name)
        cls.pg = Path(os.environ["ZB_FIELD_TEST_PG_BIN"])
        cls.data, cls.sock = cls.root / "data", cls.root / "socket"
        cls.sock.mkdir()
        try:
            subprocess.run([str(cls.pg / "initdb"), "-D", str(cls.data), "--auth=trust", "--no-locale", "--encoding=UTF8"],
                           check=True, capture_output=True, text=True)
            subprocess.run([str(cls.pg / "pg_ctl"), "-D", str(cls.data), "-l", str(cls.root / "postgres.log"),
                            "-o", f"-k {cls.sock} -c listen_addresses=''", "-w", "start"],
                           check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as exc:
            log = cls.root / "postgres.log"
            detail = log.read_text() if log.exists() else exc.stderr
            raise RuntimeError(detail) from exc
        cls.env = {**os.environ, "PGHOST": str(cls.sock), "PGPORT": "5432", "PGUSER": getpass.getuser(), "PGDATABASE": "postgres"}
        cls.env["SB_ADMIN_URL"] = "postgresql:///postgres"

    @classmethod
    def tearDownClass(cls):
        try:
            subprocess.run([str(cls.pg / "pg_ctl"), "-D", str(cls.data), "-m", "immediate", "-w", "stop"],
                           check=True, capture_output=True)
        finally:
            cls.tmp.cleanup()

    def sql(self, sql):
        return subprocess.run([str(self.pg / "psql"), "-X", "-At", "-v", "ON_ERROR_STOP=1", "-c", sql],
                              env=self.env, check=True, capture_output=True, text=True).stdout

    def test_real_export_preserves_utf8_null_versions_and_empty_snapshot(self):
        self.sql("CREATE TABLE storm_items(id text PRIMARY KEY, tenant_id text, note text, updated_at timestamptz, deleted_at timestamptz);")
        self.sql("""INSERT INTO storm_items VALUES
          ('a', 'storm', NULL, '2026-10-25 00:00:00.123456Z', NULL),
          ('b', 'storm', E'é\\n', '2026-10-25 00:00:01Z', NULL),
          ('c', 'storm', '', '2026-10-25 00:00:02Z', NULL),
          ('deleted', 'storm', 'gone', now(), now()), ('other', 'elsewhere', 'hidden', now(), NULL);""")
        out = self.root / "truth.jsonl"
        done = evidence.freeze_truth(self.env, out, "storm_items", "storm")
        _, rows = evidence.read_truth(out, done["truth_sha256"])
        expected = [("a", None), ("b", "é\n"), ("c", "")]
        h = hashlib.sha256()
        for key, note in expected:
            h.update(evidence.frame(key, note))
            self.assertEqual(rows[key]["value_digest"], hashlib.sha256(evidence.value_frame(note)).hexdigest())
        self.assertEqual(done["rows"], 3)
        self.assertEqual(done["digest"], h.hexdigest())
        self.assertIn(".123456", rows["a"]["version"])
        self.assertEqual(evidence.version_instant(rows["a"]["version"]),
                         evidence.version_instant("2026-10-25T00:00:00.123456Z"))
        self.sql("UPDATE storm_items SET note = 'changed after snapshot'")
        self.assertEqual(evidence.read_truth(out)[0]["digest"], h.hexdigest())
        self.sql("DELETE FROM storm_items")
        empty = evidence.freeze_truth(self.env, self.root / "empty.jsonl", "storm_items", "storm")
        self.assertEqual(empty["rows"], 0)
        self.assertEqual(empty["digest"], hashlib.sha256(b"").hexdigest())


if __name__ == "__main__":
    unittest.main()
