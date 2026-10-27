"""Archive coverage regressions without PostgreSQL, NATS or S3.

Run with the scenario Python dependencies: python -m unittest discover -s scripts/scenarios -p test_archive_readers.py
The real-stream sequence-hole cases live in readings_fetch.py.
"""
import asyncio
import copy
import importlib.util
import io
import pathlib
import sys
import types
import unittest
from contextlib import redirect_stdout
from unittest import mock

REPO = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "examples/16-archive"))
import archiver
import zb


class CoverageTests(unittest.IsolatedAsyncioTestCase):
    async def native(self, summaries, commit_code=0):
        manifest = {"covers_up_to": 0, "files": [], "schema": 1}
        saved, starts = [], []
        head = summaries[-1]["last_seq"]
        chunks = iter(summaries)

        async def fetch(_bin, _args, start, _out):
            starts.append(start)
            return next(chunks, dict(messages=0, rows=0, first_seq=0, last_seq=0)), None

        async def commit(_js, _tenant, current, rev):
            if not commit_code:
                saved.append(copy.deepcopy(current))
            return commit_code, (rev or 0) + 1

        def flush(_args, _s3, current, _csv, summary):
            current["files"].append({"last_seq": summary["last_seq"]})
            current["covers_up_to"] = summary["last_seq"]
            return 0

        js = types.SimpleNamespace(stream_info=mock.AsyncMock(return_value=types.SimpleNamespace(
            state=types.SimpleNamespace(last_seq=head))))
        nc = types.SimpleNamespace(close=mock.AsyncMock())
        args = types.SimpleNamespace(tenant="acme", batch=2, flush_s=0, exit_when_idle=.001, accept_loss=False)
        with mock.patch.object(archiver, "resolve_fetch", return_value="unused"), \
             mock.patch.object(archiver.common, "s3", return_value=None), \
             mock.patch.object(archiver, "fetch_chunk", side_effect=fetch), \
             mock.patch.object(archiver, "commit", side_effect=commit), \
             mock.patch.object(archiver, "flush_csv", side_effect=flush) as parquet, \
             mock.patch.object(asyncio.get_running_loop(), "add_signal_handler"), redirect_stdout(io.StringIO()):
            code = await archiver.archive_loop(args, nc, js, "READINGS_acme", manifest, None, 1, "unused")
        return code, saved, starts, parquet

    async def test_native_skipped_only_chunk_is_durable_without_parquet(self):
        code, saved, starts, parquet = await self.native([
            dict(messages=1, rows=0, first_seq=1, last_seq=1)])
        self.assertEqual(code, 0)
        self.assertEqual([m["covers_up_to"] for m in saved], [1])
        self.assertEqual(saved[0]["files"], [])
        self.assertEqual(starts, [1])
        parquet.assert_not_called()

    async def test_skipped_chunk_commits_before_a_later_reading(self):
        code, saved, starts, parquet = await self.native([
            dict(messages=2, rows=0, first_seq=1, last_seq=2),
            dict(messages=1, rows=1, first_seq=3, last_seq=3)])
        self.assertEqual(code, 0)
        self.assertEqual([m["covers_up_to"] for m in saved], [2, 3])
        self.assertEqual(starts, [1, 3])
        self.assertEqual(parquet.call_count, 1)

    async def test_skipped_chunk_honours_manifest_cas_failure(self):
        code, saved, _, parquet = await self.native([
            dict(messages=1, rows=0, first_seq=1, last_seq=1)], commit_code=3)
        self.assertEqual(code, 3)
        self.assertEqual(saved, [])
        parquet.assert_not_called()

    async def test_incomplete_summary_cannot_upload_or_advance_manifest(self):
        code, saved, _, parquet = await self.native([
            dict(messages=2, rows=2, first_seq=1, last_seq=3)])
        self.assertEqual(code, 4)
        self.assertEqual(saved, [])
        parquet.assert_not_called()



class ScenarioReaderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("archive_scenario", REPO / "scripts/scenarios/archive.py")
        cls.scenario = importlib.util.module_from_spec(spec)
        with mock.patch.object(zb, "creds_for", return_value="/fixture/creds"), \
             mock.patch.object(zb, "nats_server", return_value="nats://127.0.0.1:1"):
            spec.loader.exec_module(cls.scenario)

    def test_missing_fetcher_fails_before_connect(self):
        with mock.patch.object(sys, "argv", ["archiver.py", "--tenant", "acme"]), \
             mock.patch.object(archiver, "resolve_fetch", return_value=None), \
             mock.patch.object(archiver.common, "connect", mock.AsyncMock()) as connect:
            self.assertEqual(asyncio.run(archiver.main()), 1)
        connect.assert_not_awaited()

    def test_native_reaches_crash_and_competing_writer_processes(self):
        self.scenario.configure_archive(["--fetch-bin", "/fixture/fetch"])
        result = types.SimpleNamespace(returncode=0, stdout="", stderr="")
        with mock.patch.object(self.scenario.subprocess, "run", return_value=result) as run, \
             mock.patch.object(self.scenario.subprocess, "Popen") as popen:
            self.scenario.archiver("--crash-after-upload")
            self.scenario.archiver(wait=False)
            self.scenario.archiver("--flush-s", "2", wait=False)
        for call in [run.call_args, *popen.call_args_list]:
            cmd = call.args[0]
            self.assertNotIn("--reader", cmd)
            self.assertEqual(cmd[cmd.index("--fetch-bin") + 1], "/fixture/fetch")
        self.scenario.configure_archive([])


if __name__ == "__main__":
    unittest.main()
