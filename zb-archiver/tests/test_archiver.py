"""Archive coverage regressions without NATS, S3 or the fetcher.

    cd zb-archiver && python -m unittest discover -s tests

The real-stream cases (sequence holes, chunk sizes, a kill between upload and manifest, two
archivers) are scenarios: scripts/scenarios/readings_fetch.py and archive.py.
"""
import asyncio
import copy
import io
import pathlib
import sys
import types
import unittest
from contextlib import redirect_stdout
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))
from zb_archiver import ArchiverConfig, ExitCode, archiver, cli  # noqa: E402


class CoverageTests(unittest.IsolatedAsyncioTestCase):
    async def native(self, summaries, commit_code=0):
        manifest = {"covers_up_to": 0, "files": [], "schema": 1}
        saved, starts = [], []
        head = summaries[-1]["last_seq"]
        chunks = iter(summaries)

        async def fetch(_bin, _cfg, start, _out):
            starts.append(start)
            return next(chunks, dict(messages=0, rows=0, first_seq=0, last_seq=0)), None

        async def commit(_js, _cfg, current, rev):
            if not commit_code:
                saved.append(copy.deepcopy(current))
            return commit_code, (rev or 0) + 1

        def flush(_cfg, _s3, current, _csv, summary):
            current["files"].append({"last_seq": summary["last_seq"]})
            current["covers_up_to"] = summary["last_seq"]
            return 0

        js = types.SimpleNamespace(stream_info=mock.AsyncMock(return_value=types.SimpleNamespace(
            state=types.SimpleNamespace(last_seq=head))))
        cfg = ArchiverConfig(tenant="acme", batch=2, flush_s=0, exit_when_idle=.001)
        with mock.patch.object(archiver.env, "s3", return_value=None), \
             mock.patch.object(archiver, "fetch_chunk", side_effect=fetch), \
             mock.patch.object(archiver, "commit", side_effect=commit), \
             mock.patch.object(archiver, "flush_csv", side_effect=flush) as parquet, \
             redirect_stdout(io.StringIO()):
            code = await archiver.archive_loop(cfg, None, js, "READINGS_acme", manifest, None, 1, "unused", asyncio.Event())
        return code, saved, starts, parquet

    async def test_native_skipped_only_chunk_is_durable_without_parquet(self):
        code, saved, starts, parquet = await self.native([
            dict(messages=1, rows=0, first_seq=1, last_seq=1)])
        self.assertEqual(code, ExitCode.STOPPED)
        self.assertEqual([m["covers_up_to"] for m in saved], [1])
        self.assertEqual(saved[0]["files"], [])
        self.assertEqual(starts, [1])
        parquet.assert_not_called()

    async def test_skipped_chunk_commits_before_a_later_reading(self):
        code, saved, starts, parquet = await self.native([
            dict(messages=2, rows=0, first_seq=1, last_seq=2),
            dict(messages=1, rows=1, first_seq=3, last_seq=3)])
        self.assertEqual(code, ExitCode.STOPPED)
        self.assertEqual([m["covers_up_to"] for m in saved], [2, 3])
        self.assertEqual(starts, [1, 3])
        self.assertEqual(parquet.call_count, 1)

    async def test_skipped_chunk_honours_manifest_cas_failure(self):
        code, saved, _, parquet = await self.native([
            dict(messages=1, rows=0, first_seq=1, last_seq=1)], commit_code=ExitCode.MANIFEST_MOVED)
        self.assertEqual(code, ExitCode.MANIFEST_MOVED)
        self.assertEqual(saved, [])
        parquet.assert_not_called()

    async def test_incomplete_summary_cannot_upload_or_advance_manifest(self):
        code, saved, _, parquet = await self.native([
            dict(messages=2, rows=2, first_seq=1, last_seq=3)])
        self.assertEqual(code, ExitCode.READINGS_LOST)
        self.assertEqual(saved, [])
        parquet.assert_not_called()

    async def test_a_pruned_stream_stops_before_any_write(self):
        code, saved, _, parquet = await self.native([
            dict(messages=1, rows=1, first_seq=5, last_seq=5)])
        self.assertEqual(code, ExitCode.READINGS_LOST)
        self.assertEqual(saved, [])
        parquet.assert_not_called()


class EntryTests(unittest.TestCase):
    def test_missing_fetcher_fails_before_connect(self):
        with mock.patch.object(archiver, "resolve_fetch", return_value=None), \
             mock.patch.object(archiver.env, "connect", mock.AsyncMock()) as connect:
            self.assertEqual(cli.main(["--tenant", "acme"]), ExitCode.FAILED)
        connect.assert_not_awaited()

    def test_the_command_line_fills_the_config(self):
        cfg = cli.parse(["--tenant", "acme", "--flush-s", "2", "--batch", "7", "--accept-loss", "--fetch-bin", "/x"])
        self.assertEqual((cfg.tenant, cfg.flush_s, cfg.batch, cfg.accept_loss, cfg.fetch_bin), ("acme", 2, 7, True, "/x"))

    def test_fetcher_gets_the_configured_connection(self):
        seen = {}

        async def exec_(*cmd, env=None, **_):
            seen.update(cmd=cmd, env=env)
            return types.SimpleNamespace(returncode=0, communicate=mock.AsyncMock(return_value=(b'{"messages":0}', b"")))

        cfg = ArchiverConfig(tenant="acme", batch=3, nats_url="nats://h:1")
        with mock.patch.object(archiver.asyncio, "create_subprocess_exec", side_effect=exec_):
            summary, why = asyncio.run(archiver.fetch_chunk("/f", cfg, 11, "/o.csv"))
        self.assertEqual((summary, why), ({"messages": 0}, None))
        self.assertEqual(seen["cmd"][:9], ("/f", "--tenant", "acme", "--from", "11", "--max", "3", "--out", "/o.csv"))
        self.assertEqual(seen["env"]["NATS_URL"], "nats://h:1")


class FakeS3:
    def __init__(self, shrink=0):
        self.objects, self.shrink = {}, shrink

    def upload_file(self, path, bucket, key):
        self.objects[(bucket, key)] = pathlib.Path(path).read_bytes()

    def head_object(self, Bucket, Key):
        return {"ContentLength": len(self.objects[(Bucket, Key)]) - self.shrink}


class ParquetTests(unittest.TestCase):
    CSV = ('"acme","s1","temp",1.5,1700000000000,1700000000001,1\n'
           '"acme","s1","temp",2.5,1700000000000,1700000000002,2\n'   # resent: the later one stays
           '"acme","s,2","temp",3.0,1700000000500,1700000000003,3\n')
    SUMMARY = dict(messages=3, rows=3, first_seq=1, first_row_seq=1, last_seq=3, first_ms=1700000000000)

    def flush(self, s3, **kw):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            csv = pathlib.Path(d, "c.csv")
            csv.write_text(self.CSV)
            manifest = {"covers_up_to": 0, "files": [], "schema": 1}
            cfg = ArchiverConfig(tenant="acme", bucket="b", say=lambda *a, **k: None, **kw)
            return archiver.flush_csv(cfg, s3, manifest, str(csv), self.SUMMARY), manifest

    def test_a_chunk_becomes_one_deduplicated_parquet_file(self):
        import duckdb, tempfile
        s3 = FakeS3()
        code, manifest = self.flush(s3)
        self.assertEqual(code, ExitCode.STOPPED)
        key = "acme/readings/2023-11-14/000000000001-000000000003.parquet"
        self.assertEqual(manifest["files"], [{"key": key, "rows": 2, "bytes": len(s3.objects[("b", key)]),
                                              "first_seq": 1, "last_seq": 3}])
        self.assertEqual(manifest["covers_up_to"], 3)
        with tempfile.TemporaryDirectory() as d:
            f = pathlib.Path(d, "x.parquet")
            f.write_bytes(s3.objects[("b", key)])
            rows = duckdb.sql(f"SELECT sensor_id, value, stream_seq FROM read_parquet('{f}') ORDER BY stream_seq").fetchall()
        self.assertEqual(rows, [("s1", 2.5, 2), ("s,2", 3.0, 3)])

    def test_a_short_upload_leaves_the_manifest(self):
        code, manifest = self.flush(FakeS3(shrink=1))
        self.assertEqual(code, ExitCode.UPLOAD_MISMATCH)
        self.assertEqual(manifest, {"covers_up_to": 0, "files": [], "schema": 1})


class StartTests(unittest.IsolatedAsyncioTestCase):
    async def start(self, manifest, first_seq, max_age=0, messages=10, flush_s=60, outage_s=3600, accept_loss=False):
        info = types.SimpleNamespace(config=types.SimpleNamespace(max_age=max_age),
                                     state=types.SimpleNamespace(first_seq=first_seq, messages=messages))
        nc = types.SimpleNamespace(close=mock.AsyncMock(), jetstream=lambda: None)
        loop = mock.AsyncMock(return_value=ExitCode.STOPPED)
        cfg = ArchiverConfig(tenant="acme", fetch_bin=__file__, flush_s=flush_s, outage_s=outage_s,
                             accept_loss=accept_loss, say=lambda *a, **k: None)
        with mock.patch.object(archiver.env, "connect", mock.AsyncMock(return_value=nc)), \
             mock.patch.object(archiver.env, "stream_when_there", mock.AsyncMock(return_value=info)), \
             mock.patch.object(archiver.layout, "read_manifest", mock.AsyncMock(return_value=manifest)), \
             mock.patch.object(archiver, "archive_loop", loop):
            code = await archiver.run(cfg)
        nc.close.assert_awaited()
        return code, loop

    async def test_retention_shorter_than_two_flushes_and_an_outage_refuses(self):
        code, loop = await self.start((None, None), 1, max_age=3719, flush_s=60, outage_s=3600)
        self.assertEqual(code, ExitCode.RETENTION_SHORT)
        loop.assert_not_awaited()

    async def test_a_stream_pruned_past_the_archive_refuses(self):
        code, loop = await self.start(({"covers_up_to": 4, "files": [], "schema": 1}, 7), 6)
        self.assertEqual(code, ExitCode.READINGS_LOST)
        loop.assert_not_awaited()

    async def test_accept_loss_follows_from_the_stream_start(self):
        code, loop = await self.start(({"covers_up_to": 4, "files": [], "schema": 1}, 7), 6, accept_loss=True)
        self.assertEqual(code, ExitCode.STOPPED)
        self.assertEqual(loop.await_args.args[5:7], (7, 6))   # the revision read, the first sequence

    async def test_a_new_tenant_starts_at_the_stream_start(self):
        code, loop = await self.start((None, None), 6)
        self.assertEqual(loop.await_args.args[4], {"covers_up_to": 0, "files": [], "schema": 1})
        self.assertEqual(loop.await_args.args[6], 6)


if __name__ == "__main__":
    unittest.main()
