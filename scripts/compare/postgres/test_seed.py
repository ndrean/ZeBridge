"""The comparison must reject matching counts with different data."""
import datetime
import asyncio
import importlib.util
from pathlib import Path
import sqlite3
import tempfile
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch

spec = importlib.util.spec_from_file_location("seed_compare", Path(__file__).with_name("seed.py"))
seed = importlib.util.module_from_spec(spec)
spec.loader.exec_module(seed)


class VerificationTests(unittest.TestCase):
    def test_preparation_waits_for_readable_metadata_and_closes_connection(self):
        missing = type("NotFoundError", (Exception,), {})
        errors = types.ModuleType("nats.js.errors")
        errors.NotFoundError = errors.KeyNotFoundError = missing
        visited = []

        async def get(key):
            visited.append(key)
            if len(visited) == 1:
                raise missing()
            return types.SimpleNamespace(value=b"ready")

        js = types.SimpleNamespace(key_value=AsyncMock(return_value=types.SimpleNamespace(get=get)))
        connection = types.SimpleNamespace(jetstream=lambda **kw: js, close=AsyncMock())
        nats = types.ModuleType("nats")
        nats.connect = AsyncMock(return_value=connection)
        stack = types.SimpleNamespace(url="nats://test", bridge_creds="unused")
        with patch.dict(sys.modules, {"nats": nats, "nats.js": types.ModuleType("nats.js"),
                                      "nats.js.errors": errors}), patch.object(seed.asyncio, "sleep", new_callable=AsyncMock):
            asyncio.run(seed.wait_seed_metadata(stack, 1))
        self.assertEqual(visited, [seed.TABLE, seed.TABLE, seed.PRINCIPAL, f"{seed.TENANT}.{seed.TABLE}"])
        connection.close.assert_awaited_once()

    def test_enrollment_timing_requires_one_real_enrollment(self):
        line = "info(libzb): zebridge: enrolled in 7 ms (the bridge's /enroll, the identity saved)\n"
        self.assertEqual(seed.enrollment_duration(line), 0.007)
        for invalid in ("", "JWT renewed in 7 ms", line + line):
            with self.subTest(invalid=invalid), self.assertRaises(RuntimeError):
                seed.enrollment_duration(invalid)

    def test_null_empty_and_typed_values_are_distinct(self):
        values = [None, "", "\n", 1, "1", "é\u0000x"]
        self.assertEqual(len(values), len({seed.fingerprint([(v,)])["sha256"] for v in values}))

    def test_timestamps_compare_by_utc_instant(self):
        instant = datetime.datetime.fromisoformat("2026-01-01T02:00:00.123456+02:00")
        self.assertEqual(seed.fingerprint([(instant,)]),
                         seed.fingerprint([("2026-01-01T00:00:00.123456Z",)]))

    def test_same_count_corruption_fails_after_reopen(self):
        row = ("one", "seedbench", "original", 7, "2026-01-01T00:00:00.123456Z", None)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "local.sqlite3"
            with sqlite3.connect(path) as db:
                db.execute("CREATE TABLE seed_items (id TEXT PRIMARY KEY, tenant_id TEXT, note TEXT, "
                           "quantity INTEGER, updated_at TEXT, deleted_at TEXT) STRICT")
                db.execute("INSERT INTO seed_items VALUES (?, ?, ?, ?, ?, ?)", row)
            self.assertEqual(seed.verify(path, seed.fingerprint([row]))["rows"], 1)
            with sqlite3.connect(path) as db:
                db.execute("UPDATE seed_items SET note = 'corrupted'")
            with self.assertRaisesRegex(RuntimeError, "local contents differ"):
                seed.verify(path, seed.fingerprint([row]))


if __name__ == "__main__":
    unittest.main()
