"""The comparison must reject matching counts with different data."""
import datetime
import importlib.util
from pathlib import Path
import sqlite3
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("seed_compare", Path(__file__).with_name("seed.py"))
seed = importlib.util.module_from_spec(spec)
spec.loader.exec_module(seed)


class VerificationTests(unittest.TestCase):
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
