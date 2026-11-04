"""archive.py's own wiring, without PostgreSQL, NATS or S3.

Run with the scenario Python dependencies: python -m unittest discover -s scripts/scenarios -p test_archive_readers.py
The archiver's coverage regressions are the library's (zb-archiver/tests); the real-stream
sequence-hole cases live in readings_fetch.py.
"""
import importlib.util
import pathlib
import sys
import types
import unittest
from unittest import mock

REPO = pathlib.Path(__file__).resolve().parents[2]
import zb


class ScenarioReaderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("archive_scenario", REPO / "scripts/scenarios/archive.py")
        cls.scenario = importlib.util.module_from_spec(spec)
        with mock.patch.object(zb, "creds_for", return_value="/fixture/creds"), \
             mock.patch.object(zb, "nats_server", return_value="nats://127.0.0.1:1"):
            spec.loader.exec_module(cls.scenario)

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
