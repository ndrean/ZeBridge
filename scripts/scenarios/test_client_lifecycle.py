"""Never two client processes on one replica (NOTES "The joint fence": the third-table mismatch was the harness
restarting a client whose abort never came). Real child processes, no stack."""
import pathlib, subprocess, sys, tempfile, unittest
from unittest.mock import patch

import recreate_recovery as rr

REAL_POPEN = subprocess.Popen
STUBBORN = "import time\nwhile True: time.sleep(0.1)\n"   # never looks at its stop file, never aborts


class ClientLifecycleTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        c = rr.Client.__new__(rr.Client)
        c.db, c.log, c.p, c.tables = f"{tmp.name}/r.sqlite3", f"{tmp.name}/r.log", None, [rr.T]
        c.stack = type("S", (), dict(url="nats://x", creds={"p": "c"}, tag="t"))()
        c.name, c.principal, c.tenant, c.chunk_rows, c.lib, c.pg_url = "c", "p", "t", None, "lib", None
        c.STOP_WAIT = 0.3
        self.c, self.spawned = c, []
        self.addCleanup(self.reap)

    def reap(self):
        for p in self.spawned:
            if p.poll() is None:
                p.kill()
            p.wait()

    def popen(self, *a, **kw):
        p = REAL_POPEN([sys.executable, "-c", STUBBORN])
        self.spawned.append(p)
        return p

    def test_stop_reaps_a_killed_process_before_returning(self):
        self.c.p = self.popen()
        first = self.c.p
        self.c.stop()
        self.assertIsNone(self.c.p)
        self.assertIsNotNone(first.returncode, "stop() returned while the killed process was not yet waited for")

    def test_start_twice_leaves_one_process_when_the_first_never_exits(self):
        with patch.object(rr.subprocess, "Popen", self.popen):
            self.c.start()
            first = self.c.p
            self.assertIsNone(first.poll())   # no abort came
            self.c.start()
        self.assertIsNotNone(first.returncode, "the first process was still alive beside its replacement")
        self.assertIsNone(self.c.p.poll())
        self.assertEqual([p.poll() is None for p in self.spawned], [False, True])


if __name__ == "__main__":
    unittest.main()
