"""The owns guard excludes admission, never a competing worker; no live processes used."""
import subprocess
import unittest
from unittest.mock import patch

import zb


class WorkerGuardTests(unittest.TestCase):
    def guard(self, pids, commands="", code=0):
        results = [subprocess.CompletedProcess([], 0, pids, "")]
        if pids.strip():
            results.append(subprocess.CompletedProcess([], code, commands, ""))
        with patch.object(zb.subprocess, "run", side_effect=results) as run:
            found = zb.another_bridge_running()
        if pids.strip():
            self.assertEqual(run.call_args_list[1].args[0],
                             ["ps", "-ww", "-p", ",".join(pids.split()), "-o", "args="])
        return found

    def test_no_matching_bridge(self):
        self.assertFalse(self.guard(""))

    def test_admission_alone_is_allowed(self):
        self.assertFalse(self.guard("12\n", "zig-out/bin/bridge --mutation-ingress\n"))

    def test_worker_without_arguments_is_refused(self):
        self.assertTrue(self.guard("12\n", "zig-out/bin/bridge\n"))

    def test_worker_with_arguments_is_refused(self):
        self.assertTrue(self.guard("12\n", "/repo/zig-out/bin/bridge --slot zb_probe --port 9096\n"))

    def test_admission_does_not_hide_a_worker(self):
        self.assertTrue(self.guard("12\n13\n", "zig-out/bin/bridge --mutation-ingress\nzig-out/bin/bridge --pub zb_pub\n"))

    def test_only_an_exact_admission_option_is_exempt(self):
        self.assertTrue(self.guard("12\n", "zig-out/bin/bridge --pub mutation-ingress\n"))

    def test_failed_process_inspection_refuses(self):
        self.assertTrue(self.guard("12\n", code=1))

    def test_unparseable_process_inspection_refuses(self):
        self.assertTrue(self.guard("12\n", 'zig-out/bin/bridge "unterminated\n'))


if __name__ == "__main__":
    unittest.main()
