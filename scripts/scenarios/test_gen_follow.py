"""Focused tests for the generation-follow benchmark's measurement contract."""
import json
import subprocess
import unittest
from unittest.mock import patch

import gen_follow


class MeasurementTests(unittest.TestCase):
    def lookup(self, reply, code=0):
        result = subprocess.CompletedProcess([], code, json.dumps(reply), "")
        with patch.object(gen_follow.subprocess, "run", return_value=result):
            return gen_follow.catchup_targets("nats://fixture")

    def test_only_explicit_message_not_found_is_an_empty_target(self):
        self.assertEqual(set(self.lookup({"error": {"code": 404, "err_code": 10037}}).values()), {0})
        self.assertEqual(set(self.lookup({"message": {"seq": 42}}).values()), {42})

    def test_failed_or_malformed_target_lookup_invalidates_measurement(self):
        for reply, code in [({}, 0), ({"error": {"code": 404, "err_code": 10059}}, 0),
                            ({"message": {"seq": 42}}, 1), ({"message": {"seq": True}}, 0),
                            ({"message": {"seq": -1}}, 0), ([], 0),
                            ({"error": {"code": 404, "err_code": 10037}}, 1)]:
            with self.subTest(reply=reply, code=code), self.assertRaises(RuntimeError):
                self.lookup(reply, code)
        with patch.object(gen_follow.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "not JSON", "")):
            with self.assertRaises(RuntimeError):
                gen_follow.catchup_targets("nats://fixture")

    def test_position_completion_is_not_reported_as_replica_readiness(self):
        events = [{"event": "leg", "leg": 0, "start_ms": 1000, "targets": {"CDC_acme": 10}, "cold": True},
                  {"event": "caught_up", "leg": 0, "catchup_s": 0.5}]
        counts = {"m": "catchup", "stream": "CDC_acme", "target": 10, "via": "position", "at_ms": 1200,
                  "messages": 1, "events": 2, "applied": 0, "gated": 0, "held": 1, "failed": 1}
        row = gen_follow.seeding_report("fixture", events, "measure " + json.dumps(counts))[0]
        self.assertNotIn("usable_s", row)
        self.assertEqual(row["target_catchup_s"], 0.5)
        self.assertEqual(row["streams"]["CDC_acme"]["held"], 1)
        self.assertEqual(row["streams"]["CDC_acme"]["failed"], 1)
        row = gen_follow.seeding_report("fixture", events, "")[0]
        self.assertIsNone(row["target_catchup_s"])


if __name__ == "__main__":
    unittest.main()
