import json
import os
import tempfile
import unittest
from unittest import mock

import importlib.util

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("alert", os.path.join(HERE, "orphan-dryrun-alert.py"))
alert = importlib.util.module_from_spec(spec)
spec.loader.exec_module(alert)

LOG = (
    "noise\n"
    "cftc/cftc_trades metadata: on disk 78207, reachable 293, orphans 77914, inside safety window 4, "
    "would delete 77910 (12377126761 bytes)\n"
    "econ/fred_indicators metadata: on disk 5941, reachable 5866, orphans 75, inside safety window 7, "
    "would delete 68 (548661 bytes)\n"
    "sec/filing_metadata metadata: skipped, commit lock held\n")


class ParseTest(unittest.TestCase):
    def test_parses_results_and_ignores_skipped(self):
        r = alert.parse(LOG)
        self.assertEqual(set(r), {"cftc/cftc_trades", "econ/fred_indicators"})
        self.assertEqual(r["cftc/cftc_trades"]["deletable"], 77910)
        self.assertEqual(r["cftc/cftc_trades"]["bytes"], 12377126761)


class ThresholdTest(unittest.TestCase):
    def res(self, n):
        return {"t": {"deletable": n}}

    def test_at_threshold_is_not_a_breach(self):
        self.assertEqual(alert.evaluate(self.res(1000), [], 1000), ([], []))

    def test_over_threshold_is_new_breach(self):
        self.assertEqual(alert.evaluate(self.res(1001), [], 1000), (["t"], ["t"]))

    def test_staying_over_is_not_reported_again(self):
        self.assertEqual(alert.evaluate(self.res(5000), ["t"], 1000), ([], ["t"]))

    def test_falling_back_rearms(self):
        self.assertEqual(alert.evaluate(self.res(10), ["t"], 1000), ([], []))
        self.assertEqual(alert.evaluate(self.res(5000), [], 1000), (["t"], ["t"]))

    def test_absent_table_keeps_state(self):
        self.assertEqual(alert.evaluate({}, ["t"], 1000), ([], ["t"]))


class MainTest(unittest.TestCase):
    def run_main(self, d, log_text):
        log = os.path.join(d, "dry.log")
        with open(log, "w") as f:
            f.write(log_text)
        return alert.main(["--log", log, "--state-dir", d, "--threshold", "1000"])

    def test_one_issue_per_breach_and_state_persisted(self):
        with tempfile.TemporaryDirectory() as d, mock.patch.object(alert, "file_issue",
                                                                  return_value="#1") as fi:
            self.assertEqual(self.run_main(d, LOG), 0)
            self.assertEqual([c.args[0] for c in fi.call_args_list], ["cftc/cftc_trades"])
            self.assertEqual(self.run_main(d, LOG), 0)
            self.assertEqual(fi.call_count, 1)  # still over: not filed again
            with open(os.path.join(d, "breached.json")) as f:
                self.assertEqual(json.load(f), ["cftc/cftc_trades"])
            self.assertTrue(os.path.exists(os.path.join(d, "latest.json")))
            self.assertEqual(len(open(os.path.join(d, "history.tsv")).read().splitlines()), 4)

    def test_failed_filing_is_retried_next_run(self):
        import subprocess
        with tempfile.TemporaryDirectory() as d:
            with mock.patch.object(alert, "file_issue",
                                   side_effect=subprocess.CalledProcessError(1, "gh")):
                self.assertEqual(self.run_main(d, LOG), 1)
            with mock.patch.object(alert, "file_issue", return_value="#2") as fi:
                self.assertEqual(self.run_main(d, LOG), 0)
                self.assertEqual(fi.call_count, 1)

    def test_log_without_results_fails(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(self.run_main(d, "timeout\n"), 1)


if __name__ == "__main__":
    unittest.main()
