import csv
import importlib.util
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("summarize_results", ROOT / "scripts/summarize_results.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class SummarizeResultsTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "results.jtl"

    def write_rows(self, rows):
        fields = sorted(MODULE.REQUIRED)
        with self.path.open("w", encoding="utf-8", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=fields)
            writer.writeheader()
            for start, elapsed, success, acknowledged, attempted, first in rows:
                writer.writerow({
                    "timeStamp": start, "elapsed": elapsed, "success": success,
                    "iotdb_ack_rows": acknowledged, "iotdb_ack_points": acknowledged * 2,
                    "iotdb_attempted_rows": attempted, "iotdb_device": "root.test.device",
                    "iotdb_first_timestamp": first, "iotdb_last_timestamp": first + 4,
                })

    def test_overlapping_threads_use_wall_clock_not_sum_of_elapsed(self):
        self.write_rows([(1000, 1000, "true", 5, 5, 0), (1100, 1000, "false", 2, 4, 5)])
        result = MODULE.summarize([self.path])
        self.assertEqual(result["acknowledged_points"], 14)
        self.assertEqual(result["failed_samples"], 1)
        self.assertEqual(result["unconfirmed_rows"], 2)
        self.assertAlmostEqual(result["window_seconds"], 1.1)
        self.assertAlmostEqual(result["acknowledged_points_per_second"], 14 / 1.1)
        self.assertEqual(result["possible_overlapping_timestamp_ranges"], 0)

    def test_explicit_window_excludes_crossing_samples_and_keeps_fixed_denominator(self):
        self.write_rows([(900, 200, "true", 5, 5, 0), (1200, 100, "true", 5, 5, 5),
                         (1900, 200, "true", 5, 5, 10)])
        result = MODULE.summarize([self.path], 1000, 2000)
        self.assertEqual(result["samples"], 1)
        self.assertEqual(result["excluded_samples"], 2)
        self.assertEqual(result["acknowledged_points_per_second"], 10)

    def test_invalid_or_missing_counters_fail_instead_of_guessing(self):
        self.path.write_text("timeStamp,elapsed,success\n1000,100,true\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "missing JTL columns"):
            MODULE.summarize([self.path])
        self.write_rows([(1000, 100, "true", 5, 4, 0)])
        with self.assertRaisesRegex(ValueError, "acknowledged rows exceed"):
            MODULE.summarize([self.path])

    def test_duplicates_and_possible_timestamp_reuse_are_visible(self):
        self.write_rows([(1000, 100, "true", 5, 5, 0), (1200, 100, "true", 5, 5, 0)])
        with self.assertRaisesRegex(ValueError, "same JTL file"):
            MODULE.summarize([self.path, self.path])
        self.assertEqual(MODULE.summarize([self.path])["possible_overlapping_timestamp_ranges"], 1)


if __name__ == "__main__":
    unittest.main()
