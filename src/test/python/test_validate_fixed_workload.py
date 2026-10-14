import csv
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("validate_fixed_workload", ROOT / "scripts/validate_fixed_workload.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class FixedWorkloadValidationTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.path = self.directory / "measure.jtl"
        self.rows = []
        for worker in range(2):
            self.rows.append(self.row(worker, "prepare", 9000, 1000))
        for batch in range(2):
            for source in range(2):
                for worker in range(2):
                    start = 10000 + (batch * 2 + source) * 2 + worker
                    self.rows.append(self.row(worker, "formal", start, 2, source, batch * 100))

    def row(self, worker, phase, start, elapsed, source=0, first=0):
        prefix = f"root.jmeter.fixed.run_test.worker_{worker}"
        formal = phase == "formal"
        return {"timeStamp": start, "elapsed": elapsed, "success": "true",
                "threadName": f"writers 1-{worker + 1}", "iotdb_phase": phase,
                "iotdb_timestamp_is_start": "true",
                "iotdb_run_id": "test", "iotdb_formal_start_ms": 10000,
                "iotdb_worker": worker, "iotdb_ack_rows": 100 if formal else 0,
                "iotdb_ack_points": 1000 if formal else 0,
                "iotdb_attempted_rows": 100 if formal else 0,
                "iotdb_device": prefix + (f".source_{source}" if formal else ""),
                "iotdb_first_timestamp": first if formal else -1,
                "iotdb_last_timestamp": first + 99 if formal else -1}

    def write_rows(self):
        with self.path.open("w", encoding="utf-8", newline="") as output:
            fields = MODULE.REQUIRED | {key for row in self.rows for key in row}
            writer = csv.DictWriter(output, fieldnames=sorted(fields))
            writer.writeheader()
            writer.writerows(self.rows)

    def validate(self, readback=None):
        self.write_rows()
        return MODULE.validate([self.path], workers=2, sources_per_worker=2,
            batches_per_source=2, functional_only=True, readback=readback)

    def assert_failed(self, expected_error):
        result, _ = self.validate()
        self.assertEqual(result["status"], "FAILED")
        self.assertTrue(any(expected_error in message for message in result["errors"]), result["errors"])
        return result

    def test_complete_functional_run_waits_for_bound_readback(self):
        result, manifest = self.validate()
        self.assertEqual(result["status"], "FUNCTIONAL_COMPLETE_REQUIRES_READBACK")
        self.assertFalse(result["performance_acceptance"])
        self.assertEqual(result["unique_ack_points"], 8000)
        self.assertEqual(result["formal_start_ms"], 10000)
        self.assertEqual(result["formal_end_ms"], 10009)
        self.assertAlmostEqual(result["wall_seconds"], 0.009)
        self.assertEqual(result["common_active_start_ms"], 10005)
        self.assertEqual(result["common_active_end_ms"], 10006)
        parsed = list(csv.DictReader(manifest.splitlines(), delimiter="\t"))
        self.assertEqual(len(parsed), 4)
        self.assertTrue(all(row["rows"] == "200" and row["last_timestamp"] == "199" for row in parsed))
        self.assertEqual(hashlib.sha256(manifest.encode()).hexdigest(), result["manifest_sha256"])
        readback = self.directory / "readback.json"
        readback.write_text(json.dumps({"passed": True,
            "manifest_sha256": result["manifest_sha256"],
            "verified_devices": 4, "verified_points": 8000}))
        finalized, _ = self.validate(readback)
        self.assertEqual(finalized["status"], "FUNCTIONAL_VALIDATED")
        self.assertFalse(finalized["performance_acceptance"])

    def test_other_worker_prepare_can_flush_after_first_formal_record(self):
        preparation = self.rows.pop(1)
        self.rows.insert(2, preparation)
        self.assertEqual(self.validate()[0]["error_count"], 0)

    def test_failed_tail_is_included_and_cannot_count_as_success(self):
        self.rows[-1].update(success="false", elapsed=20002,
                             iotdb_ack_rows=0, iotdb_ack_points=0)
        result = self.assert_failed("failed formal")
        self.assertEqual(result["formal_end_ms"], 30009)
        self.assertEqual(result["unique_ack_points"], 7000)
        self.assertAlmostEqual(result["wall_seconds"], 20.009)
        self.assertTrue(any("unfinished source" in error for error in result["errors"]))

    def test_failed_sample_with_full_ack_is_rejected_not_counted(self):
        self.rows[-1]["success"] = "false"
        result = self.assert_failed("invalid formal ACK")
        self.assertEqual(result["unique_ack_points"], 7000)
        self.assertEqual(result["formal_end_ms"], 10009)

    def test_custom_root_path_is_bound_to_manifest_and_protocol(self):
        for row in self.rows:
            row["iotdb_device"] = row["iotdb_device"].replace("root.jmeter.fixed", "root.nx.acceptance")
        self.write_rows()
        result, manifest = MODULE.validate([self.path], workers=2, sources_per_worker=2,
            batches_per_source=2, functional_only=True, device_path="root.nx.acceptance")
        self.assertEqual(result["error_count"], 0)
        self.assertIn("root.nx.acceptance.run_test.worker_0.source_0", manifest)
        self.assert_failed("invalid prepare device prefix")
        with self.assertRaisesRegex(ValueError, "simple identifier"):
            MODULE.validate([self.path], functional_only=True, device_path="root.bad.*")

    def test_end_timestamp_and_explicit_error_flags_are_rejected(self):
        self.rows[-1]["iotdb_timestamp_is_start"] = "false"
        self.assert_failed("sample start")
        self.rows[-1]["iotdb_timestamp_is_start"] = "true"
        self.rows[-1]["responseCode"] = "FIXED_WORKLOAD_ERROR"
        self.assert_failed("error responseCode")
        self.rows[-1]["responseCode"] = "200"
        self.rows[-1]["errorCount"] = 1
        self.assert_failed("nonzero errorCount")

    def test_missing_source_and_worker_quota_are_not_silently_ignored(self):
        self.rows = [row for row in self.rows if not row["iotdb_device"].endswith("worker_1.source_1")]
        self.assert_failed("missing source")

    def test_prepare_counters_thread_owner_and_shared_t0_are_checked(self):
        self.rows[0]["iotdb_ack_points"] = 1000
        self.assert_failed("preparation must have zero")
        self.rows[0]["iotdb_ack_points"] = 0
        self.rows[-1]["threadName"] = self.rows[0]["threadName"]
        self.assert_failed("thread owns multiple workers")
        self.rows[-1]["threadName"] = "writers 1-2"
        self.rows[-1]["iotdb_formal_start_ms"] = 10001
        self.assert_failed("shared T0 mismatch")

    def test_wrong_round_robin_device_and_late_preparation_fail(self):
        self.rows[2]["iotdb_device"] = "root.jmeter.fixed.run_test.worker_0.source_1"
        self.assert_failed("round-robin")
        self.rows[2]["iotdb_device"] = "root.jmeter.fixed.run_test.worker_0.source_0"
        preparation = self.rows.pop(0)
        self.rows.insert(3, preparation)
        self.assert_failed("formal sample without worker preparation")

    def test_overlap_is_rejected_and_duplicate_points_are_not_added(self):
        self.rows[6]["iotdb_first_timestamp"] = 0
        self.rows[6]["iotdb_last_timestamp"] = 99
        result = self.assert_failed("source timestamp overlap or gap")
        self.assertEqual(result["unique_ack_points"], 7000)

    def test_timestamp_gap_and_wrong_batch_size_fail(self):
        self.rows[6]["iotdb_first_timestamp"] = 101
        self.rows[6]["iotdb_last_timestamp"] = 200
        self.assert_failed("source timestamp overlap or gap")
        self.rows[6]["iotdb_first_timestamp"] = 100
        self.rows[6]["iotdb_last_timestamp"] = 199
        self.rows[6]["iotdb_ack_rows"] = 99
        self.rows[6]["iotdb_ack_points"] = 990
        self.assert_failed("invalid formal ACK")

    def test_no_ack_gap_is_checked_only_until_each_source_completes(self):
        # Shift the second round beyond the agreed ten-second limit.
        for row in self.rows[6:]:
            row["timeStamp"] += 10001
        self.assert_failed("source no-ACK interval exceeds")

    def test_nonpositive_common_active_interval_and_zero_time_fail(self):
        for row in self.rows[2:]:
            row["timeStamp"] = 10000
            row["elapsed"] = 0
        self.assert_failed("common active interval")
        self.assert_failed("nonpositive formal wall-clock")

    def test_mixed_run_and_failed_preparation_cannot_pass(self):
        self.rows[-1]["iotdb_run_id"] = "other"
        self.assert_failed("mixed run_id")
        self.rows[-1]["iotdb_run_id"] = "test"
        self.rows[0].update(success="false", iotdb_formal_start_ms=0)
        self.assert_failed("failed prepare")

    def test_readback_hash_counts_and_boolean_are_all_required(self):
        result, _ = self.validate()
        readback = self.directory / "readback.json"
        correct = {"passed": True, "manifest_sha256": result["manifest_sha256"],
                   "verified_devices": 4, "verified_points": 8000}
        for field, wrong in (("manifest_sha256", "wrong"), ("verified_devices", 3),
                             ("verified_points", 7999), ("passed", "true")):
            with self.subTest(field=field):
                readback.write_text(json.dumps(dict(correct, **{field: wrong})))
                finalized, _ = self.validate(readback)
                self.assertEqual(finalized["status"], "FAILED")

    def test_reduced_workload_requires_functional_flag(self):
        self.write_rows()
        with self.assertRaisesRegex(ValueError, "functional-only"):
            MODULE.validate([self.path], workers=2, sources_per_worker=2, batches_per_source=2)
        with self.assertRaisesRegex(ValueError, "must not exceed"):
            MODULE.validate([self.path], workers=11, functional_only=True)

    def test_missing_columns_duplicate_files_and_bad_numeric_rows_fail(self):
        self.path.write_text("timeStamp,elapsed\n0,1\n")
        with self.assertRaisesRegex(ValueError, "missing JTL columns"):
            MODULE.validate([self.path], functional_only=True)
        with self.assertRaisesRegex(ValueError, "distinct JTL"):
            MODULE.validate([self.path, self.path], functional_only=True)
        self.rows[-1]["iotdb_ack_points"] = "NaN"
        self.assert_failed("invalid protocol fields")

    def test_formal_status_requires_both_throughput_and_readback(self):
        self.write_rows()
        # Small deterministic fixture exercises finalization; production limits remain fixed.
        with patch.object(MODULE, "DEFAULTS", (2, 2, 2)), \
                patch.object(MODULE, "TARGET_POINTS_PER_SECOND", 800_000):
            result, _ = MODULE.validate([self.path], workers=2, sources_per_worker=2, batches_per_source=2)
            self.assertEqual(result["status"], "WRITE_COMPLETE_REQUIRES_READBACK")
            readback = self.directory / "readback.json"
            readback.write_text(json.dumps({"passed": True,
                "manifest_sha256": result["manifest_sha256"],
                "verified_devices": 4, "verified_points": 8000}))
            result, _ = MODULE.validate([self.path], workers=2, sources_per_worker=2,
                batches_per_source=2, readback=readback)
            self.assertEqual(result["status"], "PASS")
            self.assertTrue(result["performance_acceptance"])
        with patch.object(MODULE, "DEFAULTS", (2, 2, 2)):
            result, _ = MODULE.validate([self.path], workers=2, sources_per_worker=2,
                batches_per_source=2, readback=readback)
            self.assertEqual(result["status"], "FAILED")

    def test_cli_exports_manifest_bytes_and_summary(self):
        self.write_rows()
        manifest = self.directory / "manifest.tsv"
        summary = self.directory / "summary.json"
        completed = subprocess.run([sys.executable, str(ROOT / "scripts/validate_fixed_workload.py"),
            str(self.path), "--functional-only", "--workers", "2", "--sources-per-worker", "2",
            "--batches-per-source", "2", "--manifest-out", str(manifest),
            "--summary-out", str(summary)], capture_output=True, text=True)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(summary.read_text())
        self.assertEqual(hashlib.sha256(manifest.read_bytes()).hexdigest(), result["manifest_sha256"])
        self.assertFalse(result["performance_acceptance"])

    def test_cli_rejects_output_alias_of_input_without_changing_evidence(self):
        self.write_rows()
        original = self.path.read_bytes()
        alias = self.directory / "manifest.tsv"
        alias.symlink_to(self.path)
        summary = self.directory / "summary.json"
        completed = subprocess.run([sys.executable, str(ROOT / "scripts/validate_fixed_workload.py"),
            str(self.path), "--manifest-out", str(alias), "--summary-out", str(summary)],
            capture_output=True, text=True)
        self.assertEqual(completed.returncode, 2)
        self.assertIn("must not overwrite", completed.stderr)
        self.assertEqual(self.path.read_bytes(), original)
        self.assertFalse(summary.exists())

    def test_cli_rejects_hardlinked_outputs_and_readback_alias(self):
        self.write_rows()
        manifest = self.directory / "manifest.tsv"
        summary = self.directory / "summary.json"
        manifest.write_text("existing evidence")
        os.link(manifest, summary)
        command = [sys.executable, str(ROOT / "scripts/validate_fixed_workload.py"),
            str(self.path), "--manifest-out", str(manifest), "--summary-out", str(summary)]
        completed = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(completed.returncode, 2)
        self.assertEqual(manifest.read_text(), "existing evidence")
        summary.unlink()
        readback = self.directory / "readback.json"
        os.link(manifest, readback)
        completed = subprocess.run(command + ["--readback", str(readback)], capture_output=True, text=True)
        self.assertEqual(completed.returncode, 2)
        self.assertEqual(readback.read_text(), "existing evidence")
        self.assertFalse(summary.exists())


if __name__ == "__main__":
    unittest.main()
