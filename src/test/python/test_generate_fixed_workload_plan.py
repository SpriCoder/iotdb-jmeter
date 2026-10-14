import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts/generate_fixed_workload_plan.py"
SPEC = importlib.util.spec_from_file_location("generate_fixed_workload_plan", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def arguments(root):
    return {
        element.find("stringProp[@name='Argument.name']").text:
        element.find("stringProp[@name='Argument.value']").text or ""
        for element in root.findall(".//JavaSampler/elementProp/collectionProp/elementProp")
    }


class FixedWorkloadPlanTest(unittest.TestCase):
    def test_formal_plan_preserves_point_count_and_one_batch_per_invocation(self):
        root = ET.fromstring(MODULE.build_plan())
        self.assertEqual(len(root.findall(".//ThreadGroup")), 1)
        self.assertEqual(len(root.findall(".//JavaSampler")), 1)
        self.assertEqual(root.find(".//JavaSampler/stringProp[@name='classname']").text,
                         "org.apache.iotdb.jmeter.test.FixedWorkloadSampler")
        values = arguments(root)
        workers = int(values["workers"])
        sources = int(values["sources_per_worker"])
        batches = int(values["batches_per_source"])
        self.assertEqual((workers, sources, batches), (10, 50, 2400))
        self.assertEqual(root.find(".//stringProp[@name='ThreadGroup.num_threads']").text, "10")
        self.assertEqual(int(root.find(".//stringProp[@name='LoopController.loops']").text),
                         1 + sources * batches)
        self.assertEqual(values["op_count"], values["batch_size"])
        self.assertEqual(values["op_count"], "100")
        self.assertEqual(values["measurements"].split(","), ["s" + str(i) for i in range(10)])
        self.assertEqual(values["data_types"].split(","), ["INT64"] * 10)
        self.assertEqual(workers * sources * batches * 100 * 10, 1_200_000_000)
        self.assertEqual(values["start_timestamp"], "1700000000000")
        self.assertEqual((values["timestamp_step_ms"], values["value_start"], values["value_step"]),
                         ("1", "0", "1"))

    def test_preparation_is_internal_and_plan_has_no_timed_stop_or_password_property(self):
        root = ET.fromstring(MODULE.build_plan())
        values = arguments(root)
        self.assertNotIn("phase", values)
        self.assertNotIn("device_per_thread", values)
        self.assertNotIn("stop_on_error", values)
        self.assertEqual(values["run_id"], "${__P(run_id,)}")
        self.assertEqual(values["device_path"], "${__P(device_path,root.jmeter.fixed)}")
        self.assertEqual(values["aligned"], "${__P(aligned,false)}")
        self.assertEqual(values["password"], "")
        self.assertEqual(root.find(".//boolProp[@name='ThreadGroup.scheduler']").text, "false")
        self.assertIsNone(root.find(".//stringProp[@name='ThreadGroup.duration']"))
        self.assertEqual(root.find(".//stringProp[@name='ThreadGroup.ramp_time']").text, "0")
        self.assertFalse(root.findall(".//JSR223Sampler"))
        for variable in MODULE.SAMPLE_VARIABLES:
            self.assertIn(variable, root.find(".//stringProp[@name='TestPlan.comments']").text)

    def test_smaller_workload_requires_explicit_functional_label(self):
        with self.assertRaisesRegex(ValueError, "functional-only"):
            MODULE.build_plan(workers=2, sources_per_worker=2, batches_per_source=3)
        root = ET.fromstring(MODULE.build_plan(2, 2, 3, functional_only=True))
        self.assertEqual(root.find(".//TestPlan").attrib["testname"], "FUNCTIONAL ONLY")
        self.assertEqual(root.find(".//stringProp[@name='LoopController.loops']").text, "7")
        self.assertIn("cannot support", root.find(".//stringProp[@name='TestPlan.comments']").text)
        self.assertEqual(arguments(root)["workers"], "2")

    def test_invalid_dimensions_and_timestamp_overflow_are_rejected(self):
        for name, value in [("workers", 0), ("workers", 11), ("sources_per_worker", -1),
                            ("sources_per_worker", 51), ("batches_per_source", 2401),
                            ("start_timestamp", -1), ("start_timestamp", 2 ** 63 - 1),
                            ("start_timestamp", 2 ** 63 - 240000)]:
            with self.subTest(name=name, value=value):
                with self.assertRaises(ValueError):
                    MODULE.build_plan(functional_only=True, **{name: value})

    def test_committed_example_is_reproducible(self):
        self.assertEqual((ROOT / "examples/iotdb-fixed-workload.jmx").read_bytes(), MODULE.build_plan())

    def test_cli_failure_does_not_replace_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "plan.jmx"
            output.write_text("keep this plan", encoding="utf-8")
            result = subprocess.run([sys.executable, str(SCRIPT), "--output", str(output),
                                     "--workers", "2"], capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            self.assertIn("functional-only", result.stderr)
            self.assertEqual(output.read_text(encoding="utf-8"), "keep this plan")


if __name__ == "__main__":
    unittest.main()
