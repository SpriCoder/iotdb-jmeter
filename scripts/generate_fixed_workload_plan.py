#!/usr/bin/env python3
"""Generate the fixed-point plan, or an explicitly marked functional-only plan."""

import argparse
from pathlib import Path
import xml.etree.ElementTree as ET


SAMPLER_CLASS = "org.apache.iotdb.jmeter.test.FixedWorkloadSampler"
DEFAULT_WORKERS = 10
DEFAULT_SOURCES_PER_WORKER = 50
DEFAULT_BATCHES_PER_SOURCE = 2400
DEFAULT_START_TIMESTAMP = 1700000000000
ROWS_PER_BATCH = 100
MEASUREMENTS = tuple("s" + str(index) for index in range(10))
SAMPLE_VARIABLES = (
    "iotdb_phase", "iotdb_run_id", "iotdb_formal_start_ms", "iotdb_worker", "iotdb_timestamp_is_start",
    "iotdb_ack_rows", "iotdb_ack_points", "iotdb_attempted_rows", "iotdb_device",
    "iotdb_first_timestamp", "iotdb_last_timestamp",
)


def prop(parent, tag, name, value):
    ET.SubElement(parent, tag, {"name": name}).text = str(value)


def build_plan(workers=DEFAULT_WORKERS, sources_per_worker=DEFAULT_SOURCES_PER_WORKER,
               batches_per_source=DEFAULT_BATCHES_PER_SOURCE,
               start_timestamp=DEFAULT_START_TIMESTAMP, functional_only=False):
    dimensions = (
        ("workers", workers, DEFAULT_WORKERS),
        ("sources_per_worker", sources_per_worker, DEFAULT_SOURCES_PER_WORKER),
        ("batches_per_source", batches_per_source, DEFAULT_BATCHES_PER_SOURCE),
    )
    for name, value, maximum in dimensions:
        if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= maximum:
            raise ValueError("{} must be an integer between 1 and {}".format(name, maximum))
    if not functional_only and any(value != maximum for _, value, maximum in dimensions):
        raise ValueError("Reduced workloads require --functional-only; they are not acceptance runs")
    rows_per_source = batches_per_source * ROWS_PER_BATCH
    if (not isinstance(start_timestamp, int) or isinstance(start_timestamp, bool)
            or not 0 <= start_timestamp <= (2 ** 63 - 1) - rows_per_source):
        raise ValueError("start_timestamp must be nonnegative and leave room for all INT64 timestamps")

    points = workers * sources_per_worker * rows_per_source * len(MEASUREMENTS)
    loops = 1 + sources_per_worker * batches_per_source
    mode = "FUNCTIONAL ONLY" if functional_only else "FORMAL FIXED WORKLOAD"
    root = ET.Element("jmeterTestPlan", {"version": "1.2", "properties": "5.0", "jmeter": "5.6.3"})
    outer = ET.SubElement(root, "hashTree")
    plan = ET.SubElement(outer, "TestPlan", {
        "guiclass": "TestPlanGui", "testclass": "TestPlan", "testname": mode, "enabled": "true",
    })
    comment = (
        "{}: {} workers x {} sources x {} batches x 100 rows x 10 INT64 measurements = {} points. "
        "The first invocation prepares all owned sources and waits for common T0; subsequent "
        "invocations each submit one synchronous Tablet. No scheduler or 60-second stop. "
        "Mandatory -Jrun_id; password only from IOTDB_PASSWORD. "
        "Use -Jsampleresult.timestamp.start=true and -Jsample_variables={}. "
        "{}"
    ).format(mode, workers, sources_per_worker, batches_per_source, points,
             ",".join(SAMPLE_VARIABLES),
             "Functional validation only; cannot support a 20,000,000 points/s acceptance claim."
             if functional_only else "Formal threshold: complete all 1,200,000,000 points within 60 seconds.")
    prop(plan, "stringProp", "TestPlan.comments", comment)
    prop(plan, "boolProp", "TestPlan.functional_mode", "false")
    prop(plan, "boolProp", "TestPlan.tearDown_on_shutdown", "true")
    prop(plan, "boolProp", "TestPlan.serialize_threadgroups", "false")
    variables = ET.SubElement(plan, "elementProp", {
        "name": "TestPlan.user_defined_variables", "elementType": "Arguments",
        "guiclass": "ArgumentsPanel", "testclass": "Arguments", "testname": "Variables", "enabled": "true",
    })
    ET.SubElement(variables, "collectionProp", {"name": "Arguments.arguments"})
    prop(plan, "stringProp", "TestPlan.user_define_classpath", "")
    tree = ET.SubElement(outer, "hashTree")
    group = ET.SubElement(tree, "ThreadGroup", {
        "guiclass": "ThreadGroupGui", "testclass": "ThreadGroup",
        "testname": "Fixed workload workers", "enabled": "true",
    })
    prop(group, "stringProp", "ThreadGroup.on_sample_error", "continue")
    controller = ET.SubElement(group, "elementProp", {
        "name": "ThreadGroup.main_controller", "elementType": "LoopController",
        "guiclass": "LoopControlPanel", "testclass": "LoopController",
        "testname": "Preparation plus fixed batches", "enabled": "true",
    })
    prop(controller, "boolProp", "LoopController.continue_forever", "false")
    prop(controller, "stringProp", "LoopController.loops", loops)
    prop(group, "stringProp", "ThreadGroup.num_threads", workers)
    prop(group, "stringProp", "ThreadGroup.ramp_time", 0)
    prop(group, "boolProp", "ThreadGroup.scheduler", "false")
    prop(group, "boolProp", "ThreadGroup.same_user_on_next_iteration", "true")
    group_tree = ET.SubElement(tree, "hashTree")
    sampler = ET.SubElement(group_tree, "JavaSampler", {
        "guiclass": "JavaTestSamplerGui", "testclass": "JavaSampler",
        "testname": "Fixed workload prepare then write", "enabled": "true",
    })
    arguments = ET.SubElement(sampler, "elementProp", {
        "name": "arguments", "elementType": "Arguments", "guiclass": "ArgumentsPanel",
        "testclass": "Arguments", "testname": "Fixed workload parameters", "enabled": "true",
    })
    collection = ET.SubElement(arguments, "collectionProp", {"name": "Arguments.arguments"})
    values = {
        "host": "${__P(host,127.0.0.1)}",
        "port": "${__P(port,6667)}",
        "username": "${__P(username,root)}",
        "password": "",
        "device_path": "${__P(device_path,root.jmeter.fixed)}",
        "run_id": "${__P(run_id,)}",
        "measurements": ",".join(MEASUREMENTS),
        "data_types": ",".join(["INT64"] * len(MEASUREMENTS)),
        "op_count": ROWS_PER_BATCH,
        "batch_size": ROWS_PER_BATCH,
        "start_timestamp": start_timestamp,
        "timestamp_step_ms": 1,
        "value_start": 0,
        "value_step": 1,
        "aligned": "${__P(aligned,false)}",
        "prepare_schema": "true",
        "connection_timeout_ms": "${__P(connection_timeout_ms,5000)}",
        "query_timeout_ms": "${__P(query_timeout_ms,60000)}",
        "rpc_compression": "false",
        "workers": workers,
        "sources_per_worker": sources_per_worker,
        "batches_per_source": batches_per_source,
        "prepare_timeout_ms": "${__P(prepare_timeout_ms,300000)}",
    }
    for name, value in values.items():
        argument = ET.SubElement(collection, "elementProp", {"name": name, "elementType": "Argument"})
        prop(argument, "stringProp", "Argument.name", name)
        prop(argument, "stringProp", "Argument.value", value)
        prop(argument, "stringProp", "Argument.metadata", "=")
    prop(sampler, "stringProp", "classname", SAMPLER_CLASS)
    ET.SubElement(group_tree, "hashTree")
    ET.indent(root, space="  ")
    return ET.tostring(root, encoding="utf-8", xml_declaration=True) + b"\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="Destination JMX path")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--sources-per-worker", type=int, default=DEFAULT_SOURCES_PER_WORKER)
    parser.add_argument("--batches-per-source", type=int, default=DEFAULT_BATCHES_PER_SOURCE)
    parser.add_argument("--start-timestamp", type=int, default=DEFAULT_START_TIMESTAMP)
    parser.add_argument("--functional-only", action="store_true",
                        help="Allow smaller dimensions; mark the plan as functional validation only")
    args = parser.parse_args()
    try:
        plan = build_plan(args.workers, args.sources_per_worker, args.batches_per_source,
                          args.start_timestamp, args.functional_only)
    except ValueError as error:
        parser.error(str(error))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(plan)


if __name__ == "__main__":
    main()
