#!/usr/bin/env python3
"""Validate the phase-aware fixed-workload JTL and bind database readback to its manifest."""

import argparse
import csv
import hashlib
import io
import json
from pathlib import Path
import re
import sys


REQUIRED = {
    "timeStamp", "elapsed", "success", "threadName", "iotdb_phase", "iotdb_run_id",
    "iotdb_formal_start_ms", "iotdb_worker", "iotdb_ack_rows", "iotdb_ack_points",
    "iotdb_attempted_rows", "iotdb_device", "iotdb_first_timestamp", "iotdb_last_timestamp",
    "iotdb_timestamp_is_start",
}
DEFAULTS = (10, 50, 2400)
TARGET_POINTS_PER_SECOND = 20_000_000


def validate(paths, *, run_id=None, workers=10, sources_per_worker=50,
             batches_per_source=2400, functional_only=False, readback=None,
             device_path="root.jmeter.fixed"):
    dimensions = (workers, sources_per_worker, batches_per_source)
    if any(type(value) is not int or value <= 0 for value in dimensions):
        raise ValueError("Workload dimensions must be positive integers")
    if not functional_only and dimensions != DEFAULTS:
        raise ValueError("Reduced workloads require --functional-only; formal dimensions are 10/50/2400")
    if functional_only and any(value > limit for value, limit in zip(dimensions, DEFAULTS)):
        raise ValueError("Functional dimensions must not exceed 10/50/2400")
    if run_id is not None and not re.fullmatch(r"[A-Za-z0-9_]+", run_id):
        raise ValueError("run_id must be a simple identifier")
    if not re.fullmatch(r"root(?:\.[A-Za-z_][A-Za-z0-9_]*)+", device_path):
        raise ValueError("device_path must be a root path with simple identifier nodes")
    resolved = [Path(path).resolve() for path in paths]
    if not resolved or len(set(resolved)) != len(resolved):
        raise ValueError("Supply distinct JTL files once, in per-worker record order")

    errors, error_count = [], 0

    def fail(message):
        nonlocal error_count
        error_count += 1
        if len(errors) < 100:
            errors.append(message)

    worker_state = [{"thread": None, "prepare": 0, "prepare_end": None,
                     "formal": 0, "last_end": None} for _ in range(workers)]
    owners, sources = {}, {}
    t0 = t1 = None
    formal_samples = failed_samples = ack_points = 0
    for path in resolved:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames and len(set(reader.fieldnames)) != len(reader.fieldnames):
                raise ValueError(f"{path}: duplicate JTL column names")
            missing = REQUIRED - set(reader.fieldnames or [])
            if missing:
                raise ValueError(f"{path}: missing JTL columns: {', '.join(sorted(missing))}")
            for line, row in enumerate(reader, 2):
                where = f"{path.name}:{line}"
                try:
                    start, elapsed = int(row["timeStamp"]), int(row["elapsed"])
                    if start < 0 or elapsed < 0:
                        raise ValueError("negative sample time")
                    end = start + elapsed
                except (ValueError, TypeError, KeyError) as exc:
                    fail(f"{where}: invalid sample time ({exc})")
                    continue
                phase = row.get("iotdb_phase")
                if str(row.get("iotdb_timestamp_is_start", "")).lower() != "true":
                    fail(f"{where}: JTL timeStamp must be the sample start")
                if phase == "formal":
                    formal_samples += 1
                    t1 = end if t1 is None else max(t1, end)
                try:
                    worker = int(row["iotdb_worker"])
                    shared_start = int(row["iotdb_formal_start_ms"])
                    acknowledged = int(row["iotdb_ack_rows"])
                    points = int(row["iotdb_ack_points"])
                    attempted = int(row["iotdb_attempted_rows"])
                    first = int(row["iotdb_first_timestamp"])
                    last = int(row["iotdb_last_timestamp"])
                    if not 0 <= worker < workers:
                        raise ValueError("worker outside expected range")
                    if str(row["success"]).lower() not in {"true", "false"}:
                        raise ValueError("success must be true/false")
                    successful = str(row["success"]).lower() == "true"
                    identity = row["iotdb_run_id"]
                    if not re.fullmatch(r"[A-Za-z0-9_]+", identity):
                        raise ValueError("invalid run_id")
                    if run_id is None:
                        run_id = identity
                    if identity != run_id:
                        raise ValueError("mixed run_id")
                    if phase not in {"prepare", "formal"}:
                        raise ValueError("unknown phase")
                except (ValueError, TypeError, KeyError) as exc:
                    fail(f"{where}: invalid protocol fields ({exc})")
                    continue
                if not successful:
                    failed_samples += 1
                    fail(f"{where}: failed {phase} sample")
                if successful and row.get("responseCode") not in {None, "", "200"}:
                    fail(f"{where}: successful sample has an error responseCode")
                if row.get("errorCount") not in {None, ""}:
                    try:
                        if int(row["errorCount"]) != 0:
                            fail(f"{where}: nonzero errorCount")
                    except ValueError:
                        fail(f"{where}: invalid errorCount")
                if shared_start <= 0:
                    fail(f"{where}: missing positive shared T0")
                elif t0 is None:
                    t0 = shared_start
                elif shared_start != t0:
                    fail(f"{where}: shared T0 mismatch")
                state = worker_state[worker]
                thread = row["threadName"]
                if not thread or (state["thread"] is not None and thread != state["thread"]):
                    fail(f"{where}: worker thread ownership changed or missing")
                if thread in owners and owners[thread] != worker:
                    fail(f"{where}: thread owns multiple workers")
                owners[thread] = worker
                state["thread"] = thread
                prefix = f"{device_path}.run_{run_id}.worker_{worker}"
                if phase == "prepare":
                    state["prepare"] += 1
                    if state["prepare"] != 1 or state["formal"]:
                        fail(f"{where}: duplicate or late worker preparation")
                    state["prepare_end"] = end
                    if row["iotdb_device"] != prefix:
                        fail(f"{where}: invalid prepare device prefix")
                    if (acknowledged, points, attempted, first, last) != (0, 0, 0, -1, -1):
                        fail(f"{where}: preparation must have zero counters and no data timestamps")
                    if successful and not start <= shared_start <= end:
                        fail(f"{where}: prepare sample does not contain barrier release T0")
                    continue

                if state["prepare"] != 1 or state["prepare_end"] is None:
                    fail(f"{where}: formal sample without worker preparation")
                elif start < state["prepare_end"]:
                    fail(f"{where}: formal sample starts before worker preparation ends")
                if start < shared_start:
                    fail(f"{where}: formal sample starts before T0")
                if state["last_end"] is not None and start < state["last_end"]:
                    fail(f"{where}: worker samples overlap or are out of order")
                state["last_end"] = end
                expected_source = state["formal"] % sources_per_worker
                state["formal"] += 1
                device = row["iotdb_device"]
                if device != f"{prefix}.source_{expected_source}":
                    fail(f"{where}: device or round-robin source order mismatch")
                match = re.fullmatch(re.escape(prefix) + r"\.source_(\d+)", device)
                if match is None or int(match.group(1)) >= sources_per_worker:
                    fail(f"{where}: unexpected source path")
                    continue
                source = sources.setdefault(device, {"batches": 0, "rows": 0,
                    "first_timestamp": None, "last_timestamp": None, "ack_max_timestamp": None,
                    "first_ack_ms": None, "last_ack_ms": None, "complete_ack_ms": None,
                    "max_no_ack_ms": 0})
                source["batches"] += 1
                range_valid = first >= 0 and last == first + 99
                if not range_valid:
                    fail(f"{where}: formal timestamp range must contain 100 consecutive milliseconds")
                elif source["last_timestamp"] is not None and first != source["last_timestamp"] + 1:
                    fail(f"{where}: source timestamp overlap or gap")
                if range_valid:
                    if source["first_timestamp"] is None:
                        source["first_timestamp"] = first
                    source["last_timestamp"] = last
                counters_valid = ((successful and (attempted, acknowledged, points) == (100, 100, 1000))
                    or (not successful and attempted in {0, 100} and (acknowledged, points) == (0, 0)))
                if not counters_valid:
                    fail(f"{where}: invalid formal ACK/attempt counters")
                if successful and counters_valid and range_valid:
                    maximum = source["ack_max_timestamp"]
                    unique_rows = last - max(first, maximum + 1 if maximum is not None else first) + 1
                    unique_rows = max(0, unique_rows)
                    source["ack_max_timestamp"] = last if maximum is None else max(maximum, last)
                    source["rows"] += unique_rows
                    ack_points += unique_rows * 10
                    previous = source["last_ack_ms"]
                    gap = end - (shared_start if previous is None else previous)
                    source["max_no_ack_ms"] = max(source["max_no_ack_ms"], gap)
                    if gap > 10_000:
                        fail(f"{where}: source no-ACK interval exceeds 10 seconds")
                    if source["first_ack_ms"] is None:
                        source["first_ack_ms"] = end
                    source["last_ack_ms"] = end
                    if source["rows"] == batches_per_source * 100:
                        source["complete_ack_ms"] = end

    expected_points = workers * sources_per_worker * batches_per_source * 1000
    for worker, state in enumerate(worker_state):
        if state["prepare"] != 1 or state["formal"] != sources_per_worker * batches_per_source:
            fail(f"worker {worker}: missing preparation or formal batch quota")
    manifest = io.StringIO(newline="")
    writer = csv.writer(manifest, delimiter="\t", lineterminator="\n")
    writer.writerow(["device", "rows", "first_timestamp", "last_timestamp"])
    source_results = {}
    for worker in range(workers):
        for ordinal in range(sources_per_worker):
            device = f"{device_path}.run_{run_id}.worker_{worker}.source_{ordinal}"
            source = sources.get(device)
            if source is None:
                fail(f"{device}: missing source")
                writer.writerow([device, 0, "", ""])
                continue
            if source["batches"] != batches_per_source or source["rows"] != batches_per_source * 100:
                fail(f"{device}: incomplete or excessive source quota")
            if source["complete_ack_ms"] is None and t1 is not None and t0 is not None:
                gap = t1 - (source["last_ack_ms"] if source["last_ack_ms"] is not None else t0)
                source["max_no_ack_ms"] = max(source["max_no_ack_ms"], gap)
                if gap > 10_000:
                    fail(f"{device}: unfinished source no-ACK tail exceeds 10 seconds")
            writer.writerow([device, source["rows"], source["first_timestamp"], source["last_timestamp"]])
            source_results[device] = {key: value for key, value in source.items()
                                      if key != "ack_max_timestamp"}
    complete = len(source_results) == workers * sources_per_worker and all(
        source["complete_ack_ms"] is not None for source in source_results.values())
    active_start = max(source["first_ack_ms"] for source in source_results.values()) if complete else None
    active_end = min(source["complete_ack_ms"] for source in source_results.values()) if complete else None
    if not complete or active_end <= active_start:
        fail("All-source common active interval must have positive length")
    seconds = (t1 - t0) / 1000.0 if t0 is not None and t1 is not None else None
    if seconds is None or seconds <= 0:
        fail("Missing or nonpositive formal wall-clock duration")
    rate = ack_points / seconds if seconds is not None and seconds > 0 else None
    if ack_points != expected_points:
        fail("Successful unique ACK points do not equal the fixed workload")
    if not functional_only and (rate is None or rate < TARGET_POINTS_PER_SECOND):
        fail("Effective throughput is below 20,000,000 unique ACK points/second")
    manifest_text = manifest.getvalue()
    manifest_sha256 = hashlib.sha256(manifest_text.encode("utf-8")).hexdigest()
    readback_passed = False
    if readback is not None:
        try:
            result = json.loads(Path(readback).read_text(encoding="utf-8"))
            readback_passed = (result.get("passed") is True
                and result.get("manifest_sha256") == manifest_sha256
                and type(result.get("verified_devices")) is int
                and result["verified_devices"] == workers * sources_per_worker
                and type(result.get("verified_points")) is int
                and result["verified_points"] == expected_points)
        except (OSError, ValueError, AttributeError) as exc:
            fail(f"Cannot read database verification: {exc}")
        if not readback_passed:
            fail("Database readback failed, incomplete, or bound to a different manifest")
    if error_count:
        status = "FAILED"
    elif functional_only:
        status = "FUNCTIONAL_VALIDATED" if readback_passed else "FUNCTIONAL_COMPLETE_REQUIRES_READBACK"
    else:
        status = "PASS" if readback_passed else "WRITE_COMPLETE_REQUIRES_READBACK"
    return {
        "status": status, "performance_acceptance": status == "PASS",
        "functional_only": functional_only, "run_id": run_id, "device_path": device_path,
        "workers": workers, "sources_per_worker": sources_per_worker,
        "batches_per_source": batches_per_source, "expected_points": expected_points,
        "formal_start_ms": t0, "formal_end_ms": t1, "wall_seconds": seconds,
        "formal_samples": formal_samples, "failed_samples": failed_samples,
        "unique_ack_points": ack_points, "unique_ack_points_per_second": rate,
        "common_active_start_ms": active_start, "common_active_end_ms": active_end,
        "manifest_sha256": manifest_sha256, "readback_passed": readback_passed,
        "error_count": error_count, "errors": errors, "sources": source_results,
    }, manifest_text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("jtl", nargs="+")
    parser.add_argument("--run-id")
    parser.add_argument("--device-path", default="root.jmeter.fixed")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--sources-per-worker", type=int, default=50)
    parser.add_argument("--batches-per-source", type=int, default=2400)
    parser.add_argument("--functional-only", action="store_true")
    parser.add_argument("--manifest-out", required=True)
    parser.add_argument("--summary-out", required=True)
    parser.add_argument("--readback")
    args = parser.parse_args()
    try:
        outputs = [Path(args.manifest_out).resolve(), Path(args.summary_out).resolve()]
        protected = [Path(path).resolve() for path in args.jtl]
        if args.readback:
            protected.append(Path(args.readback).resolve())

        def same_file(first, second):
            return first == second or (first.exists() and second.exists() and first.samefile(second))

        if same_file(*outputs) or any(same_file(output, source)
                                     for output in outputs for source in protected):
            raise ValueError("Outputs must be distinct and must not overwrite JTL or readback evidence")
        summary, manifest = validate(args.jtl, run_id=args.run_id, workers=args.workers,
            sources_per_worker=args.sources_per_worker, batches_per_source=args.batches_per_source,
            functional_only=args.functional_only, readback=args.readback, device_path=args.device_path)
        Path(args.manifest_out).write_bytes(manifest.encode("utf-8"))
        Path(args.summary_out).write_text(json.dumps(summary, ensure_ascii=False,
            indent=2, allow_nan=False) + "\n", encoding="utf-8")
    except (OSError, ValueError) as exc:
        parser.exit(2, f"Cannot validate fixed workload: {exc}\n")
    print(summary["status"])
    return 1 if summary["status"] == "FAILED" else 0


if __name__ == "__main__":
    sys.exit(main())
