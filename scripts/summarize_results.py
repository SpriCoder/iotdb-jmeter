#!/usr/bin/env python3
"""Summarize sampler JTL counters using wall-clock duration, not summed thread latency."""

import argparse
import csv
import json
import math
from pathlib import Path
import sys


REQUIRED = {
    "timeStamp", "elapsed", "success", "iotdb_ack_rows", "iotdb_ack_points",
    "iotdb_attempted_rows", "iotdb_device", "iotdb_first_timestamp", "iotdb_last_timestamp",
}


def summarize(paths, window_start=None, window_end=None):
    if (window_start is None) != (window_end is None):
        raise ValueError("Provide both --window-start-ms and --window-end-ms")
    if window_start is not None and window_start >= window_end:
        raise ValueError("The window end must be greater than the window start")
    resolved = [Path(path).resolve() for path in paths]
    if len(set(resolved)) != len(resolved):
        raise ValueError("The same JTL file was supplied more than once")
    devices = {}
    samples = failures = ack_rows = ack_points = attempted = excluded = overlaps = 0
    earliest = latest = None
    for path in resolved:
        with path.open("r", encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            missing = REQUIRED - set(reader.fieldnames or [])
            if missing:
                raise ValueError(f"{path}: missing JTL columns: {', '.join(sorted(missing))}")
            for line, row in enumerate(reader, 2):
                try:
                    start, elapsed = int(row["timeStamp"]), int(row["elapsed"])
                    confirmed_rows = int(row["iotdb_ack_rows"])
                    points = int(row["iotdb_ack_points"])
                    sent_rows = int(row["iotdb_attempted_rows"])
                    first, last = int(row["iotdb_first_timestamp"]), int(row["iotdb_last_timestamp"])
                    success = row["success"].lower()
                    if success not in {"true", "false"}:
                        raise ValueError("success must be true/false")
                    if min(start, elapsed, confirmed_rows, points, sent_rows) < 0:
                        raise ValueError("negative time or row/point counter")
                    if confirmed_rows > sent_rows:
                        raise ValueError("acknowledged rows exceed attempted rows")
                    if (confirmed_rows == 0) != (points == 0) or points < confirmed_rows:
                        raise ValueError("inconsistent row/point counters")
                except (ValueError, TypeError, KeyError) as error:
                    raise ValueError(f"{path}:{line}: invalid sampler row ({error})") from error
                end = start + elapsed
                # Whole samples only: assigning an entire multi-RPC sample to
                # its start time would incorrectly attribute boundary batches.
                if window_start is not None and not (window_start <= start < window_end and end <= window_end):
                    excluded += 1
                    continue
                earliest = start if earliest is None else min(earliest, start)
                latest = end if latest is None else max(latest, end)
                samples += 1
                failures += success == "false"
                ack_rows += confirmed_rows
                ack_points += points
                attempted += sent_rows
                device = row["iotdb_device"] or "<setup failed>"
                item = devices.setdefault(device, {
                    "samples": 0, "failed_samples": 0, "acknowledged_rows": 0,
                    "acknowledged_points": 0, "attempted_rows": 0, "ranges": [],
                })
                item["samples"] += 1
                item["failed_samples"] += success == "false"
                item["acknowledged_rows"] += confirmed_rows
                item["acknowledged_points"] += points
                item["attempted_rows"] += sent_rows
                if first >= 0 and last >= first:
                    item["ranges"].append((first, last))
    if samples == 0:
        raise ValueError("No sampler rows fell inside the selected window")
    for item in devices.values():
        ranges = sorted(item.pop("ranges"))
        maximum = -1
        for first, last in ranges:
            if first <= maximum:
                overlaps += 1
            maximum = max(maximum, last)
        item["first_timestamp"] = ranges[0][0] if ranges else None
        item["last_timestamp"] = maximum if ranges else None
    start = earliest if window_start is None else window_start
    end = latest if window_end is None else window_end
    seconds = (end - start) / 1000.0
    rate = ack_points / seconds if seconds > 0 else None
    if rate is not None and not math.isfinite(rate):
        raise ValueError("The computed rate is not finite")
    return {
        "input_files": [str(path) for path in resolved],
        "window_start_ms": start,
        "window_end_ms": end,
        "window_seconds": seconds,
        "samples": samples,
        "failed_samples": failures,
        "excluded_samples": excluded,
        "acknowledged_rows": ack_rows,
        "acknowledged_points": ack_points,
        "attempted_rows": attempted,
        "unconfirmed_rows": attempted - ack_rows,
        "acknowledged_points_per_second": rate,
        "device_count": len(devices),
        "devices_with_acknowledged_data": sum(item["acknowledged_rows"] > 0 for item in devices.values()),
        "possible_overlapping_timestamp_ranges": overlaps,
        "count_reconciliation_required": True,
        "devices": devices,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("jtl", nargs="+", help="CSV JTL files with IoTDB sample_variables columns")
    parser.add_argument("--window-start-ms", type=int)
    parser.add_argument("--window-end-ms", type=int)
    args = parser.parse_args()
    try:
        result = summarize(args.jtl, args.window_start_ms, args.window_end_ms)
    except (OSError, ValueError) as error:
        parser.exit(2, f"Cannot summarize JTL: {error}\n")
    json.dump(result, sys.stdout, ensure_ascii=False, indent=2, allow_nan=False)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
