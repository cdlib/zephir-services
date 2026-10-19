#!/usr/bin/env python3
"""Benchmark this checkout's CID inquiry CLI using only the standard library.

Generate the same query workload on each branch by keeping --seed and other
workload options unchanged. Reports contain every query, result, and duration.
"""

import argparse
import datetime
import hashlib
import json
import os
import random
import subprocess
import sys
import time


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default="dev", choices=("test", "dev", "stg", "prd"))
    parser.add_argument("--count", type=int, default=1000, help="Number of queries (default: 1000)")
    parser.add_argument("--batch-size", type=int, default=8, help="OCNs per query (default: 8)")
    parser.add_argument("--min-ocn", type=int, default=1)
    parser.add_argument("--max-ocn", type=int, default=1200000000)
    parser.add_argument("--seed", type=int, default=20261019)
    parser.add_argument("--queries", help="Optional file, one comma-separated OCN query per line")
    parser.add_argument("--sysids", help="Optional file with one representative system ID per line")
    parser.add_argument("--report", required=True, help="Output JSON report path")
    parser.add_argument("--compare", help="Compare this report with an earlier branch's report")
    parser.add_argument("--python", default=sys.executable, help="Python interpreter to run cid_inquiry.py")
    parser.add_argument("--inquiry", default=os.path.join(os.path.dirname(__file__), "cid_inquiry.py"),
                        help="Path to cid_inquiry.py")
    parser.add_argument("--local-minter", default=os.path.join(os.path.dirname(__file__), "local_cid_minter.py"),
                        help="Path to local_cid_minter.py")
    parser.add_argument("--warmups", type=int, default=1, help="Warmup inquiries excluded from measurements")
    return parser.parse_args()


def load_queries(args):
    if args.queries:
        queries = []
        with open(args.queries, encoding="utf-8") as file:
            for line in file:
                values = [part.strip() for part in line.split(",") if part.strip()]
                if values:
                    queries.append([int(value) for value in values])
        if not queries:
            raise ValueError("query file contains no queries")
        return queries
    if args.count < 1 or args.batch_size < 1 or args.warmups < 0:
        raise ValueError("count and batch-size must be positive; warmups cannot be negative")
    if args.min_ocn < 1 or args.max_ocn < args.min_ocn:
        raise ValueError("OCN bounds must be positive and ordered")
    rng = random.Random(args.seed)
    return [[rng.randint(args.min_ocn, args.max_ocn) for _ in range(args.batch_size)]
            for _ in range(args.count)]


def run_query(command):
    started = time.perf_counter()
    completed = subprocess.run(command, text=True, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, check=False)
    duration = time.perf_counter() - started
    if completed.returncode:
        raise RuntimeError("lookup failed (exit {}): {}\n{}".format(
            completed.returncode, " ".join(command), completed.stderr.strip()))
    try:
        result = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError("lookup returned invalid JSON for {}: {}".format(command, error))
    return result, duration


def make_workloads(args, queries):
    workloads = {
        "cid_inquiry_by_ocns": [
            [args.python, args.inquiry, args.env, ",".join(map(str, query))]
            for query in queries
        ],
        "minter_store_by_ocns": [
            [args.python, args.local_minter, args.env, "read", "ocn", ",".join(map(str, query))]
            for query in queries
        ],
    }
    if args.sysids:
        with open(args.sysids, encoding="utf-8") as file:
            sysids = [line.strip() for line in file if line.strip()]
        if not sysids:
            raise ValueError("system ID file contains no values")
    else:
        sysids = ["benchmark-sysid-{}".format(index) for index in range(len(queries))]
    workloads["minter_store_by_sysid"] = [
        [args.python, args.local_minter, args.env, "read", "sysid", sysid]
        for sysid in sysids
    ]
    return workloads


def main():
    args = arguments()
    try:
        queries = load_queries(args)
        lookup_workloads = make_workloads(args, queries)
        all_results = {}
        for lookup_type, commands in lookup_workloads.items():
            for _ in range(args.warmups):
                run_query(commands[0])
            entries = []
            total = 0.0
            for index, command in enumerate(commands):
                result, seconds = run_query(command)
                total += seconds
                entries.append({"index": index, "input": command[-1],
                                "seconds": seconds, "result": result})
            all_results[lookup_type] = {
                "query_count": len(entries),
                "total_seconds": total,
                "queries_per_second": len(entries) / total if total else None,
                "queries": entries,
            }
    except (OSError, ValueError, RuntimeError) as error:
        print("benchmark failed: {}".format(error), file=sys.stderr)
        return 2

    report = {
        "format": "cid-inquiry-benchmark-v1",
        "created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "branch": subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"],
                                  text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout.strip(),
        "commit": subprocess.run(["git", "rev-parse", "HEAD"], text=True,
                                 stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout.strip(),
        "env": args.env,
        "lookup_types": all_results,
        "seed": args.seed if not args.queries else None,
        "query_file_sha256": None,
        "batch_size": args.batch_size if not args.queries else None,
        "warmups": args.warmups,
    }
    if args.queries:
        with open(args.queries, "rb") as file:
            report["query_file_sha256"] = hashlib.sha256(file.read()).hexdigest()
    with open(args.report, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=2, sort_keys=True)
        file.write("\n")
    print("Wrote {} lookup types to {}".format(len(all_results), args.report))
    for lookup_type, values in all_results.items():
        print("{}: {} queries, {:.3f}s, {:.2f} queries/s".format(
            lookup_type, values["query_count"], values["total_seconds"],
            values["queries_per_second"] or 0.0))
    if args.compare:
        try:
            with open(args.compare, encoding="utf-8") as file:
                previous = json.load(file)
        except (OSError, json.JSONDecodeError) as error:
            print("could not read comparison report: {}".format(error), file=sys.stderr)
            return 2
        if set(previous.get("lookup_types", {})) != set(all_results):
            print("reports contain different lookup types", file=sys.stderr)
            return 1
        print("Compared with {} ({}):".format(args.compare, previous.get("branch", "unknown branch")))
        any_mismatch = False
        for lookup_type, after_values in all_results.items():
            before_values = previous["lookup_types"][lookup_type]
            if len(before_values["queries"]) != len(after_values["queries"]):
                print("  {}: query count differs".format(lookup_type), file=sys.stderr)
                any_mismatch = True
                continue
            mismatches = [index for index, (before, after) in enumerate(
                zip(before_values["queries"], after_values["queries"]))
                if before.get("input") != after["input"] or before.get("result") != after["result"]]
            old_total = before_values["total_seconds"]
            print("  {}: {} result mismatches, elapsed change {:+.2f}%".format(
                lookup_type, len(mismatches),
                (after_values["total_seconds"] / old_total - 1) * 100 if old_total else 0))
            if mismatches:
                any_mismatch = True
                print("    differing query indexes: {}".format(
                    ", ".join(str(index + 1) for index in mismatches[:10])), file=sys.stderr)
        if any_mismatch:
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
