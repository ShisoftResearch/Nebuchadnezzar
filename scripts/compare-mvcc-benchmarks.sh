#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 baseline-1.json baseline-2.json baseline-3.json -- candidate-1.json candidate-2.json candidate-3.json" >&2
  exit 2
}

if [[ "${1:-}" == "--self-test" ]]; then
  test_dir="$(mktemp -d)"
  trap 'rm -rf "$test_dir"' EXIT
  python3 - "$test_dir" <<'PY'
import json
import pathlib
import sys

directory = pathlib.Path(sys.argv[1])
summary = {
    "committed": 10, "attempts": 10, "not_realizable": 0,
    "logical_retries": 0, "waits": 0, "commits_per_second": 100.0,
    "p50_ns": 10, "p95_ns": 15, "p99_ns": 20, "unexpected": [],
    "invariants_passed": True, "retained_revisions": 0,
    "retained_bytes": 0, "segment_count": 1,
}
report = {"label": "self-test", "revision": "test", "scenarios": {"mvcc/rmw_one_cell": summary}}
for name in ("base-1", "base-2", "base-3", "candidate-1", "candidate-2", "candidate-3"):
    with (directory / f"{name}.json").open("w", encoding="utf-8") as handle:
        json.dump(report, handle)
bad = json.loads(json.dumps(report))
bad["scenarios"]["mvcc/rmw_one_cell"]["unexpected"] = ["injected failure"]
with (directory / "candidate-bad.json").open("w", encoding="utf-8") as handle:
    json.dump(bad, handle)
invariants_failed = json.loads(json.dumps(report))
invariants_failed["scenarios"]["mvcc/rmw_one_cell"]["invariants_passed"] = False
with (directory / "candidate-invariants-failed.json").open("w", encoding="utf-8") as handle:
    json.dump(invariants_failed, handle)
names_mismatched = json.loads(json.dumps(report))
names_mismatched["scenarios"] = {"mvcc/different_name": summary}
with (directory / "candidate-names-mismatched.json").open("w", encoding="utf-8") as handle:
    json.dump(names_mismatched, handle)
regression = json.loads(json.dumps(report))
regression["scenarios"]["mvcc/rmw_one_cell"]["commits_per_second"] = 90.0
with (directory / "candidate-regression.json").open("w", encoding="utf-8") as handle:
    json.dump(regression, handle)
p99_regression = json.loads(json.dumps(report))
p99_regression["scenarios"]["mvcc/rmw_one_cell"]["p99_ns"] = 22
with (directory / "candidate-p99-regression.json").open("w", encoding="utf-8") as handle:
    json.dump(p99_regression, handle)
for index, throughput in enumerate((90.0, 100.0, 110.0), 1):
    high_cv = json.loads(json.dumps(report))
    high_cv["scenarios"]["mvcc/rmw_one_cell"]["commits_per_second"] = throughput
    with (directory / f"candidate-high-cv-{index}.json").open("w", encoding="utf-8") as handle:
        json.dump(high_cv, handle)
PY
  "$0" "$test_dir/base-1.json" "$test_dir/base-2.json" "$test_dir/base-3.json" -- \
    "$test_dir/candidate-1.json" "$test_dir/candidate-2.json" "$test_dir/candidate-3.json"
  if "$0" "$test_dir/base-1.json" "$test_dir/base-2.json" "$test_dir/base-3.json" -- \
    "$test_dir/candidate-bad.json" "$test_dir/candidate-2.json" "$test_dir/candidate-3.json"; then
    echo "self-test expected the unexpected-outcome case to fail" >&2
    exit 1
  fi
  if "$0" "$test_dir/base-1.json" "$test_dir/base-2.json" "$test_dir/base-3.json" -- \
    "$test_dir/candidate-invariants-failed.json" "$test_dir/candidate-2.json" "$test_dir/candidate-3.json"; then
    echo "self-test expected the failed-invariants case to fail" >&2
    exit 1
  fi
  if "$0" "$test_dir/base-1.json" "$test_dir/base-2.json" "$test_dir/base-3.json" -- \
    "$test_dir/candidate-names-mismatched.json" "$test_dir/candidate-2.json" "$test_dir/candidate-3.json"; then
    echo "self-test expected the mismatched-names case to fail" >&2
    exit 1
  fi
  if "$0" "$test_dir/base-1.json" "$test_dir/base-2.json" "$test_dir/base-3.json" -- \
    "$test_dir/candidate-regression.json" "$test_dir/candidate-regression.json" "$test_dir/candidate-regression.json"; then
    echo "self-test expected the reproducible regression case to fail" >&2
    exit 1
  fi
  if "$0" "$test_dir/base-1.json" "$test_dir/base-2.json" "$test_dir/base-3.json" -- \
    "$test_dir/candidate-p99-regression.json" "$test_dir/candidate-p99-regression.json" "$test_dir/candidate-p99-regression.json"; then
    echo "self-test expected the p99 regression case to fail" >&2
    exit 1
  fi
  if "$0" "$test_dir/base-1.json" "$test_dir/base-2.json" "$test_dir/base-3.json" -- \
    "$test_dir/candidate-high-cv-1.json" "$test_dir/candidate-high-cv-2.json" "$test_dir/candidate-high-cv-3.json"; then
    echo "self-test expected the high-CV case to fail" >&2
    exit 1
  fi
  echo "comparator self-test passed"
  exit 0
fi

[[ $# -eq 7 && "$4" == "--" ]] || usage
for report in "$1" "$2" "$3" "$5" "$6" "$7"; do
  [[ -f "$report" ]] || { echo "missing benchmark report: $report" >&2; exit 2; }
done

python3 - "$@" <<'PY'
import json
import math
import statistics
import sys

baseline_paths = sys.argv[1:4]
candidate_paths = sys.argv[5:8]

def load(path):
    with open(path, encoding="utf-8") as handle:
        report = json.load(handle)
    scenarios = report.get("scenarios")
    if not isinstance(scenarios, dict):
        raise ValueError(f"{path}: missing scenarios object")
    return scenarios

def cv(values):
    mean = statistics.fmean(values)
    if mean <= 0:
        return math.inf
    return statistics.pstdev(values) / mean

def historical(name):
    return name.startswith("mvcc/history_") or name in {
        "mvcc/hot_cell_old_snapshot",
        "mvcc/cleaner_retained_revisions",
        "mvcc/cleaner_reader_contention",
    }

try:
    baseline = [load(path) for path in baseline_paths]
    candidate = [load(path) for path in candidate_paths]
except (OSError, ValueError, json.JSONDecodeError) as error:
    print(f"invalid benchmark report: {error}", file=sys.stderr)
    sys.exit(2)

expected = set(baseline[0])
failed = False
for side, reports in (("baseline", baseline), ("candidate", candidate)):
    for index, report in enumerate(reports, 1):
        if set(report) != expected:
            print(f"{side} report {index} scenario names do not match baseline report 1", file=sys.stderr)
            failed = True
        for name, summary in report.items():
            unexpected = summary.get("unexpected")
            if not isinstance(unexpected, list) or unexpected:
                print(f"{side} report {index} {name}: nonempty or invalid unexpected list", file=sys.stderr)
                failed = True
            if summary.get("invariants_passed") is not True:
                print(f"{side} report {index} {name}: invariants did not pass", file=sys.stderr)
                failed = True

if failed:
    sys.exit(1)

for name in sorted(expected):
    try:
        before_tp = [float(report[name]["commits_per_second"]) for report in baseline]
        after_tp = [float(report[name]["commits_per_second"]) for report in candidate]
        before_p99 = [float(report[name]["p99_ns"]) for report in baseline]
        after_p99 = [float(report[name]["p99_ns"]) for report in candidate]
    except (KeyError, TypeError, ValueError) as error:
        print(f"{name}: invalid metric: {error}", file=sys.stderr)
        failed = True
        continue
    before_cv, after_cv = cv(before_tp), cv(after_tp)
    before_median, after_median = statistics.median(before_tp), statistics.median(after_tp)
    before_p99_median, after_p99_median = statistics.median(before_p99), statistics.median(after_p99)
    throughput_delta = math.inf if before_median == 0 else (after_median / before_median - 1.0) * 100.0
    p99_delta = math.inf if before_p99_median == 0 else (after_p99_median / before_p99_median - 1.0) * 100.0
    print(f"{name}: throughput median {before_median:.3f} -> {after_median:.3f} ({throughput_delta:+.2f}%), p99 median {before_p99_median:.0f} -> {after_p99_median:.0f} ({p99_delta:+.2f}%), throughput CV baseline={before_cv:.2%} candidate={after_cv:.2%}")
    if before_cv >= 0.05 or after_cv >= 0.05:
        print(f"{name}: throughput CV must be below 5% on both sides", file=sys.stderr)
        failed = True
    if not historical(name) and (throughput_delta < -5.0 or p99_delta > 5.0):
        print(f"{name}: reproducible regression exceeds the 5% gate", file=sys.stderr)
        failed = True

sys.exit(1 if failed else 0)
PY
