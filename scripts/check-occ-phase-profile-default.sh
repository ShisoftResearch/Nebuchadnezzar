#!/usr/bin/env bash
set -euo pipefail

cargo rustc --lib --release -- --emit=obj

artifact="$(
  find "target/release/deps" -maxdepth 1 -type f -name 'neb-*.o' -printf '%T@ %p\n' \
    | sort -nr \
    | awk 'NR == 1 { $1 = ""; sub(/^ /, ""); print; exit }'
)"

if [[ -z "${artifact}" ]]; then
  echo "error: no release library object found under target/release/deps" >&2
  exit 1
fi

if nm -a "${artifact}" | grep -q 'phase_profile'; then
  echo "error: default OCC profile check found profiler symbols in ${artifact}" >&2
  exit 1
fi

echo "default OCC profile check: no profiler symbols in ${artifact}"
