#!/usr/bin/env bash
set -euo pipefail

temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/neb-occ-phase-profile.XXXXXXXX")"
cargo_messages="${temp_dir}/cargo-messages.json"
nm_output="${temp_dir}/nm-output.txt"

cleanup() {
  rm -f -- "${cargo_messages}" "${nm_output}"
  rmdir -- "${temp_dir}" 2>/dev/null || true
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

if ! cargo rustc --message-format=json --lib --release -- --emit=obj \
  >"${cargo_messages}"; then
  echo "error: default release library build failed" >&2
  exit 1
fi

rmeta="$(
  awk '
    /"reason":"compiler-artifact"/ &&
    /"target":[{][^}]*"name":"neb"/ {
      if (match($0, /"[^"]*\/libneb-[^"]*[.]rmeta"/)) {
        print substr($0, RSTART + 1, RLENGTH - 2)
        exit
      }
    }
  ' "${cargo_messages}"
)"

if [[ -z "${rmeta}" ]]; then
  echo "error: could not resolve the neb release metadata artifact from Cargo JSON" >&2
  exit 1
fi

artifact_dir="${rmeta%/*}"
artifact_name="${rmeta##*/}"
artifact_name="${artifact_name#lib}"
artifact="${artifact_dir}/${artifact_name%.rmeta}.o"

if [[ ! -f "${artifact}" ]]; then
  echo "error: Cargo's default release object does not exist: ${artifact}" >&2
  exit 1
fi

nm_bin="${NM:-nm}"
if ! "${nm_bin}" -a "${artifact}" >"${nm_output}"; then
  echo "error: ${nm_bin} failed while reading ${artifact}" >&2
  exit 1
fi

if grep -Fq 'phase_profile' "${nm_output}"; then
  echo "error: default OCC profile check found profiler symbols in ${artifact}" >&2
  exit 1
fi

echo "default OCC profile check: no profiler symbols in ${artifact}"
