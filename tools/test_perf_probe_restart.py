#!/usr/bin/env python3
"""Check the core probe across separate processes and preserve its evidence.

Run this script in a 24 GiB systemd scope with a prebuilt probe binary.
The output directory must not exist. No store or log is removed.
"""

import argparse
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cells", type=int, default=10000)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    if not os.access(binary, os.X_OK):
        parser.error("the prebuilt probe is not executable")
    if args.cells < 1000:
        parser.error("use at least 1000 cells to exercise batched transactions")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    result = {"binary": str(binary), "cells": args.cells, "commands": [], "passed": False}
    try:
        for phase in ("populate", "recover"):
            command = [str(binary), phase, "--base", str(output / "store"),
                       "--cells", str(args.cells)]
            with (output / f"{phase}.log").open("w") as log:
                process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
                record = {"argv": command, "pid": process.pid,
                          "address": f"127.0.0.1:{47900 + process.pid % 100}"}
                result["commands"].append(record)
                try:
                    record["exit_code"] = process.wait(timeout=180)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise AssertionError(f"{phase} exceeded 180 seconds")
            assert record["exit_code"] == 0, f"{phase} failed; see {phase}.log"
        addresses = [record["address"] for record in result["commands"]]
        assert addresses[0] != addresses[1], "fixture requires different process addresses"
        metrics = (output / "recover.log").read_text().splitlines()
        assert "METRIC sampled_wrong 0" in metrics, "recovery did not preserve the sampled cells"
        result["passed"] = True
    finally:
        (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
