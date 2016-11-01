#!/bin/bash
RUST_BACKTRACE=1 cargo test
RUST_BACKTRACE=1 fswatch src/ tests/ -e ".*" -i "\\.rs$" | (while read; do cargo test; done)
