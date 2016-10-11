#!/bin/bash
cargo test
fswatch src/ tests/ -e ".*" -i "\\.rs$" | (while read; do cargo test; done)