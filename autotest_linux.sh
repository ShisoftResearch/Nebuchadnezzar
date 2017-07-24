#!/bin/bash

export RUST_BACKTRACE=full
export RUST_LOG=neb=debug

cargo test -- --nocapture
# RUST_BACKTRACE=1 fswatch src/ tests/ -e ".*" -i "\\.rs$" | (while read; do cargo test; done)

while true; do

inotifywait -e modify,create,delete -r src/ tests/ && \
cargo test -- --nocapture

done