#!/bin/bash

RUST_BACKTRACE=1

cargo test
# RUST_BACKTRACE=1 fswatch src/ tests/ -e ".*" -i "\\.rs$" | (while read; do cargo test; done)

while true; do

inotifywait -e modify,create,delete -r src/ tests/ && \
cargo test

done