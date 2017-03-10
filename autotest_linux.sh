#!/bin/bash

RUST_BACKTRACE=1 cargo test -- --nocapture
# RUST_BACKTRACE=1 fswatch src/ tests/ -e ".*" -i "\\.rs$" | (while read; do cargo test; done)

while true; do

inotifywait -e modify,create,delete -r src/ tests/ && \
RUST_BACKTRACE=1 RUST_LOG=debug cargo test -- --nocapture

done