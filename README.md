# Nebuchadnezzar

RAMCloud implementation for Clojure applications.

## Features

* In-memory Key-Value store
* Full off-heap, no GC impact and pauses, internal defragment mechanism
* High concurrency, cell level lock
* Able to allocate large amount of memory (TBs)
* Low latency, tests indicates each read/write costs less than 1 ~ 1/10 msec
* Support both schema (for memory efficient) and schema less
* Shard-nothing distributed architecture (working in progress)

## Usage

Under heavy development, no docs at this time

## License

Copyright © 2016 Shisoft Research

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
