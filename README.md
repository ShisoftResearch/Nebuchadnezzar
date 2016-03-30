# Nebuchadnezzar

RAMCloud implementation for Clojure applications.

## Features

* In-memory Key-Value store
* Full off-heap, no GC impact and pauses, internal defragment mechanism
* High concurrency, cell level lock
* Able to allocate large amount of memory (TBs)
* Low latency, tests indicates each read/write costs less than 1 ~ 1/100 msec
* Support both schema (for memory efficiency) and schema less
* Shard-nothing distributed architecture
* Rich data type support, including text, geo coordinate, bytes, java objects, and arrays
* Array, Map and nested schemas. Use the DSL to describe your documents
* Optional durability. Backup memory data into multiply replications of on-disk copy, in (nearly) real-time, recover them back into the cluster memory when needed.

## Usage

Under heavy development, no docs at this time

## License

Copyright © 2016 Shisoft Research

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
