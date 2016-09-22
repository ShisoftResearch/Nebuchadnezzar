# Nebuchadnezzar (JVM based version)

### WARNING: I found that building a storage system based on a JVM based language is not a good idea. To continue on this project and Morpheus for performance and memory efficacy, I am going to rewrite all of the code. See develop branch for newest code.

> "This is my ship...the Nebuchadnezzar, it's a hovercraft."
> ― Morpheus, The Matrix

RAMCloud implementation for Clojure applications.
It is a distributed in-memory Key-Value store, made to power the [Morpheus project](https://github.com/shisoft/Morpheus).

## Features

* In-memory compact key-value store, it can store the whole [Wikidata entity relation graph](https://dumps.wikimedia.org/wikidatawiki/entities/) (80.7GB) with only 20.8GB of RAM and 16.8GB of disk backup, for 108,004,015 cells.
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

Distributed under the Eclipse Public License either version 1.0 or BSD any later version.
