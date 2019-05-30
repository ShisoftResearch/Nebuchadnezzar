# Nebuchadnezzar
[![Build Status](http://hq.shisoft.net:4211/buildStatus/icon?job=Neb%20Build%20Check)](http://hq.shisoft.net:4211/job/Neb%20Build%20Check/)
> "This is my ship...the Nebuchadnezzar, it's a hovercraft."
> ― Morpheus, The Matrix

High performance, rich typing RAMCloud implementation.
Distributed in-memory Key-Value store, optional transaction, made to power the [Morpheus project](https://github.com/shisoft/Morpheus).

* In-memory, persistant storage
* Shared-nothing distributed architecture
* Rich-typed schema (scalar, array, map, nested, length-variable data type)
* Hash based low overhead primal Key-Value operations 
* Timestamp transactions
* Non-blocking LSM-tree (B+ tree based) for range query (comming soon)

Nebuchadnezzar (aka Neb) have already been converted from it's former [Clojure version](https://github.com/shisoft/Nebuchadnezzar/tree/clojure-version) and will stay with rust afterwards.  

Copyright © 2017 Shisoft Research

Distributed under the GNU Lesser General Public License v3.0 or any later version.
