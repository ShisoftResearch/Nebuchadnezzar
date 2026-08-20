#![feature(test)]
#![feature(async_closure)]
#![feature(async_fn_traits)]
#![feature(thread_id_value)]
#![feature(is_sorted)]

#[cfg(feature = "mimalloc-allocator")]
#[global_allocator]
static GLOBAL_ALLOC: mem_shim::CountingAlloc<mimalloc::MiMalloc> =
    mem_shim::CountingAlloc(mimalloc::MiMalloc);

// Mutually exclusive with the above: both define a global allocator.
#[cfg(all(feature = "jemalloc-profiling", not(feature = "mimalloc-allocator")))]
#[global_allocator]
static GLOBAL_ALLOC: mem_shim::CountingAlloc<tikv_jemallocator::Jemalloc> =
    mem_shim::CountingAlloc(tikv_jemallocator::Jemalloc);

// No custom allocator selected: still count, wrapping the system allocator,
// so heap accounting is identical across allocator choices.
#[cfg(not(any(feature = "mimalloc-allocator", feature = "jemalloc-profiling")))]
#[global_allocator]
static GLOBAL_ALLOC: mem_shim::CountingAlloc<std::alloc::System> =
    mem_shim::CountingAlloc(std::alloc::System);

extern crate static_assertions;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate bifrost;
extern crate bifrost_hasher;
extern crate bifrost_plugins;
#[allow(unused_imports)]
#[macro_use]
pub extern crate dovahkiin;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate byteorder;
extern crate core;
extern crate libc;
extern crate num_cpus;
extern crate parking_lot;
extern crate rand;
extern crate serde;
#[allow(unused_imports)]
#[macro_use]
extern crate itertools;
extern crate serde_json;
extern crate smallvec;
extern crate test;

pub mod mem_shim;
pub mod utils;
#[macro_use]
pub mod ram;
pub mod client;
pub mod fill;
pub mod migration;
pub mod slots;
pub mod exec;
pub mod index;
pub mod query;
pub mod server;
