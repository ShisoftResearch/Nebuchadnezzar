#![crate_type = "lib"]
#![feature(proc_macro)]
#![feature(asm)]
#![feature(exact_size_is_empty)]
#![feature(use_extern_macros)]
#![feature(integer_atomics)]
#![feature(generators)]
#![feature(box_syntax)]
#![feature(test)]
#![feature(proc_macro_hygiene)]

#[macro_use]
extern crate static_assertions;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate bifrost;
extern crate bifrost_hasher;
extern crate bifrost_plugins;
#[macro_use]
pub extern crate dovahkiin;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate bitflags;

extern crate bincode;
extern crate byteorder;
extern crate core;
extern crate libc;
extern crate linked_hash_map;
extern crate num_cpus;
extern crate parking_lot;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate itertools;
#[macro_use]
extern crate smallvec;
extern crate owning_ref;
extern crate serde_json;
extern crate test;

pub mod utils;
#[macro_use]
pub mod ram;
pub mod client;
pub mod index;
pub mod server;
