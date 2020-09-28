#![feature(asm)]
#![feature(box_syntax)]
#![feature(test)]
#![feature(async_closure)]

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
#[allow(unused_imports)]
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
