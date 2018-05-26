#![crate_type = "lib"]
#![feature(proc_macro)]
#![feature(plugin)]
#![feature(asm)]
#![plugin(bifrost_plugins)]
#![feature(exact_size_is_empty)]
#![feature(use_extern_macros)]
#![feature(integer_atomics)]

#![feature(proc_macro, generators)]
#![feature(box_syntax)]


#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate bifrost;
extern crate bifrost_hasher;
#[macro_use]
pub extern crate dovahkiin;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate bitflags;

extern crate bincode;
extern crate serde;
extern crate parking_lot;
extern crate core;
extern crate rand;
extern crate futures_await as futures;
extern crate futures_cpupool;
extern crate linked_hash_map;
extern crate libc;
extern crate chashmap;
extern crate num_cpus;
extern crate byteorder;

pub mod utils;
#[macro_use]
pub mod ram;
pub mod server;
pub mod client;