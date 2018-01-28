#![crate_type = "lib"]
#![feature(proc_macro)]
#![feature(plugin)]
#![feature(asm)]
#![plugin(bifrost_plugins)]
#![feature(conservative_impl_trait)]
#![feature(exact_size_is_empty)]
#![feature(macro_reexport)]
#![feature(integer_atomics)]

#![feature(proc_macro, conservative_impl_trait, generators)]
#![feature(box_syntax)]


#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate bifrost;
extern crate bifrost_hasher;
#[macro_use]
#[macro_reexport(data_map)]
pub extern crate dovahkiin;
#[macro_use]
extern crate serde_derive;

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

pub mod utils;
#[macro_use]
pub mod ram;
pub mod server;
pub mod client;