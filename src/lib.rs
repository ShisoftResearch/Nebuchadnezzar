#![crate_type = "lib"]
#![feature(proc_macro)]
#![feature(plugin)]
#![feature(asm)]
#![plugin(bifrost_plugins)]
#![feature(conservative_impl_trait)]

extern crate libc;
extern crate chashmap;

pub mod utils;
pub mod ram;
pub mod server;
pub mod client;

#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate bifrost;
extern crate bifrost_hasher;

extern crate bincode;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate parking_lot;
extern crate core;
extern crate rand;
extern crate futures;
extern crate linked_hash_map;