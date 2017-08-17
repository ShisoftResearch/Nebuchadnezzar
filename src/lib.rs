#![crate_type = "lib"]
#![feature(proc_macro)]
#![feature(plugin)]
#![feature(asm)]
#![plugin(bifrost_plugins)]
#![feature(conservative_impl_trait)]
#![feature(exact_size_is_empty)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate bifrost;
extern crate bifrost_hasher;
#[macro_use]
extern crate dovahkiin;
#[macro_use]
extern crate serde_derive;

extern crate bincode;
extern crate serde;
extern crate parking_lot;
extern crate core;
extern crate rand;
extern crate futures;
extern crate linked_hash_map;
extern crate libc;
extern crate chashmap;

pub use dovahkiin;

pub mod utils;
#[macro_use]
pub mod ram;
pub mod server;
pub mod client;