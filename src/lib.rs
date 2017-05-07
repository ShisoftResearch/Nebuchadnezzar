#![crate_type = "lib"]
#![feature(proc_macro)]
#![feature(integer_atomics)]
#![feature(plugin)]
#![feature(asm)]
#![feature(core_intrinsics)]
#![feature(btree_range, collections_bound)]
#![plugin(bifrost_plugins)]
#![feature(conservative_impl_trait)]

extern crate libc;
extern crate uuid;
extern crate chashmap;

pub mod ram;
pub mod server;

#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate bifrost;
extern crate bifrost_plugins;
extern crate bifrost_hasher;

extern crate bincode;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate parking_lot;
extern crate core;
extern crate rand;
extern crate futures;
#[macro_use]
extern crate itertools;
extern crate linked_hash_map;