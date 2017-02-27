#![crate_type = "lib"]
#![feature(proc_macro)]
#![feature(integer_atomics)]
#![feature(plugin)]

#![plugin(bifrost_plugins)]

extern crate libc;
extern crate uuid;
extern crate concurrent_hashmap;

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