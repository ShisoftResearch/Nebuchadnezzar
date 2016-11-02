#![crate_type = "lib"]
#![feature(proc_macro)]
#![feature(integer_atomics)]

extern crate libc;
extern crate uuid;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate concurrent_hashmap;
extern crate crossbeam;

pub mod ram;
pub mod server;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
