#![crate_type = "lib"]
#![feature(proc_macro)]
#![feature(integer_atomics)]

extern crate libc;
extern crate uuid;
extern crate concurrent_hashmap;

pub mod ram;
pub mod server;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
extern crate mio;