#![crate_type = "lib"]

extern crate libc;
extern crate uuid;

pub mod ram;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;