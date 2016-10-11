#![crate_type = "lib"]

extern crate libc;
extern crate uuid;
extern crate serde;
extern crate serde_json;

pub mod ram;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;