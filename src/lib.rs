#![crate_type = "lib"]
#![feature(proc_macro)]

extern crate libc;
extern crate uuid;
extern crate serde;
extern crate serde_json;
extern crate lfmap;
#[macro_use]
extern crate serde_derive;

pub mod ram;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
