#![feature(integer_atomics)]


extern crate neb;

mod chunk;
mod types;
mod server;
mod cell;
mod transaction;

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate rand;
extern crate uuid;