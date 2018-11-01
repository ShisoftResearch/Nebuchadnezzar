#![feature(integer_atomics)]

#[macro_use]
extern crate neb;
#[macro_use]
extern crate dovahkiin;
#[macro_use]
extern crate bifrost;

mod cell;
mod chunk;
mod cleaner;
mod client;
mod index;
mod server;
mod transaction;
mod types;

#[macro_use]
extern crate log;
extern crate env_logger;
extern crate rand;
#[macro_use]
extern crate lazy_static;
extern crate futures_await as futures;
extern crate itertools;
extern crate parking_lot;
extern crate rayon;

use neb::ram::schema::Field;

pub fn default_fields() -> Field {
    Field::new(
        &String::from("*"),
        0,
        false,
        false,
        Some(vec![
            Field::new(&String::from("id"), 6, false, false, None),
            Field::new(&String::from("name"), 20, false, false, None),
            Field::new(&String::from("score"), 10, false, false, None),
        ]),
    )
}
