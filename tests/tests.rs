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
#[macro_use]
extern crate lazy_static;

use neb::ram::schema::Field;

pub fn default_fields () -> Field {
    Field {
        type_id: 0,
        name: String::from("*"),
        nullable: false,
        is_array: false,
        sub: Some(vec![
            Field {
                type_id: 6,
                name: String::from("id"),
                nullable:false,
                is_array:false,
                sub: None,
            },
            Field {
                type_id: 20,
                name: String::from("name"),
                nullable:false,
                is_array:false,
                sub: None,
            },
            Field {
                type_id: 10,
                name: String::from("score"),
                nullable:false,
                is_array:false,
                sub: None,
            }
        ])
    }
}