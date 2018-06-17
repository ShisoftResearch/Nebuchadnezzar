use neb::ram::cell;
use neb::ram::cell::*;
use neb::ram::schema::*;
use neb::ram::chunk::Chunks;
use neb::ram::types;
use neb::ram::types::*;
use neb::ram::io::writer;
use neb::ram::cleaner::Cleaner;
use neb::ram::schema::Field;
use neb::server::ServerMeta;
use env_logger;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std;
use std::rc::Rc;
use super::*;

pub fn default_fields () -> Field {
    Field::new ("*", 0, false, false, Some(
        vec![
            Field::new("id", type_id_of(Type::I32), false, false, None),
            Field::new("data", type_id_of(Type::U8), false, true, None)
        ]
    ))
}

#[test]
pub fn full_clean_cycle() {
    env_logger::init();
    // let
}