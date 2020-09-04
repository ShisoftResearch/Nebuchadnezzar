mod cell;
mod chunk;
mod types;

use crate::ram::schema::Field;

pub fn default_fields() -> Field {
    Field::new(
        &String::from("*"),
        0,
        false,
        false,
        Some(vec![
            Field::new(&String::from("id"), 6, false, false, None, vec![]),
            Field::new(&String::from("name"), 20, false, false, None, vec![]),
            Field::new(&String::from("score"), 10, false, false, None, vec![]),
        ]),
        vec![],
    )
}
