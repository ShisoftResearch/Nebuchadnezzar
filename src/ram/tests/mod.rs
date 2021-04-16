mod cell;
mod chunk;
mod types;

use dovahkiin::types::Type;

use crate::ram::schema::Field;

pub fn default_fields() -> Field {
    Field::new(
        &String::from("*"),
        Type::Map,
        false,
        false,
        Some(vec![
            Field::new(&String::from("id"), Type::I64, false, false, None, vec![]),
            Field::new(
                &String::from("name"),
                Type::String,
                false,
                false,
                None,
                vec![],
            ),
            Field::new(
                &String::from("score"),
                Type::U64,
                false,
                false,
                None,
                vec![],
            ),
        ]),
        vec![],
    )
}
