pub mod cell;
pub mod chunk;
pub mod types;

use dovahkiin::types::Type;

use crate::ram::schema::{Field, IndexType};

pub fn default_fields() -> Field {
    Field::new(
        &String::from("*"),
        Type::Map,
        false,
        false,
        Some(vec![
            Field::new(
                &String::from("id"),
                Type::I64,
                false,
                false,
                None,
                vec![IndexType::Statistics],
            ),
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
                vec![IndexType::Statistics],
            ),
        ]),
        vec![],
    )
}

pub fn simple_fields() -> Field {
    Field::new(&String::from("*"), Type::U64, false, false, None, vec![])
}

pub fn complex_fields() -> Field {
    Field::new(
        &String::from("*"),
        Type::Map,
        false,
        false,
        Some(vec![
            Field::new(&String::from("id"), Type::I64, false, false, None, vec![]),
            Field::new(
                &String::from("strings"),
                Type::String,
                false, // Not null
                true,  // String array
                None,
                vec![],
            ),
            Field::new(&String::from("num"), Type::U64, false, false, None, vec![]),
            Field::new(
                &String::from("nums"),
                Type::U64,
                true, // Nullable
                true, // Is array
                None,
                vec![],
            ),
            Field::new(
                &String::from("sub"),
                Type::Map,
                false,
                false,
                Some(vec![
                    Field::new(&String::from("sub1"), Type::U32, false, false, None, vec![]),
                    Field::new(
                        &String::from("sub2"),
                        Type::U32,
                        false,
                        true, // array
                        None,
                        vec![],
                    ),
                    Field::new(&String::from("sub3"), Type::U32, false, false, None, vec![]),
                    Field::new(
                        &String::from("sub4"),
                        Type::Map,
                        false,
                        false,
                        Some(vec![
                            Field::new("sub4sub1", Type::U32, false, false, None, vec![]),
                            Field::new(
                                "sub4sub2",
                                Type::U32,
                                false,
                                true, // array
                                None,
                                vec![],
                            ),
                            Field::new(
                                "sub4sub3",
                                Type::U64,
                                true, // nullable
                                true, // array
                                None,
                                vec![],
                            ),
                            Field::new("sub4sub4", Type::U16, false, false, None, vec![]),
                        ]),
                        vec![],
                    ),
                    dyn_map_field("sub5"),
                    Field::new(
                        &String::from("subend"),
                        Type::U32,
                        false,
                        false,
                        None,
                        vec![],
                    ),
                ]),
                vec![],
            ),
        ]),
        vec![],
    )
}

pub fn dyn_map_field<'a>(name: &'a str) -> Field {
    Field::new(
        &String::from(name),
        Type::Map,
        false,
        true, // array
        Some(vec![
            Field::new("sub5sub1", Type::U32, false, false, None, vec![]),
            Field::new(
                "sub5sub2",
                Type::U32,
                false,
                true, // array
                None,
                vec![],
            ),
            Field::new(
                "sub5sub3",
                Type::U64,
                true, // nullable
                true, // array
                None,
                vec![],
            ),
            Field::new("sub5sub4", Type::U16, false, false, None, vec![]),
        ]),
        vec![],
    )
}
