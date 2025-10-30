pub mod alignment_tests;
pub mod cell;
pub mod chunk;
pub mod throughput_test;
pub mod types;

use dovahkiin::types::Type;

use crate::ram::schema::{Field, IndexType};

pub fn default_fields() -> Field {
    Field::new_schema(vec![
        Field::new_indexed("id", Type::I64, vec![IndexType::Statistics]),
        Field::new_unindexed("name", Type::String),
        Field::new_indexed("score", Type::U64, vec![IndexType::Statistics]),
    ])
}

pub fn simple_fields() -> Field {
    Field::new_unindexed("*", Type::U64)
}

pub fn complex_fields() -> Field {
    Field::new_schema(vec![
        Field::new_unindexed("id", Type::I64),
        Field::new_unindexed_array("strings", Type::String),
        Field::new_unindexed("num", Type::U64),
        Field::new_unindexed_vector("vec1", Type::F64, 16),
        Field::new_unindexed_vector_nullable("vec2", Type::F32, 8),
        Field::new_unindexed_array_nullable("nums", Type::U64),
        Field::new_map(
            "sub",
            vec![
                Field::new_unindexed("sub1", Type::U32),
                Field::new_unindexed_array("sub2", Type::U32),
                Field::new_unindexed("sub3", Type::U32),
                Field::new_map(
                    "sub4",
                    vec![
                        Field::new_unindexed("sub4sub1", Type::U32),
                        Field::new_unindexed_array("sub4sub2", Type::U32),
                        Field::new_unindexed_array_nullable("sub4sub3", Type::U64),
                        Field::new_unindexed("sub4sub4", Type::U16),
                    ],
                ),
                dyn_map_field("sub5"),
                Field::new_unindexed("subend", Type::U32),
            ],
        ),
    ])
}

pub fn dyn_map_field<'a>(name: &'a str) -> Field {
    Field::new_map_array(
        name,
        vec![
            Field::new_unindexed("sub5sub1", Type::U32),
            Field::new_unindexed_array("sub5sub2", Type::U32),
            Field::new_unindexed_array_nullable("sub5sub3", Type::U64),
            Field::new_unindexed("sub5sub4", Type::U16),
        ],
    )
}
