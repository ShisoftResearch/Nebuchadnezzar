use std::sync::Arc;

use crate::{query::statistics::SchemaStatistics, server::NebServer};

use super::*;

pub struct RangeIndexCost {
    server: Arc<NebServer>,
}

impl CostFunction for RangeIndexCost {
    fn cost<'a>(
        &self,
        schema: u32,
        field: Option<u64>,
        range: Option<ValueRange<'a>>,
        projection: Vec<u64>,
        row_cost: u64,
    ) -> CostResult {
        // let statics = self
        //     .server
        //     .chunks
        //     .all_chunk_statistics(schema)
        //     .into_iter()
        //     .filter_map(|s| s)
        //     .collect::<Vec<_>>();
        unimplemented!()
    }
}
