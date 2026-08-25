use crate::ram::schema::SchemaVid;
use std::sync::Arc;

use crate::server::DatabaseRuntime;

use super::*;

pub struct ScanIndexCost {
    database_runtime: Arc<DatabaseRuntime>,
}

impl CostFunction for ScanIndexCost {
    fn cost<'a>(
        &self,
        schema: u32,
        _field: Option<u64>,
        _range: Option<&ValueRange>,
        projection: Vec<u64>,
    ) -> Option<CostResult> {
        // TASK 3: statistics are a per-family aggregate and this selector is
        // logical, so both sides become `SchemaUid`. One generation today.
        let stat = self
            .database_runtime
            .chunks()
            .overall_statistics(SchemaVid(schema));
        let row_count = stat.count;
        let row_bytes = row_bytes(schema, &projection, self.database_runtime.meta(), &stat)?;
        Some(CostResult {
            row_count,
            row_bytes,
        })
    }
}
