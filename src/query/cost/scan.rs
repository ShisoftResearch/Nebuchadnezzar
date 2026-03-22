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
        let stat = self.database_runtime.chunks().overall_statistics(schema);
        let row_count = stat.count;
        let row_bytes = row_bytes(schema, &projection, self.database_runtime.meta(), &stat)?;
        Some(CostResult {
            row_count,
            row_bytes,
        })
    }
}
