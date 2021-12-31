use std::sync::Arc;

use crate::server::NebServer;

use super::*;

pub struct ScanIndexCost {
    server: Arc<NebServer>,
}

impl CostFunction for ScanIndexCost {
    fn cost<'a>(
        &self,
        schema: u32,
        field: Option<u64>,
        range: Option<&ValueRange>,
        projection: Vec<u64>,
    ) -> Option<CostResult> {
        let stat = self.server.chunks.overall_statistics(schema);
        let nrow_count = stat.count;
        let row_bytes = row_bytes(schema, &projection, &self.server, &stat);
        Some(CostResult {
            row_count,
            row_bytes,
        })
    }
}
