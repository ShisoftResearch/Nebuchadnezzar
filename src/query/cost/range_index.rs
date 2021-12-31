use std::sync::Arc;

use crate::server::NebServer;

use super::*;

pub struct RangeIndexCost {
    server: Arc<NebServer>,
}

impl CostFunction for RangeIndexCost {
    fn cost(
        &self,
        schema: u32,
        field: Option<u64>,
        range: Option<&ValueRange>,
        projection: Vec<u64>,
    ) -> Option<CostResult> {
        let stat = self.server.chunks.overall_statistics(schema);
        let field = field?;
        let field_histo = stat.histogram.get(&field)?;
        let num_all_rows = stat.count;
        let range = range?;
        let start_index = range.start.pos_of(field_histo).unwrap_or(0);
        let end_index = range.end.pos_of(field_histo).unwrap_or(field_histo.len());
        let width = end_index - start_index;
        let ratio = (width as f64) / (field_histo.len() as f64);
        let row_count = (ratio * (num_all_rows as f64)) as usize;
        let row_bytes = row_bytes(schema, &projection, &self.server, &stat);
        Some(CostResult {
            row_count,
            row_bytes,
        })
    }
}
