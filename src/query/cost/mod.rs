use std::iter::Sum;

use dovahkiin::expr::serde::Expr;

use super::data_client::ValueRange;

pub mod range_index;
pub mod scan;

pub struct CostResult {
    row_count: usize,
    row_bytes: usize,
}

pub struct DistHostCostResult {
    costs: Vec<CostResult>,
}

trait CostFunction {
    fn cost<'a>(
        &self,
        schema: u32,
        field: Option<u64>,
        range: Option<&ValueRange>,
        projection: Vec<u64>,
    ) -> Option<CostResult>;
}

impl DistHostCostResult {
    fn total_cost(&self) -> CostResult {
        let mut total_rows = 0;
        let mut total_bytes = 0;
        self.costs.iter().for_each(|x| {
            total_rows += x.row_count;
            total_bytes += x.row_bytes;
        });
        CostResult {
            row_count: total_rows,
            row_bytes: total_bytes,
        }
    }
}

impl Default for CostResult {
    fn default() -> Self {
        Self {
            row_count: Default::default(),
            row_bytes: Default::default(),
        }
    }
}
