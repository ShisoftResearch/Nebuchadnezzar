use std::iter::Sum;

use dovahkiin::expr::serde::Expr;

use super::data_client::ValueRange;

pub mod range_index;
pub mod scan;

pub struct CostResult {
    compute: usize,
    row_count: usize,
    total_bytes: usize,
}

pub struct DistHostCostResult {
    costs: Vec<CostResult>,
}

trait CostFunction {
    fn cost<'a>(
        &self,
        schema: u32,
        field: Option<u64>,
        range: Option<ValueRange<'a>>,
        projection: Vec<u64>,
        row_cost: u64,
    ) -> CostResult;
}

impl DistHostCostResult {
    fn total_cost(&self) -> CostResult {
        let mut total_compute = 0;
        let mut total_rows = 0;
        let mut total_bytes = 0;
        self.costs.iter().for_each(|x| {
            total_compute += x.compute;
            total_rows += x.row_count;
            total_bytes += x.total_bytes;
        });
        CostResult {
            compute: total_compute,
            row_count: total_rows,
            total_bytes,
        }
    }
}

impl Default for CostResult {
    fn default() -> Self {
        Self {
            compute: Default::default(),
            row_count: Default::default(),
            total_bytes: Default::default(),
        }
    }
}
