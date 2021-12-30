use dovahkiin::expr::serde::Expr;

use super::data_client::ValueRange;

pub mod scan;
pub mod range_index;

pub struct HostCostResult {
    compute: usize,
    row_count: usize,
    total_bytes: usize
}

pub struct DistHostCostResult {
    costs: Vec<HostCostResult>,
}

trait CostFunction {
    fn cost<'a>(
        &self, 
        schema: u32, 
        field: Option<u64>, 
        range: Option<ValueRange<'a>>, 
        projection: Vec<u64>,
        selection: Expr,
    ) -> HostCostResult;
    fn distributed_cost<'a>(
        &self, 
        schema: u32, 
        field: Option<u64>, 
        range: Option<ValueRange<'a>>, 
        projection: Vec<u64>,
        selection: Expr,
    ) -> DistHostCostResult;
}