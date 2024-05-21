use dovahkiin::expr::serde::Expr;

use super::symbols::Symbol;

pub mod coordinator;
pub mod worker;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub struct StageId {
    id: u64,
    host: u64
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Stage {
    seq: Vec<Procedure>,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct Procedure{
    symbol: Symbol,
    params: Vec<ProcParam>
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ProcParam {
    Expr(Expr),
    StageOut(StageId)
}
