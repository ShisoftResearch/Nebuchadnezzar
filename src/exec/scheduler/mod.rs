use dovahkiin::expr::serde::Expr;

pub mod coordinator;
pub mod worker;

pub struct Stage {
    seq: Vec<Expr>,
}
