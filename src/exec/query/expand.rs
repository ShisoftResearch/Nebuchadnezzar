use dovahkiin::expr::serde::Expr;

pub trait Macro {
    fn expand(expr: Expr) -> Expr;
}