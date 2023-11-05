use dovahkiin::expr::serde::Expr;

use self::{engines::Device, symbols::Symbol};

// Intermediate presentation and AST for user input
pub mod ir;
// Directed acyclic graph for query plannning
pub mod dag;
// Instruction sequence for execution
pub mod seq;
// Shared symbols
pub mod symbols;

pub mod engines;

struct DataFrame {
    dev: Device,
    rel: Vec<DataFrame>,
    syn: Symbol,
    arg: Expr,
}

impl DataFrame {}

trait Func {}
