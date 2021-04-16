use dovahkiin::{expr::SExpr, types::OwnedValue};
use serde::{Deserialize, Serialize};
use std::rc::Rc;

use super::*;

#[derive(Serialize, Deserialize, Debug)]
pub enum DAGExpr {
    Symbol(String),
    ISymbol(u64, String),
    Value(OwnedValue),
    List(Vec<u64>),
    Vec(Vec<u64>),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Node {
    id: u64,
    expr: DAGExpr,
}

#[derive(Serialize, Deserialize)]
pub struct DAG {
    nodes: Vec<Rc<Node>>,
}
