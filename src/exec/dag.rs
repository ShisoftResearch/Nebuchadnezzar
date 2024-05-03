use std::collections::HashMap;

use dovahkiin::expr::serde::Expr;
use serde::{Deserialize, Serialize};

use super::symbols::Symbol;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    id: u32,
    symbol: Symbol,
    params: Expr
}

#[derive(Serialize, Deserialize)]
pub struct DAG {
    nodes: Vec<Node>,
    outlinks: HashMap<u32, Vec<u32>>,
    inlinks: HashMap<u32, Vec<u32>>
}

impl DAG {
    pub fn new() -> Self {
        Self {
            nodes: vec![],
            outlinks: HashMap::new(),
            inlinks: HashMap::new(),
        }
    }

    pub fn push_node(&mut self, symbol: Symbol, params: Expr) -> Node {
        let id = self.nodes.len() as u32;
        let node = Node {
            id, symbol, params
        };
        self.nodes.push(node.clone());
        return node;
    }

    pub fn link(&mut self, from: u32, to: u32) {
        self.outlinks.entry(from).or_insert_with(|| vec![]).push(to);
        self.inlinks.entry(to).or_insert_with(|| vec![]).push(from);
    }

    pub fn outlinks(&self, id: u32) -> &Vec<u32> {
        &self.outlinks[&id]
    }

    pub fn inlinks(&self, id: u32) -> &Vec<u32> {
        &self.inlinks[&id]
    }
}