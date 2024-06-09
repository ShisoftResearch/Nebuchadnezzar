use std::{collections::VecDeque, sync::Arc};

use bifrost::conshash::ConsistentHashing;
use dovahkiin::{
    ahash::{HashMap, HashMapExt},
    expr::serde::Expr,
};

pub struct Environment {
    binding: VecDeque<HashMap<u64, Expr>>,
    chash: Arc<ConsistentHashing>,
}

impl Environment {
    pub fn new(chash: Arc<ConsistentHashing>) -> Self {
        let mut binding = VecDeque::new();
        binding.push_front(HashMap::new());
        return Self { binding, chash };
    }
    pub fn set_binding(&mut self, sym_id: u64, expr: Expr) {
        self.binding.front_mut().unwrap().insert(sym_id, expr);
    }
    pub fn get_binding(&self, sym_id: &u64) -> Option<&Expr> {
        self.binding.front().unwrap().get(sym_id)
    }
    pub fn push_scope(&mut self) {
        self.binding.push_front(HashMap::new());
    }
    pub fn pop_scope(&mut self) {
        self.binding.pop_front();
    }
    pub fn get_chash(&self) -> Arc<ConsistentHashing> {
        self.chash.clone()
    }
}
