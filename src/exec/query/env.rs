use std::collections::VecDeque;

use dovahkiin::{ahash::{HashMap, HashMapExt}, expr::serde::Expr};

#[derive(Debug, Serialize, Deserialize)]
pub struct Environment {
    binding: VecDeque<HashMap<u64, Expr>>
}

impl Environment {
    pub fn new() -> Self {
        let mut binding = VecDeque::new();
        binding.push_front(HashMap::new());
        return Self {
            binding
        }
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
}