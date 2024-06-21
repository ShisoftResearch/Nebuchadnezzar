use std::collections::VecDeque;

use bifrost::conshash::ConsistentHashing;
use dovahkiin::{
    ahash::{HashMap, HashMapExt},
    expr::{interpreter::Interpreter, serde::Expr},
};
use lightning::aarc::Arc;

pub struct Environment<'a> {
    binding: VecDeque<HashMap<u64, Expr>>,
    interpreter: Interpreter<'a>,
    chash: Arc<ConsistentHashing>,
}

impl <'a> Environment<'a> {
    pub fn new(chash: Arc<ConsistentHashing>) -> Self {
        let mut binding = VecDeque::new();
        let interpreter = Interpreter::new();
        binding.push_front(HashMap::new());
        return Self { binding, chash, interpreter };
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
    pub fn get_interpreter(&mut self) -> &mut Interpreter<'a> {
        &mut self.interpreter
    } 
}
