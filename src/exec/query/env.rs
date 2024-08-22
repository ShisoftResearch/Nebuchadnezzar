use std::collections::VecDeque;

use bifrost::conshash::ConsistentHashing;
use bifrost_plugins::hash_ident;
use dovahkiin::{
    ahash::{HashMap, HashMapExt},
    expr::{interpreter::Interpreter, serde::Expr},
};
use lightning::aarc::Arc;

const PARAMS_SYM_ID: u64 = hash_ident!(__PARAMS_SYM_ID);

pub struct Environment<'a> {
    binding: VecDeque<HashMap<u64, Expr>>,
    interpreter: Interpreter<'a>,
    chash: Arc<ConsistentHashing>,
}

impl<'a> Environment<'a> {
    pub fn new(chash: Arc<ConsistentHashing>) -> Self {
        let mut binding = VecDeque::new();
        let interpreter = Interpreter::new();
        binding.push_front(HashMap::new());
        return Self {
            binding,
            chash,
            interpreter,
        };
    }
    pub fn set_params(&mut self, expr: &Expr) {
        self.set_binding(PARAMS_SYM_ID, expr.clone())
    }
    pub fn get_params(&self) -> &Expr {
        self.get_binding(&PARAMS_SYM_ID).unwrap()
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
