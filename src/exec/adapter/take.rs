use dovahkiin::{expr::serde::Expr, types::OwnedValue};

use super::Adapter;

pub struct Take<T> {
    current: usize,
    limit: usize,
    iter: Box<dyn Iterator<Item = T>>
}

impl <T> Adapter<T, T, Expr> for Take<T> {
    fn from(input: impl Iterator<Item = T> + 'static, params: Expr) -> Result<Self, String> {
        let limit: usize = match params {
            Expr::Value(OwnedValue::I8(n)) => n as _,
            Expr::Value(OwnedValue::I16(n)) => n as _,
            Expr::Value(OwnedValue::I32(n)) => n as _,
            Expr::Value(OwnedValue::I64(n)) => n as _,
            _ => return Err(format!("Cannot parse limit for take, got {:?}", params))
        };
        let iter: Box<dyn Iterator<Item = T>> = Box::new(input);
        return Ok(Self {
            current: 0,
            iter,
            limit
        })
    }
}

impl <T> Iterator for Take<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.limit {
            return None;
        }
        self.current += 1;
        self.iter.next()
    }
}