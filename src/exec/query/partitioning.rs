use dovahkiin::{
    expr::serde::Expr,
    types::Id,
};

use crate::exec::partitioner::{hash, Partitioner};

use super::env::Environment;

pub trait Partitioning {
    fn get_partitioner(
        &self,
        expr: &Expr,
        env: &mut Environment,
    ) -> Result<Option<Box<dyn Partitioner>>, String>;

    fn get_partition(
        &self,
        data_ptr: *mut (),
        env: &mut Environment,
        partitioner: &Box<dyn Partitioner>,
    ) -> Option<u64>;
}

pub fn get_hash_partitioner(env: &mut Environment) -> Result<Option<Box<dyn Partitioner>>, String> {
    let partitioner = hash::init(env.get_chash());
    let boxed: Box<dyn Partitioner> = Box::new(partitioner);
    return Ok(Some(boxed));
}

pub fn get_id_partition(data_ptr: *mut (), partitioner: &Box<dyn Partitioner>) -> Option<u64> {
    let id = unsafe { &*(data_ptr as *mut Id) };
    let key = id.higher;
    partitioner.partition(key)
}