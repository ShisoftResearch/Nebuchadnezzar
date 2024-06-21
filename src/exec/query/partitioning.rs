use crossbeam_epoch::Shared;
use dovahkiin::{expr::serde::Expr, types::{Id, SharedValue}};

use crate::exec::{
    partitioner::{hash, Partitioner},
    symbols::objs::{IdCell, IdCellSel},
};

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

pub fn get_shared_value_partition(data_ptr: *mut (), cond: &Expr, partitioner: &Box<dyn Partitioner>, env: &mut Environment) -> Option<u64> {
    let value = unsafe { &*(data_ptr as *mut SharedValue) };
    env.get_interpreter().set_global_val(value);
    unimplemented!()
}

