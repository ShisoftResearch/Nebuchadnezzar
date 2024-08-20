use dovahkiin::{
    expr::{interpreter::Interpreter, serde::Expr, SExpr},
    types::SharedValue,
};

use crate::exec::{
    partitioner::{self, range, Partitioner},
    query::{
        env::Environment,
        partitioning::{get_hash_partitioner, Partitioning},
    },
    symbols,
};

use super::Sort;

pub type SortKeyValuePair<'a> = (i64, SharedValue<'a>);

pub struct SortBySharedValueASC<'a> {
    exec: Interpreter<'a>,
    expr: SExpr<'a>,
}

pub struct SortBySharedValueDESC<'a> {
    exec: Interpreter<'a>,
    expr: SExpr<'a>,
}

fn extract_shared_value<'a>(
    exec: &mut Interpreter<'a>,
    expr: &SExpr<'a>,
    x: SharedValue<'a>,
) -> Result<i64, String> {
    unsafe {
        exec.unsafe_set_global_val(&x);
    }
    let eval_res = exec.eval(vec![expr.clone()])?;
    exec.unset_global_val();
    eval_res
        .shared_val()
        .and_then(|v| v.get_int())
        .ok_or_else(|| format!("Cannot decode result to compare: {:?}", eval_res))
        .map(|n| n as _)
}

impl<'a> Sort<SharedValue<'a>> for SortBySharedValueASC<'a> {
    fn extract(&mut self, x: SharedValue<'a>) -> Result<i64, String> {
        extract_shared_value(&mut self.exec, &self.expr, x)
    }

    fn compare(x: i64, y: i64) -> i64 {
        x - y
    }
}

impl<'a> Sort<SharedValue<'a>> for SortBySharedValueDESC<'a> {
    fn extract(&mut self, x: SharedValue<'a>) -> Result<i64, String> {
        extract_shared_value(&mut self.exec, &self.expr, x)
    }

    fn compare(x: i64, y: i64) -> i64 {
        y - x
    }
}

// Due to complexity, we are going to use hash partition on higher bits for now
// The information for range partition exists in statistics and histogram but
// making use of them is less flexiable with filtering
pub fn get_sorting_partitioner(
    env: &mut Environment,
) -> Result<Option<Box<dyn Partitioner>>, String> {
    get_hash_partitioner(env)
}

pub fn get_sorting_partition(
    data_ptr: *mut (),
    partitioner: &Box<dyn crate::exec::partitioner::Partitioner>,
) -> Option<u64> {
    let (key, _) = unsafe { &*(data_ptr as *mut SortKeyValuePair) };
    partitioner.partition(*key as u64 >> 4)
}

impl Partitioning for symbols::objs::SortByASC {
    fn get_partitioner(
        &self,
        _expr: &Expr,
        env: &mut Environment,
    ) -> Result<Option<Box<dyn Partitioner>>, String> {
        get_sorting_partitioner(env)
    }

    fn get_partition(
        &self,
        data_ptr: *mut (),
        _env: &mut crate::exec::query::env::Environment,
        partitioner: &Box<dyn crate::exec::partitioner::Partitioner>,
    ) -> Option<u64> {
        get_sorting_partition(data_ptr, partitioner)
    }
}

impl Partitioning for symbols::objs::SortByDESC {
    fn get_partitioner(
        &self,
        _expr: &Expr,
        env: &mut Environment,
    ) -> Result<Option<Box<dyn Partitioner>>, String> {
        get_sorting_partitioner(env)
    }

    fn get_partition(
        &self,
        data_ptr: *mut (),
        _env: &mut crate::exec::query::env::Environment,
        partitioner: &Box<dyn crate::exec::partitioner::Partitioner>,
    ) -> Option<u64> {
        get_sorting_partition(data_ptr, partitioner)
    }
}
