use crate::{
    exec::{
        partitioner::Partitioner,
        query::{
            env::Environment,
            planner::{get_cell_partitioner, get_id_partition, Partitioning},
        },
        symbols,
    },
    ram::{cell::SharedCell, chunk::Chunks},
};
use dovahkiin::{expr::serde::Expr, types::Id};

use super::Adapter;

pub struct IdCell<'a> {
    chunks: &'a Chunks,
    iter: Box<dyn Iterator<Item = Id>>,
}

pub struct IdCellParams<'a> {
    chunks: &'a Chunks,
}

impl<'a> Adapter<Id, SharedCell<'a>, IdCellParams<'a>> for IdCell<'a> {
    fn from(
        input: impl Iterator<Item = Id> + 'static,
        params: IdCellParams<'a>,
    ) -> Result<Self, String> {
        let iter: Box<dyn Iterator<Item = Id>> = Box::new(input);
        let r = Self {
            iter,
            chunks: params.chunks,
        };
        return Ok(r);
    }
}

impl<'a> Iterator for IdCell<'a> {
    type Item = SharedCell<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(id) = self.iter.next() {
                match self.chunks.read_cell(&id) {
                    Ok(res) => return Some(res),
                    Err(e) => error!("Error on reading cell with id {:?}, error {:?}", id, e),
                }
            } else {
                return None;
            }
        }
    }
}

impl Partitioning for symbols::objs::IdCell {
    fn get_partitioner(
        &self,
        _expr: &Expr,
        env: &mut Environment,
    ) -> Result<Option<Box<dyn Partitioner>>, String> {
        get_cell_partitioner(env)
    }

    fn get_partition(
        &self,
        data_ptr: *mut (),
        _env: &mut Environment,
        partitioner: &Box<dyn Partitioner>,
    ) -> Option<u64> {
        get_id_partition(data_ptr, partitioner)
    }
}
