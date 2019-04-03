use client::AsyncClient;
use futures::Future;
use index::btree::{external, BPlusTree};
use index::btree::external::ExtNode;
use rayon::prelude::*;
use ram::cell::Cell;

pub fn store_changed_nodes(neb: &AsyncClient) {
    let nodes = external::flush_changed();
    nodes.into_par_iter().for_each(|(id, node)| {
        if let Some(node) = node {
            node.persist(neb);
        } else {
            neb.remove_cell(id).wait().unwrap().unwrap();
        }
    })
}
