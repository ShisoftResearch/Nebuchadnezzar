use client::AsyncClient;
use futures::Future;
use index::btree::external::ExtNode;
use index::btree::{external, BPlusTree};
use ram::cell::Cell;
use rayon::prelude::*;

pub fn store_changed_nodes(neb: &AsyncClient) {
    let nodes = external::flush_changed();
    nodes.into_par_iter().for_each(|(id, node)| {
        if let Some(changing) = node {
            let deletion = changing.deletion.read();
            changing.node.persist(&*deletion, neb);
        } else {
            neb.remove_cell(id).wait().unwrap().unwrap();
        }
    })
}
