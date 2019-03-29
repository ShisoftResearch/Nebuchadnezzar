use client::AsyncClient;
use index::btree::external::ExtNode;
use futures::Future;
use index::btree::external;

pub fn store_changed_nodes(neb: &AsyncClient) {
    let nodes = external::flush_changed();
    nodes.into_iter().for_each(|(id, node)| {
        if let Some(node) = node {
            node.persist(neb);
        } else {
            neb.remove_cell(id).wait().unwrap().unwrap();
        }
    })
}