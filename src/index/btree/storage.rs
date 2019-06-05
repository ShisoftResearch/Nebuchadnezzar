use client::AsyncClient;
use futures::Future;
use index::btree::external::ExtNode;
use index::btree::{external, BPlusTree};
use ram::cell::Cell;
use rayon::prelude::*;
use itertools::Itertools;
use server::NebServer;
use std::sync::Arc;
use futures::future::join_all;
use client;

pub fn store_changed_nodes(neb: &Arc<NebServer>) {
    let nodes = external::flush_changed();
    let groups = nodes
        .into_iter()
        .group_by(|(id, _)| neb.get_server_id_by_id(id).unwrap());
    let futs = groups
        .into_iter()
        .map(|(sid, group)| {
            let rpc_client = neb.get_member_by_server_id_async(sid);
            rpc_client
                .map_err(|_| ())
                .and_then(|c| {
                    let neb = client::client_by_rpc_client(&c);
                    join_all(group.map(move |(id, node)| {
                        if let Some(changing) = node {
                            let deletion = changing.deletion.read();
                            changing.node.persist(&*deletion, &*neb)
                        } else {
                            box neb.remove_cell(id).map(|_| ()).map_err(|_| ())
                        }
                    }))
                })
        })
        .collect_vec();
    join_all(futs).wait().unwrap();
}
