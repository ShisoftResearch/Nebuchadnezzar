use client;
use client::AsyncClient;
use futures::future::join_all;
use futures::Future;
use index::btree::external::ExtNode;
use index::btree::{external, BPlusTree};
use itertools::Itertools;
use ram::cell::Cell;
use rayon::prelude::*;
use server::NebServer;
use std::sync::Arc;

pub fn store_changed_nodes(neb: Arc<NebServer>) -> impl Future<Item = (), Error = ()> {
    let neb_2 = neb.clone();
    let futs =
        external::flush_changed()
            .into_iter()
            .group_by(move |(id, _)| neb.get_server_id_by_id(id).unwrap())
            .into_iter()
            .map(|(sid, group)| (sid, group.map(|(id, node)| (id, node.clone())).collect_vec()))
            .map(move |(sid, group)| {
                let rpc_client = neb_2.get_member_by_server_id_async(sid);
                rpc_client.map_err(move |_| ()).and_then(|c| {
                    let neb = client::client_by_rpc_client(&c);
                    join_all(group.into_iter().map(move |(id, node)| {
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
    join_all(futs).map(|_| ())
}
