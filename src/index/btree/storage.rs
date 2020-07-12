use crate::client;
use crate::index::btree::{external};
use itertools::Itertools;
use futures::prelude::*;
use crate::server::NebServer;
use std::sync::Arc;
use tokio::prelude::*;

pub fn store_changed_nodes(neb: &Arc<NebServer>) {
    external::flush_changed()
        .into_iter()
        .group_by(move |(id, _)| neb.get_server_id_by_id(id).unwrap())
        .into_iter()
        .map(|(sid, group)| (sid, group.map(|(id, node)| (id, node)).collect_vec()))
        .for_each(move |(sid, group)| {
            tokio::spawn(async move {
                if let Ok(rpc_client) = neb.get_member_by_server_id_async(sid).await {
                    let neb = client::client_by_rpc_client(&rpc_client);
                    group.into_iter().for_each(move |(id, node)| {
                        tokio::spawn(async move {
                            if let Some(changing) = node {
                                changing.node.persist(&changing.deletion, &neb).await;
                            } else {
                                neb.remove_cell(id).await;
                            }
                        });
                    });
                }
            });
        });
}
