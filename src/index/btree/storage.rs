use crate::client;
use crate::index::btree::{external};
use itertools::Itertools;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use crate::server::NebServer;
use std::sync::Arc;
use tokio::prelude::*;

pub async fn store_changed_nodes(neb: &Arc<NebServer>) {
    external::flush_changed()
        .into_iter()
        .group_by(move |(id, _)| neb.get_server_id_by_id(id).unwrap())
        .into_iter()
        .map(|(sid, group)| (sid, group.map(|(id, node)| (id, node)).collect_vec()))
        .map(move |(sid, group)| {
            tokio::spawn(async move {
                if let Ok(rpc_client) = neb.get_member_by_server_id_async(sid).await {
                    let neb = client::client_by_rpc_client(&rpc_client).await;
                    group.into_iter().map(move |(id, node)| {
                        tokio::spawn(async move {
                            if let Some(changing) = node {
                                let deletion = changing.deletion.read();
                                changing.node.persist(&*deletion, &*neb).await
                            } else {
                                neb.remove_cell(id).await
                            }
                        })
                    }).collect::<FuturesUnordered<_>>().await;
                }
            })
        }).collect::<FuturesUnordered<_>>().await;
}
