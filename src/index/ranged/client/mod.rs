use super::{lsm::btree::Ordering, sm::TreePlacement};
use super::lsm::service::*;
use super::sm::client::SMClient;
use crate::client::AsyncClient;
use crate::index::EntryKey;
use crate::ram::types::Id;
use bifrost::{conshash::ConsistentHashing, raft::state_machine::master::ExecError};
use bifrost::raft::client::RaftClient;
use bifrost::rpc::RPCError;
use futures::future::BoxFuture;
use futures::prelude::*;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

pub mod cursor;

pub struct RangedQueryClient {
    conshash: Arc<ConsistentHashing>,
    sm: Arc<SMClient>,
    placement: RwLock<BTreeMap<EntryKey, (TreePlacement, EntryKey)>>,
    neb_client: Arc<AsyncClient>,
}

impl RangedQueryClient {
    pub async fn new(
        conshash: &Arc<ConsistentHashing>,
        raft_client: &Arc<RaftClient>,
        neb_client: &Arc<AsyncClient>,
    ) -> Self {
        let sm = SMClient::new(crate::index::ranged::sm::DEFAULT_SM_ID, raft_client);
        Self {
            conshash: conshash.clone(),
            sm: Arc::new(sm),
            neb_client: neb_client.clone(),
            placement: RwLock::new(BTreeMap::new()),
        }
    }

    pub async fn seek(
        self_ref: &Arc<Self>,
        key: &EntryKey,
        ordering: Ordering,
        buffer_size: u16
    ) -> Result<Option<cursor::ClientCursor>, RPCError> {
        self_ref
            .run_on_destinated_tree(
                key,
                |key, client, tree_id, epoch| {
                    async move { client.seek(tree_id, key, ordering, buffer_size, epoch).await }.boxed()
                },
                |action_res, _tree_client, lower, upper| {
                    async move {
                        if let Some(block) = action_res {
                            if block.buffer.is_empty() {
                                return Ok(Some(None))
                            } else {
                                let client_cursor = cursor::ClientCursor::new(
                                    ordering, 
                                    block, 
                                    lower,
                                    self_ref.clone(),
                                    buffer_size,
                                ).await?;
                                return Ok(Some(Some(client_cursor)));
                            }
                        } else {
                            unreachable!()
                        }
                    }
                    .boxed()
                },
            )
            .await
    }

    pub async fn delete(&self, key: &EntryKey) -> Result<bool, RPCError> {
        self.run_on_destinated_tree(
            key,
            |key, client, tree_id, epoch| async move { client.delete(tree_id, key.clone(), epoch).await }.boxed(),
            |action_res, _, _, _| future::ready(Ok(action_res)).boxed(),
        )
        .await
    }

    pub async fn insert(&self, key: &EntryKey) -> Result<bool, RPCError> {
        self.run_on_destinated_tree(
            key,
            |key, client, tree_id, epoch| async move { client.insert(tree_id, key.clone(), epoch).await }.boxed(),
            |action_res, _, _, _| future::ready(Ok(action_res)).boxed(),
        )
        .await
    }

    pub async fn tree_stats(&self) -> Result<Vec<LSMTreeStat>, RPCError> {
        let mut res = vec![];
        for tree_placement in self.placement.read().values().map(|(id, _)| id) {
            let tree_id = tree_placement.id;
            let tree_client = locate_tree_server_from_conshash(&tree_id, &self.conshash).await?;
            match tree_client.stat(tree_id).await? {
                OpResult::Successful(stat_res) => {
                    res.push(stat_res);
                }
                _ => unreachable!(),
            }
        }
        Ok(res)
    }

    #[inline(always)]
    async fn run_on_destinated_tree<'a, AR, PR, A, P>(
        &'a self,
        key: &EntryKey,
        action: A,
        proc: P,
    ) -> Result<PR, RPCError>
    where
        A: Fn(
            EntryKey,
            Arc<AsyncServiceClient>,
            Id,
            u64
        ) -> BoxFuture<'a, Result<OpResult<AR>, RPCError>>,
        P: Fn(
            Option<AR>,
            Arc<AsyncServiceClient>,
            EntryKey,
            EntryKey,
        ) -> BoxFuture<'a, Result<Option<PR>, RPCError>>,
    {
        let mut ensure_updated = false;
        let mut retried: i32 = 0;
        loop {
            if retried >= 300 {
                // Retry attempts all failed
                return Result::Err(RPCError::IOError(io::Error::new(
                    io::ErrorKind::Other,
                    "Too many retry",
                )));
            }
            let (placement, tree_client, lower, upper) =
                self.locate_key_server(&key, ensure_updated).await?;
            match action(key.clone(), tree_client.clone(), placement.id, placement.epoch).await? {
                OpResult::Successful(res) => {
                    if let Some(proc_res) = proc(Some(res), tree_client, lower, upper).await? {
                        return Ok(proc_res);
                    }
                }
                OpResult::Migrating => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                OpResult::OutOfBound | OpResult::NotFound => {
                    ensure_updated = true;
                }
                OpResult::EpochMissMatch(expect, actual) => {
                    debug!("Epoch mismatch, expect {}, actual {}", expect, actual);
                    ensure_updated = true;
                }
            }
            retried += 1;
        }
    }

    async fn locate_key_server(
        &self,
        key: &EntryKey,
        ensure_updated: bool,
    ) -> Result<(TreePlacement, Arc<AsyncServiceClient>, EntryKey, EntryKey), RPCError> {
        let mut tree_prop = None;
        if !ensure_updated {
            if let Some((lower, (id, upper))) = self.placement.read().range(..key.clone()).last() {
                if key >= lower && key < upper {
                    tree_prop = Some((lower.clone(), *id, upper.clone()));
                }
            }
        }
        if tree_prop.is_none() {
            tree_prop = Some(self.refresh_key_mapping(key).await.expect("Cannot locate key"));
        }
        let (lower, tree_placement, upper) = tree_prop.unwrap();
        let tree_client = locate_tree_server_from_conshash(&tree_placement.id, &self.conshash).await?;
        Ok((tree_placement, tree_client, lower, upper))
    }
    
    async fn refresh_key_mapping(&self, key: &EntryKey) -> Result<(EntryKey, TreePlacement, EntryKey), ExecError> {
        let (lower, placement, upper) = self.sm.locate_key(key).await?;
        debug_assert!(key >= &lower && key < &upper, "Key {:?}, lower {:?}, upper {:?}", key, lower, upper);
        self.placement
            .write()
            .insert(lower.clone(), (placement, upper.clone()));
        return Ok((lower, placement, upper));
    }
}
