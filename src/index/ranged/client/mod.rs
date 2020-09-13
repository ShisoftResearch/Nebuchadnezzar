use super::lsm::btree::Ordering;
use super::lsm::service::*;
use super::sm::client::SMClient;
use crate::index::EntryKey;
use crate::ram::types::Id;
use bifrost::conshash::ConsistentHashing;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use futures::future::BoxFuture;
use futures::prelude::*;
use std::io;
use bifrost::rpc::RPCError;

pub mod cursor;

pub struct RangedQueryClient {
    conshash: Arc<ConsistentHashing>,
    sm: Arc<SMClient>,
    placement: RwLock<BTreeMap<EntryKey, (Id, EntryKey)>>,
}

impl RangedQueryClient {
    pub async fn seek<'a>(
        &'a self,
        key: &'a EntryKey,
        ordering: Ordering,
    ) -> Result<Option<cursor::ClientCursor<'a>>, RPCError> {
        self.run_on_destinated_tree(
            key,
            |key, client, tree_id| {
                async move {
                    client.seek(tree_id, key.clone(), ordering, 160).await
                }.boxed()
            },
            |action_res, tree_client, lower,  upper| {
                async move {
                    if let Some((cursor, init_entry)) = action_res {
                        let tree_boundary = match ordering {
                            Ordering::Forward => upper,
                            Ordering::Backward => lower,
                        };
                        if let Some(init_entry) = init_entry {
                            return Some(Some(cursor::ClientCursor::new(
                                cursor,
                                init_entry,
                                ordering,
                                tree_boundary,
                                tree_client,
                                &self,
                            )));
                        } else {
                            return Some(None);
                        }
                    } else {
                        unreachable!()
                    }
                }.boxed()
            }
        ).await
    }

    pub async fn delete(&self, key: &EntryKey) -> Result<bool, RPCError> {
        self.run_on_destinated_tree(
            key,
            |key, client, tree_id| {
                async move {
                    client.delete(tree_id, key.clone()).await
                }.boxed()
            },
            |action_res, _, _, _| future::ready(action_res).boxed()
        ).await
    }

    async fn run_on_destinated_tree<'a, AR, PR, A, P>(&'a self, key: &'a EntryKey, action: A, proc: P) -> Result<PR, RPCError>
        where A: Fn(&'a EntryKey,  Arc<AsyncServiceClient>, Id) -> BoxFuture<'a, Result<OpResult<AR>, RPCError>>,
              P: Fn(Option<AR>, Arc<AsyncServiceClient>, EntryKey, EntryKey) -> BoxFuture<'a, Option<PR>>
    {
        let mut ensure_updated = false;
        let mut retried: i32 = 0;
        loop {
            if retried >= 10 {
                // Retry attempts all failed
                return Result::Err(RPCError::IOError(io::Error::new(io::ErrorKind::Other, "Too many retry")));
            }
            let (tree_id, tree_client, lower, upper) =
                self.locate_key_server(key, ensure_updated).await?;
            match action(key, tree_client.clone(), tree_id).await?
            {
                OpResult::Successful(res) => {
                    if let Some(proc_res) = proc(Some(res), tree_client, lower, upper).await {
                        return Ok(proc_res);
                    } 
                }
                OpResult::Migrating => {
                    tokio::time::delay_for(Duration::from_millis(500)).await;
                }
                OpResult::OutOfBound | OpResult::NotFound => {
                    ensure_updated = true;
                },
            }
            retried += 1;
        }
    }

    async fn locate_key_server(
        &self,
        key: &EntryKey,
        ensure_updated: bool,
    ) -> Result<(Id, Arc<AsyncServiceClient>, EntryKey, EntryKey), RPCError> {
        let mut tree_prop = None;
        if !ensure_updated {
            if let Some((lower, (id, upper))) = self.placement.read().range(..key.clone()).last() {
                if key >= lower && key < upper {
                    tree_prop = Some((*id, lower.clone(), upper.clone()));
                }
            }
        }
        if tree_prop.is_none() {
            let (lower, id, upper) = self.sm.locate_key(key).await.unwrap();
            debug_assert!(key >= &lower && key < &upper);
            self.placement
                .write()
                .insert(lower.clone(), (id, upper.clone()));
            tree_prop = Some((id, lower, upper));
        }
        let (tree_id, lower, upper) = tree_prop.unwrap();
        let tree_client = locate_tree_server_from_conshash(&tree_id, &self.conshash).await?;
        Ok((tree_id, tree_client, lower, upper))
    }
}
