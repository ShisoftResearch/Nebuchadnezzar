use super::lsm::btree::Ordering;
use super::lsm::service::AsyncServiceClient;
use super::lsm::service::*;
use super::sm::client::SMClient;
use crate::index::EntryKey;
use crate::ram::types::Id;
use bifrost::conshash::ConsistentHashing;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

pub mod cursor;

pub struct RangedQueryClient {
    conshash: Arc<ConsistentHashing>,
    sm: Arc<SMClient>,
    placement: RwLock<BTreeMap<EntryKey, (Id, EntryKey)>>,
}

impl RangedQueryClient {
    pub async fn seek<'a>(
        &'a self,
        key: &EntryKey,
        ordering: Ordering,
    ) -> Option<cursor::ClientCursor<'a>> {
        let mut ensure_updated = false;
        let mut retried = 0;
        loop {
            if retried >= 10 {
                // Retry attempts all failed
                return None;
            }
            let (tree_id, tree_client, lower, upper) =
                self.locate_key_server(key, ensure_updated).await;
            match tree_client
                .seek(tree_id, key.clone(), ordering, 128)
                .await
                .unwrap()
            {
                OpResult::Successful((cursor, init_entry)) => {
                    let tree_boundary = match ordering {
                        Ordering::Forward => upper,
                        Ordering::Backward => lower,
                    };
                    if let Some(init_entry) = init_entry {
                        return Some(cursor::ClientCursor::new(
                            cursor,
                            init_entry,
                            ordering,
                            tree_boundary,
                            tree_client,
                            &self,
                        ));
                    } else {
                        return None;
                    }
                }
                OpResult::Migrating => {
                    tokio::time::delay_for(Duration::from_millis(500)).await;
                }
                OpResult::OutOfBound | OpResult::NotFound => {
                    ensure_updated = true;
                }
                OpResult::Failed => unreachable!(),
            }
            retried += 1;
        }
    }
    async fn locate_key_server(
        &self,
        key: &EntryKey,
        ensure_updated: bool,
    ) -> (Id, Arc<AsyncServiceClient>, EntryKey, EntryKey) {
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
        let tree_client = locate_tree_server_from_conshash(&tree_id, &self.conshash)
            .await
            .unwrap();
        (tree_id, tree_client, lower, upper)
    }
}
