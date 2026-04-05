use super::external::ExtNode;
use super::internal::InNode;
use super::node::{write_node, Node, NodeWriteGuard};
use super::*;
use super::{max_entry_key, BPlusTree, NodeCellRef};
use crate::client::AsyncClient;
use crate::ram::cell::OwnedCell;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::cell::RefCell;
use std::collections::HashSet;
use std::fmt::Debug;
use std::mem;
use std::rc::Rc;

const PARALLEL_FETCH_THRESHOLD: usize = 16;
const PARALLEL_FETCH_CONCURRENCY: usize = 8;

#[derive(Debug, Clone)]
pub enum ReconstructError {
    MissingPage { page_id: Id, pages_read: usize },
    RpcReadFailed { page_id: Id, error: String },
}

pub struct TreeConstructor<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    level_guards: Vec<Rc<RefCell<NodeWriteGuard<KS, PS>>>>,
}

impl<KS, PS> TreeConstructor<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub fn new() -> Self {
        TreeConstructor {
            level_guards: vec![],
        }
    }

    pub fn push_extnode(&mut self, node: &NodeCellRef, first_key: EntryKey) {
        self.push(0, node, None, first_key);
    }

    fn push(
        &mut self,
        level: usize,
        node: &NodeCellRef,
        left_node: Option<&mut NodeWriteGuard<KS, PS>>,
        left_bound: EntryKey,
    ) {
        let mut new_tree = false;
        trace!("Push node at {}", level);
        if self.level_guards.len() < level + 1 {
            debug!("Creating new level {}", level);
            let mut new_root_innode = InNode::<KS, PS>::new(0, max_entry_key());
            if level > 0 {
                let left_node = left_node.unwrap();
                new_root_innode.ptrs.as_slice()[0] = left_node.node_ref().clone();
            } else {
                new_tree = true;
            }
            let new_root_ref = NodeCellRef::new(Node::with_internal(new_root_innode));
            self.level_guards
                .push(Rc::new(RefCell::new(write_node::<KS, PS>(&new_root_ref))));
        }
        let parent_page_ref = self.level_guards[level].clone();
        let mut node_guard = parent_page_ref.borrow_mut();
        let cap = KS::slice_len();
        if node_guard.len() >= cap {
            // current page overflowed, need a new page
            debug!("Creating new node at level {}", level);
            let mut new_innode = InNode::<KS, PS>::new(1, max_entry_key());
            let parent_right_bound = node_guard.last_key().clone();
            let new_innode_head_ptr = {
                // take a key and a ptr from current page to new page
                // reset current page right bound to the taken key
                // return the taken ptr
                let node_innode = node_guard.innode_mut();
                node_innode.len -= 1;
                node_innode.right_bound = parent_right_bound.clone();
                mem::replace(
                    &mut node_innode.ptrs.as_slice()[cap],
                    NodeCellRef::default(),
                )
            };
            // arrange a valid new page by putting the ptr from current page at 1st
            new_innode.ptrs.as_slice()[0] = new_innode_head_ptr;
            new_innode.ptrs.as_slice()[1] = node.clone();
            new_innode.keys = InternalKeys::from_keys(&[left_bound]);
            let new_node = NodeCellRef::new(Node::with_internal(new_innode));
            self.push(
                level + 1,
                &new_node,
                Some(&mut *node_guard),
                parent_right_bound,
            );
            node_guard.right_ref_mut().map(|rn| *rn = new_node.clone());
            *node_guard = write_node::<KS, PS>(&new_node);
        } else {
            let parent_innode = node_guard.innode_mut();
            let new_len = if new_tree {
                0
            } else {
                let len = parent_innode.len;
                let mut parent_keys = parent_innode.keys.to_vec(len);
                parent_keys.push(left_bound);
                parent_innode.keys = InternalKeys::from_keys(parent_keys.as_slice());
                len + 1
            };
            parent_innode.ptrs.as_slice()[new_len] = node.clone();
            parent_innode.len = new_len;
        }
    }

    pub fn root(&self) -> NodeCellRef {
        debug_assert!(self.levels() > 0, "reconstructed levels is zero");
        debug!("The tree have {} levels", self.levels());
        let last_ref = self.level_guards.last().unwrap().clone();
        let last_guard = last_ref.borrow();
        if last_guard.len() == 0 {
            debug!("Taking root from first ptr of overprovisioned level root");
            last_guard.innode().ptrs.as_slice_immute()[0].clone()
        } else {
            debug!("Taking level root");
            last_guard.node_ref().clone()
        }
    }

    pub fn levels(&self) -> usize {
        self.level_guards.len()
    }
}

pub async fn reconstruct_from_head_id<KS, PS>(
    head_id: Id,
    neb: &AsyncClient,
    deletion: &Arc<DeletionSet>,
    level: usize,
) -> Result<BPlusTree<KS, PS>, ReconstructError>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let resolved_head_id = resolve_chain_head_id::<KS, PS>(head_id, neb).await;
    if resolved_head_id != head_id {
        info!(
            "[B-TREE LOAD] Resolved non-head start {:?} -> real head {:?}",
            head_id, resolved_head_id
        );
    }

    info!(
        "[B-TREE LOAD] Starting reconstruction of level {} tree from head cell {:?}",
        level, resolved_head_id
    );
    let page_ids = discover_page_chain_ids::<KS, PS>(resolved_head_id, neb).await?;
    let page_cells = fetch_page_chain_cells(page_ids, neb).await?;
    let mut len = 0;
    let mut page_count = 0;
    let (root, height) = {
        let mut constructor = TreeConstructor::<KS, PS>::new();
        let mut prev_ref = NodeCellRef::new_none::<KS, PS>();
        for cell in page_cells {
            page_count += 1;
            let page = ExtNode::<KS, PS>::from_cell(&cell);
            let next_id = page.next_id;
            let prev_id = page.prev_id;
            let mut node = page.node;

            debug!(
                "[B-TREE LOAD] Page {} has {} keys, next_id={:?}, prev_id={:?}",
                page_count, node.len, next_id, prev_id
            );

            let at_end = next_id.is_unit_id();
            if !at_end {
                debug!("[B-TREE LOAD] Will read next page: {:?}", next_id);
            } else {
                debug!("[B-TREE LOAD] This is the last page in the chain");
            }
            if at_end {
                node.next = NodeCellRef::new_none::<KS, PS>();
            }
            let mut prev_lock = write_node::<KS, PS>(&prev_ref);
            debug_assert!(prev_ref.is_default() || node.len != 0);
            let first_key = node.keys.as_slice_immute()[0].clone();
            len += node.len;
            node.prev = prev_ref.clone();
            let node_ref = NodeCellRef::new(Node::with_external(Box::new(node)));
            if !prev_lock.is_ref_none() {
                *prev_lock.right_bound_mut() = first_key.clone();
                *prev_lock.right_ref_mut().unwrap() = node_ref.clone();
            } else {
                debug!("Previous page is unit id; this is the first page in the chain (expected)");
            }
            constructor.push_extnode(&node_ref, first_key);
            prev_ref = node_ref;
        }
        (constructor.root(), constructor.levels())
    };
    info!("[B-TREE LOAD] Completed reconstruction of tree {:?} at level {} with {} keys, height {}", resolved_head_id, level, len, height);
    let tree = BPlusTree::from_root(root, resolved_head_id, len, height, deletion);
    debug!(
        "[B-TREE LOAD] Verifying reconstruction at level {}",
        level
    );
    // debug_assert!(verification::tree_has_no_empty_node(&tree));
    debug_assert!(verification::is_tree_in_order(&tree, level));
    debug!(
        "[B-TREE LOAD] Reconstruction verification completed at level {}",
        level
    );
    Ok(tree)
}

async fn discover_page_chain_ids<KS, PS>(head_id: Id, neb: &AsyncClient) -> Result<Vec<Id>, ReconstructError>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut ids = Vec::new();
    let mut current = head_id;
    let mut seen = HashSet::new();

    loop {
        if !seen.insert(current) {
            warn!(
                "[B-TREE LOAD] Detected cycle while discovering page chain from {:?}",
                head_id
            );
            break;
        }

        ids.push(current);
        let cell = match neb.read_cell(current).await {
            Ok(Ok(cell)) => cell,
            Ok(Err(e)) => {
                return Err(ReconstructError::MissingPage {
                    page_id: current,
                    pages_read: ids.len() - 1,
                })
            }
            Err(e) => {
                return Err(ReconstructError::RpcReadFailed {
                    page_id: current,
                    error: format!("{:?}", e),
                })
            }
        };
        let page = ExtNode::<KS, PS>::from_cell(&cell);
        if page.next_id.is_unit_id() {
            break;
        }
        current = page.next_id;
    }

    Ok(ids)
}

async fn fetch_page_chain_cells(
    page_ids: Vec<Id>,
    neb: &AsyncClient,
) -> Result<Vec<OwnedCell>, ReconstructError> {
    if page_ids.len() < PARALLEL_FETCH_THRESHOLD {
        let mut cells = Vec::with_capacity(page_ids.len());
        for (idx, page_id) in page_ids.into_iter().enumerate() {
            let cell = match neb.read_cell(page_id).await {
                Ok(Ok(cell)) => cell,
                Ok(Err(_)) => {
                    return Err(ReconstructError::MissingPage {
                        page_id,
                        pages_read: idx,
                    })
                }
                Err(e) => {
                    return Err(ReconstructError::RpcReadFailed {
                        page_id,
                        error: format!("{:?}", e),
                    })
                }
            };
            cells.push(cell);
        }
        return Ok(cells);
    }

    let mut indexed_cells = stream::iter(page_ids.into_iter().enumerate())
        .map(|(idx, page_id)| async move {
            match neb.read_cell(page_id).await {
                Ok(Ok(cell)) => Ok((idx, cell)),
                Ok(Err(_)) => Err(ReconstructError::MissingPage {
                    page_id,
                    pages_read: idx,
                }),
                Err(e) => Err(ReconstructError::RpcReadFailed {
                    page_id,
                    error: format!("{:?}", e),
                }),
            }
        })
        .buffer_unordered(PARALLEL_FETCH_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;

    indexed_cells.sort_by_key(|(idx, _)| *idx);
    Ok(indexed_cells.into_iter().map(|(_, cell)| cell).collect())
}

async fn resolve_chain_head_id<KS, PS>(start_id: Id, neb: &AsyncClient) -> Id
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut current = start_id;
    let mut seen = HashSet::new();

    loop {
        if !seen.insert(current) {
            warn!(
                "[B-TREE LOAD] Detected cycle while resolving head from {:?}; using {:?}",
                start_id, current
            );
            return current;
        }

        let cell = match neb.read_cell(current).await {
            Ok(Ok(cell)) => cell,
            Ok(Err(e)) => {
                warn!(
                    "[B-TREE LOAD] Failed to read candidate head {:?}: {:?}; using original {:?}",
                    current, e, start_id
                );
                return start_id;
            }
            Err(e) => {
                warn!(
                    "[B-TREE LOAD] RPC error reading candidate head {:?}: {:?}; using original {:?}",
                    current, e, start_id
                );
                return start_id;
            }
        };

        let page = ExtNode::<KS, PS>::from_cell(&cell);
        if page.prev_id.is_unit_id() {
            return current;
        }

        current = page.prev_id;
    }
}

unsafe impl<KS, PS> Send for TreeConstructor<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
}

#[cfg(test)]
mod test {
    use super::external::*;
    use crate::index::ranged::tree::btree::test::*;
    use crate::index::ranged::tree::btree::*;
    use crate::ram::types::*;
    use crate::rand::Rng;
    use crate::server::*;
    use crate::{client, ram::cell::OwnedCell};
    use dovahkiin::types::custom_types::id::Id;
    use itertools::Itertools;
    use lightning::map::HashSet;
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread")]
    async fn tree_reconstruct_from_head() {
        let _ = env_logger::try_init();
        let server_group = "btree-reconstruct";
        let server_addr = String::from("127.0.0.1:5600");
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_size: 16 * 12024 * 1024,
                db_size: 16 * 12024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None, // No persistence for regular tests
                index_enabled: false,
                services: vec![Service::Cell],
                enable_recovery: false,
            },
            &server_addr,
            &server_group,
            async |_| {},
        )
        .await;
        let client = Arc::new(
            client::AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr],
                server_group,
            )
            .await
            .unwrap(),
        );
        client
            .new_schema_with_id(page_schema())
            .await
            .unwrap()
            .unwrap();
        let mut last_id = Id::unit_id();
        let cell_limit = 1000;
        let mut counter = 0;
        let mut all_keys = vec![];
        for i in 1..=cell_limit {
            let new_id = Id::new(i, i);
            let mut value = OwnedValue::Map(OwnedMap::new());
            value[*PREV_PAGE_KEY_HASH] = OwnedValue::Id(last_id);
            value[*NEXT_PAGE_KEY_HASH] = if i < cell_limit {
                OwnedValue::Id(Id::new(i + 1, i + 1))
            } else {
                OwnedValue::Id(Id::unit_id())
            };
            value[*KEYS_KEY_HASH] = (0..PAGE_SIZE)
                .map(|_| {
                    counter += 1;
                    let mut id = new_id;
                    id.lower = counter;
                    let key = EntryKey::from_id(&id);
                    all_keys.push(key.clone());
                    SmallBytes::from_vec(key.as_slice().to_vec())
                })
                .collect_vec()
                .value();
            client
                .write_cell(OwnedCell::new_with_id(*PAGE_SCHEMA_ID, &new_id, value))
                .await
                .unwrap()
                .unwrap();
            last_id = new_id;
        }
        let deletion = Arc::new(HashSet::with_capacity(8));
        let tree = Arc::new(
            LevelBPlusTree::from_head_id(&Id::new(1, 1), &client, &deletion, 0)
                .await
                .expect("reconstruct from head id should succeed"),
        );
        let threads = all_keys
            .clone()
            .into_iter()
            .enumerate()
            .map(|(i, key)| {
                let tree = tree.clone();
                let all_keys = all_keys.clone();
                std::thread::spawn(move || {
                    trace!("Checking {:?}", key);
                    let mut cursor = tree.seek(&key, Ordering::Forward);
                    assert_eq!(cursor.current().unwrap(), &key);
                    let mut rng = rand::thread_rng();
                    if i > all_keys.len() / 2 && rng.gen_range(0..50) == 1 {
                        for j in i..all_keys.len() {
                            assert_eq!(cursor.current().unwrap(), &all_keys[j]);
                            cursor.next();
                        }
                        assert!(cursor.current().is_none());
                    }
                })
            })
            .collect_vec();
        for t in threads {
            t.join().unwrap();
        }
    }
}
