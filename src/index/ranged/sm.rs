use super::tree::service::*;
use super::tree::{service::AsyncServiceClient as LSMServiceClient, tree::INITIAL_TREE_EPOCH};
use super::trees::*;
use crate::ram::types::Id;
use crate::ram::types::RandValue;
use bifrost::conshash::ConsistentHashing;
use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::raft::PlaneId;
use bifrost::raft::RaftService;
use bifrost::rpc::{RPCError, ServiceClient};
use bifrost::utils;
use bifrost_hasher::hash_str;
use bifrost_plugins::hash_ident;
use futures::prelude::*;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::ops::Bound::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const DEFAULT_SM_ID: u64 = hash_ident!("RANGED_INDEX_SM_ID") as u64;
const LOCAL_TREE_ID_SELECTION_MAX_ATTEMPTS: usize = 10_000;

pub fn generate_scoped_sm_id(group_name: &str, database_name: &str) -> u64 {
    if group_name == database_name {
        DEFAULT_SM_ID
    } else {
        hash_str(&format!(
            "RANGED_INDEX_SM_ID-{}-{}",
            group_name, database_name
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreePlacement {
    pub id: Id,
    pub epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeInfo {
    pub lower: EntryKey,
    pub upper: EntryKey,
    pub placement: TreePlacement,
}

pub struct MasterTreeSM {
    tree: BTreeMap<EntryKey, TreePlacement>,
    raft_svr: Arc<RaftService>,
    plane_id: PlaneId,
    conshash: Arc<ConsistentHashing>,
    tree_service_id: u64,
    persistence_path: Option<PathBuf>,
    sm_id: u64,
}

raft_state_machine! {
    def qry locate_key(entry: EntryKey) -> Option<(EntryKey, TreePlacement, EntryKey)>;
    def qry next_tree(tree_lower: EntryKey, ordering: Ordering) -> Option<TreeInfo>;
    def cmd split(src_tree: Id, new_tree: Id, pivot: EntryKey);
    def cmd try_init_genesis(tree_id: Id) -> bool;
    def qry has_placements() -> bool;
    def cmd reset_placements() -> bool;
    // No subscription for clients
}

impl StateMachineCmds for MasterTreeSM {
    /// `None` when no tree covers the key, which means the placement map is
    /// empty -- a member has not yet applied the genesis snapshot.
    ///
    /// This used to `unwrap`, so a routine startup state killed the process:
    /// under parallel load a member can query this before genesis has been
    /// applied locally, and every caller inherited the panic. Callers already
    /// `match` on this call and have a not-ready path; give them something to
    /// match on instead of aborting.
    fn locate_key(
        &self,
        entry: EntryKey,
    ) -> BoxFuture<'_, Option<(EntryKey, TreePlacement, EntryKey)>> {
        let Some((lower, tree)) = self
            .tree
            .range(..=&entry)
            .last()
            .map(|(key, tree)| (key.to_owned(), tree.to_owned()))
        else {
            return future::ready(None).boxed();
        };
        let upper = self
            .tree
            .range((Excluded(entry), Unbounded))
            .next()
            .map(|(key, _)| key)
            .unwrap_or_else(|| &*MAX_ENTRY_KEY)
            .to_owned();
        future::ready(Some((lower, tree, upper))).boxed()
    }

    fn next_tree(
        &self,
        tree_lower: EntryKey,
        ordering: Ordering,
    ) -> BoxFuture<'_, Option<TreeInfo>> {
        debug!(
            "Query next tree for {:?}, ordering {:?}, trees {:?}",
            tree_lower, ordering, self.tree
        );
        future::ready(match ordering {
            Ordering::Forward => {
                let mut iter = self.tree.range((Excluded(&tree_lower), Unbounded));
                if let Some((next_lower, next_place)) = iter.next() {
                    let next_upper = iter
                        .next()
                        .map(|(k, _)| k)
                        .unwrap_or_else(|| &*MAX_ENTRY_KEY);
                    Some(TreeInfo {
                        lower: next_lower.clone(),
                        upper: next_upper.clone(),
                        placement: next_place.clone(),
                    })
                } else {
                    None
                }
            }
            Ordering::Backward => {
                let mut iter = self.tree.range(..&tree_lower).rev();
                if let Some((next_lower, next_place)) = iter.next() {
                    Some(TreeInfo {
                        lower: next_lower.clone(),
                        upper: tree_lower.clone(),
                        placement: next_place.clone(),
                    })
                } else {
                    None
                }
            }
        })
        .boxed()
    }

    fn split(&mut self, src_tree: Id, new_tree: Id, pivot: EntryKey) -> BoxFuture<'_, ()> {
        // Call this after the tree have been split and persisted
        async move {
            let upper_bound = match self.tree.range(&pivot..).next() {
                Some((k, _id)) => k.clone(),
                None => max_entry_key(),
            };
            debug_assert!(pivot < upper_bound);
            debug!(
                "Splitted to new tree {:?}, starts at {:?}, ends at {:?}",
                new_tree, pivot, upper_bound
            );
            self.tree
                .insert(pivot.clone(), TreePlacement::new(new_tree));
            if let Some((_, prev_tree)) = self.tree.range_mut(..&pivot).last() {
                assert_eq!(prev_tree.id, src_tree);
                prev_tree.epoch += 1;
            }

            // Persist the tree state after split
            if let Err(e) = self.persist_tree() {
                error!("Failed to persist tree after split: {:?}", e);
            } else {
                debug!(
                    "Persisted placement split for source {:?}, target {:?}, pivot {:?}; deferred target load to balancer",
                    src_tree,
                    new_tree,
                    pivot
                );
            }
        }
        .boxed()
    }

    fn has_placements(&self) -> BoxFuture<'_, bool> {
        future::ready(!self.tree.is_empty()).boxed()
    }

    /// Forget every placement. Dropping a database deletes its storage and its
    /// trees, but the state machine registered on the meta plane keeps living
    /// in memory -- bifrost has no unregister -- so a database recreated under
    /// the same name inherited placements naming trees that no longer exist.
    /// `has_placements()` then answered true, genesis was skipped, and every
    /// seek retried "tree placement was not found" until it gave up.
    fn reset_placements(&mut self) -> BoxFuture<'_, bool> {
        async move {
            if self.tree.is_empty() {
                return false;
            }
            info!(
                "[RANGED INDEX] Clearing {} stale placement(s) on sm {}",
                self.tree.len(),
                self.sm_id
            );
            self.tree.clear();
            if let Err(e) = self.persist_tree() {
                error!("Failed to persist placement reset: {:?}", e);
            }
            true
        }
        .boxed()
    }

    fn try_init_genesis(&mut self, tree_id: Id) -> BoxFuture<'_, bool> {
        // Genesis placement must be established through consensus: seeding it
        // into a member-local instance before registration leaves each member
        // with a different placement map (split-brain trees). The command is
        // idempotent — the first proposer wins, later proposers see data.
        async move {
            if !self.tree.is_empty() {
                return false;
            }
            info!(
                "[RANGED INDEX] Initializing genesis placement with tree {:?} on sm {} server {}",
                tree_id,
                self.sm_id,
                self.raft_svr.get_server_id()
            );
            self.tree
                .insert(min_entry_key(), TreePlacement::new(tree_id));
            if let Err(e) = self.persist_tree() {
                error!("Failed to persist genesis placement: {:?}", e);
            }
            true
        }
        .boxed()
    }
}

impl StateMachineCtl for MasterTreeSM {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        self.sm_id
    }
    fn snapshot(&self) -> Vec<u8> {
        utils::serde::serialize(&self.tree)
    }
    fn recover(&mut self, data: Vec<u8>) -> BoxFuture<'_, ()> {
        let tree = utils::serde::deserialize(&data).unwrap();
        self.tree = tree;
        // Load the trees these placements name. `try_initialize` does this at
        // startup, but it runs BEFORE registration, so on a runtime reload it
        // saw an empty map and loaded nothing -- the placements then arrived
        // here and named trees no service had, and every seek retried "tree
        // placement was not found" until it gave up.
        async move {
            self.load_all_placements().await;
        }
        .boxed()
    }

    fn recoverable(&self) -> bool {
        // Replicas must converge on the placement map: a member that
        // registers after the genesis command committed has to pull the
        // snapshot, otherwise each member ends up with its own genesis
        // tree and index entries strand per-member.
        true
    }

    fn accept_buffered_replay(&self) -> bool {
        // The placement map recovers from its own persistence file; buffered
        // plane entries replayed on top of that state double-apply (splits
        // bump epochs non-idempotently). Only a genuinely fresh instance —
        // e.g. a member joining an established plane — wants the replay.
        self.tree.is_empty()
    }
}

impl MasterTreeSM {
    pub fn new_with_id_and_persistence_on_plane(
        sm_id: u64,
        tree_service_id: u64,
        raft_svr: &Arc<RaftService>,
        plane_id: PlaneId,
        conshash: &Arc<ConsistentHashing>,
        persistence_path: Option<PathBuf>,
    ) -> Self {
        info!(
            "[RANGED INDEX LOAD] Creating MasterTreeSM with persistence_path={:?}",
            persistence_path
        );
        let mut sm = Self {
            tree: BTreeMap::new(),
            raft_svr: raft_svr.clone(),
            plane_id,
            conshash: conshash.clone(),
            tree_service_id,
            persistence_path: persistence_path.clone(),
            sm_id,
        };

        // Try to recover from disk if persistence path is provided
        if let Some(ref path) = persistence_path {
            info!(
                "[RANGED INDEX LOAD] Attempting to load persisted tree map from: {:?}",
                path
            );
            if let Err(e) = sm.recover_from_disk(path) {
                info!(
                    "[RANGED INDEX LOAD] No existing persisted tree map or failed to load: {:?}",
                    e
                );
            } else {
                info!(
                    "[RANGED INDEX LOAD] Successfully loaded MasterTreeSM from disk with {} tree entries",
                    sm.tree.len()
                );
            }
        } else {
            info!("[RANGED INDEX LOAD] No persistence path provided, starting with empty tree");
        }

        sm
    }

    async fn is_plane_leader(&self) -> bool {
        self.raft_svr
            .is_leader_on_plane(self.plane_id)
            .await
            .unwrap_or(false)
    }

    pub async fn try_initialize(&mut self) -> bool {
        // Don't initialize if tree already has data (recovered from disk)
        if !self.tree.is_empty() {
            info!(
                "[RANGED INDEX LOAD] MasterTreeSM already has persisted data, loading {} trees",
                self.tree.len()
            );
            self.load_all_placements().await;
            return true;
        }

        // No local data: genesis is established via the try_init_genesis raft
        // command after this state machine registers on the plane. Seeding it
        // here would give every cluster member a different member-local
        // placement map — the split-brain that strands index entries.
        info!(
            "[RANGED INDEX LOAD] MasterTreeSM has no persisted data; genesis deferred to consensus"
        );
        false
    }

    /// Load every placed tree into the tree service. Idempotent, and needed
    /// from two places: startup with persisted placements, and recovery, where
    /// the placement map arrives from the plane AFTER `try_initialize` has
    /// already run against an empty map.
    async fn load_all_placements(&mut self) {
        {
            let tree_entries: Vec<_> = self
                .tree
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            for i in 0..tree_entries.len() {
                let (lower, placement) = &tree_entries[i];
                let upper = if i + 1 < tree_entries.len() {
                    tree_entries[i + 1].0.clone()
                } else {
                    max_entry_key()
                };
                info!(
                    "[RANGED INDEX LOAD] Loading tree {}/{}: tree_id={:?}, boundary=[{:?}, {:?}), epoch={}",
                    i + 1,
                    tree_entries.len(),
                    placement.id,
                    lower,
                    upper,
                    placement.epoch
                );
                self.load_sub_tree(placement.id, lower, &upper, placement.epoch)
                    .await;
                info!(
                    "[RANGED INDEX LOAD] Completed loading tree {}/{}",
                    i + 1,
                    tree_entries.len()
                );
            }
            info!(
                "[RANGED INDEX LOAD] All {} trees loaded successfully",
                tree_entries.len()
            );
        }
    }

    /// Selects a candidate genesis tree id preferring local ownership.
    pub fn select_genesis_tree_id(conshash: &ConsistentHashing, local_server_id: u64) -> Id {
        for _ in 0..LOCAL_TREE_ID_SELECTION_MAX_ATTEMPTS {
            let id = Id::rand();
            // By slot, like everything else that locates stored data. A tree's
            // pages now inherit its locality, so choosing its server the same way
            // keeps service and pages on one member -- and keeps them together
            // when that slot migrates.
            if conshash.get_server_id_for_slot(id.locality() as u64) == Some(local_server_id) {
                return id;
            }
        }
        Id::rand()
    }

    fn persist_tree(&self) -> io::Result<()> {
        let path = match &self.persistence_path {
            Some(p) => p,
            None => return Ok(()), // No persistence configured
        };

        // Create parent directory if it doesn't exist
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Write to a temporary file first
        let temp_path = path.with_extension("tmp");
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;
        let mut writer = BufWriter::new(file);

        // Convert BTreeMap to Vec for serialization
        let entries: Vec<(EntryKey, TreePlacement)> = self
            .tree
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Serialize the vec
        let serialized = utils::serde::serialize(&entries);
        writer.write_all(&serialized)?;
        writer.flush()?;
        // A rename is only atomic-durable if the file's contents are on
        // disk first and the directory entry follows: without these syncs
        // the placement snapshot could come back empty or truncated after
        // a power failure, and split epochs would rewind.
        writer.get_ref().sync_all()?;

        // Atomically replace the old file
        fs::rename(&temp_path, path)?;
        if let Some(parent) = path.parent() {
            if let Ok(dir) = fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }

        Ok(())
    }

    fn recover_from_disk(&mut self, path: &Path) -> io::Result<()> {
        info!(
            "[RANGED INDEX LOAD] Attempting to load MasterTreeSM from disk: {:?}",
            path
        );
        if !path.exists() {
            info!(
                "[RANGED INDEX LOAD] Tree persistence file not found at {:?}",
                path
            );
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Tree persistence file not found",
            ));
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;
        info!(
            "[RANGED INDEX LOAD] Read {} bytes from persistence file",
            buffer.len()
        );

        if buffer.is_empty() {
            info!("[RANGED INDEX LOAD] Persistence file is empty");
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Empty persistence file",
            ));
        }

        let entries: Vec<(EntryKey, TreePlacement)> = utils::serde::deserialize(&buffer)
            .ok_or_else(|| {
                error!("[RANGED INDEX LOAD] Failed to deserialize tree data");
                io::Error::new(io::ErrorKind::InvalidData, "Failed to deserialize tree")
            })?;

        // Convert Vec back to BTreeMap
        self.tree = entries.into_iter().collect();
        info!(
            "[RANGED INDEX LOAD] Successfully loaded tree map with {} entries",
            self.tree.len()
        );
        for (key, placement) in &self.tree {
            info!(
                "[RANGED INDEX LOAD]   Tree entry: key={:?}, tree_id={:?}, epoch={}",
                key, placement.id, placement.epoch
            );
        }

        Ok(())
    }


    async fn load_sub_tree(&mut self, id: Id, lower: &EntryKey, upper: &EntryKey, epoch: u64) {
        if self.is_plane_leader().await {
            // Only the leader can initiate the request to load the sub tree
            info!(
                "[RANGED INDEX LOAD] Placement leader calling to load sub tree {:?} with lower key {:?}, upper key {:?}, epoch={}",
                id, lower, upper, epoch
            );
            let client = self.locate_tree_server(&id).await.unwrap();
            info!(
                "[RANGED INDEX LOAD] Located tree {:?} at server {:?}",
                id,
                client.server_id()
            );
            client
                .load_tree(id, Boundary::new(lower.clone(), upper.clone()), epoch)
                .await
                .unwrap();
            info!(
                "[RANGED INDEX LOAD] Successfully loaded tree {:?} into LSM service",
                id
            );
        } else {
            info!(
                "[RANGED INDEX LOAD] Not leader, skipping load_sub_tree for {:?}",
                id
            );
        }
    }

    async fn locate_tree_server(&self, id: &Id) -> Result<Arc<LSMServiceClient>, RPCError> {
        // Slot-keyed, so the tree is served by whoever holds its pages. Hashing
        // the whole id (the old rule) is a placement function the slot table does
        // not govern, so a migration moved the pages and left the service behind.
        if let Some(server_id) = self.conshash.get_server_id_for_slot(id.locality() as u64) {
            bifrost::rpc::DEFAULT_CLIENT_POOL
                .get_by_id(server_id, move |sid| self.conshash.try_server_name(sid))
                .await
                .map_err(|e| RPCError::IOError(e))
                .map(|rpc| LSMServiceClient::new_with_service_id(self.tree_service_id, &rpc))
        } else {
            Err(RPCError::RequestError(bifrost::rpc::RPCRequestError::Other))
        }
    }
}

impl TreePlacement {
    pub fn new(id: Id) -> Self {
        TreePlacement {
            id,
            epoch: INITIAL_TREE_EPOCH,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost::conshash::weights::Weights;
    use bifrost::membership::client::ObserverClient;
    use bifrost::membership::member::MemberService;
    use bifrost::membership::server::Membership;
    use bifrost::raft;
    use bifrost::raft::client::RaftClient;
    use bifrost::raft::disk::DiskOptions;
    use bifrost::rpc::Server;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_master_tree_persistence_and_recovery() {
        let _ = env_logger::try_init();

        let temp_dir = TempDir::new().unwrap();
        let tree_path = temp_dir.path().join("master_tree.dat");

        info!("=== Testing direct tree persistence and recovery ===");

        // Create test data
        let test_id_1 = Id::rand();
        let test_id_2 = Id::rand();
        let test_id_3 = Id::rand();

        let key1 = min_entry_key();
        let mut key2_bytes = vec![0x20];
        key2_bytes.extend(vec![0; 31]);
        let key2 = EntryKey::from_slice(&key2_bytes);
        let mut key3_bytes = vec![0x30];
        key3_bytes.extend(vec![0; 31]);
        let key3 = EntryKey::from_slice(&key3_bytes);

        // Create initial tree and persist
        {
            let mut tree = BTreeMap::new();
            tree.insert(
                key1.clone(),
                TreePlacement {
                    id: test_id_1,
                    epoch: 1,
                },
            );
            tree.insert(
                key2.clone(),
                TreePlacement {
                    id: test_id_2,
                    epoch: 1,
                },
            );
            tree.insert(
                key3.clone(),
                TreePlacement {
                    id: test_id_3,
                    epoch: 2,
                },
            );

            info!("Created tree with {} entries", tree.len());

            // Serialize and write manually
            let entries: Vec<(EntryKey, TreePlacement)> =
                tree.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            let serialized = utils::serde::serialize(&entries);
            fs::write(&tree_path, &serialized).unwrap();

            info!("Persisted tree to disk");
        }

        // Verify persistence file exists
        assert!(tree_path.exists(), "Persistence file should exist");
        let file_size = fs::metadata(&tree_path).unwrap().len();
        info!("Persistence file size: {} bytes", file_size);
        assert!(file_size > 0, "Persistence file should not be empty");

        // Recover the tree
        {
            let buffer = fs::read(&tree_path).unwrap();
            let entries: Vec<(EntryKey, TreePlacement)> =
                utils::serde::deserialize(&buffer).unwrap();
            let recovered_tree: BTreeMap<EntryKey, TreePlacement> = entries.into_iter().collect();

            info!("Recovered tree with {} entries", recovered_tree.len());

            // Verify recovery worked - should have the same 3 entries
            assert_eq!(
                recovered_tree.len(),
                3,
                "Recovered tree should have 3 entries"
            );

            // Verify specific entries exist
            assert!(recovered_tree.contains_key(&key1));
            assert!(recovered_tree.contains_key(&key2));
            assert!(recovered_tree.contains_key(&key3));

            // Verify epochs
            let entry1 = recovered_tree.get(&key1).unwrap();
            assert_eq!(entry1.epoch, 1);
            assert_eq!(entry1.id, test_id_1);

            let entry2 = recovered_tree.get(&key2).unwrap();
            assert_eq!(entry2.epoch, 1);
            assert_eq!(entry2.id, test_id_2);

            let entry3 = recovered_tree.get(&key3).unwrap();
            assert_eq!(entry3.epoch, 2);
            assert_eq!(entry3.id, test_id_3);
        }

        info!("Tree persistence and recovery test passed!");
    }

    #[tokio::test]
    async fn test_master_tree_no_persistence() {
        let _ = env_logger::try_init();

        let temp_dir = TempDir::new().unwrap();
        let raft_path = temp_dir.path().join("raft");
        let server_addr = &crate::utils::test_port::unique_localhost_addr();
        let group_name = "tree_no_persist_test";

        info!("=== Testing MasterTreeSM without persistence ===");

        let rpc_server = Server::new(&server_addr.to_string());
        let storage = raft::Storage::DISK(DiskOptions {
            path: raft_path.to_str().unwrap().to_string(),
            take_snapshots: false,
            append_logs: false,
            trim_logs: false,
            snapshot_log_threshold: 1000,
            log_compaction_threshold: 2000,
        });

        let raft_service = raft::RaftService::new(raft::Options {
            storage,
            address: server_addr.to_string(),
            service_id: raft::DEFAULT_SERVICE_ID,
        });

        Weights::new_with_id(1000, &raft_service).await;
        rpc_server.register_service(&raft_service).await;
        Server::listen_and_resume(&rpc_server).await;
        Membership::new(&rpc_server, &raft_service).await;
        raft::RaftService::start(&raft_service, false).await;
        raft_service.bootstrap().await;

        let raft_client = RaftClient::new(&vec![server_addr.to_string()], raft::DEFAULT_SERVICE_ID)
            .await
            .unwrap();
        RaftClient::prepare_subscription(&rpc_server).await;

        let member_service =
            MemberService::new(&server_addr.to_string(), &raft_client, &raft_service).await;
        member_service
            .join_group(&group_name.to_string())
            .await
            .unwrap();

        let membership_client = Arc::new(ObserverClient::new(&raft_client));
        let conshash = bifrost::conshash::ConsistentHashing::new_with_id(
            1000,
            &group_name.to_string(),
            &raft_client,
            &membership_client,
        )
        .await
        .unwrap();
        conshash
            .set_weight(&server_addr.to_string(), 1024)
            .await
            .unwrap();
        conshash.init_table().await.unwrap();

        // Create MasterTreeSM without persistence on the root plane.
        let mut tree_sm = MasterTreeSM::new_with_id_and_persistence_on_plane(
            DEFAULT_SM_ID,
            DEFAULT_SERVICE_ID,
            &raft_service,
            PlaneId::type1(),
            &conshash,
            None,
        );

        // Add an entry
        let mut key_bytes = vec![0x40];
        key_bytes.extend(vec![0; 31]);
        let key = EntryKey::from_slice(&key_bytes);
        tree_sm.tree.insert(
            key,
            TreePlacement {
                id: Id::rand(),
                epoch: 1,
            },
        );

        // Try to persist - should succeed but do nothing
        tree_sm.persist_tree().unwrap();

        info!("Added entry without persistence path");

        // Verify no persistence file was created
        let potential_path = temp_dir.path().join("master_tree.dat");
        assert!(
            !potential_path.exists(),
            "No persistence file should be created when path is None"
        );

        info!("No-persistence test passed!");
    }

    #[test]
    fn test_tree_placement_creation() {
        let test_id = Id::rand();
        let placement = TreePlacement::new(test_id);

        assert_eq!(placement.id, test_id);
        assert_eq!(placement.epoch, INITIAL_TREE_EPOCH);
    }

    #[test]
    fn scoped_sm_id_differs_across_databases() {
        assert_eq!(generate_scoped_sm_id("group_a", "group_a"), DEFAULT_SM_ID);
        assert_ne!(
            generate_scoped_sm_id("group_a", "db_a"),
            generate_scoped_sm_id("group_a", "db_b")
        );
    }
}
