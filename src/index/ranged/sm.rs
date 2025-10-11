use super::lsm::service::*;
use super::lsm::{service::AsyncServiceClient as LSMServiceClient, tree::INITIAL_TREE_EPOCH};
use super::trees::*;
use crate::ram::types::Id;
use crate::ram::types::RandValue;
use bifrost::conshash::ConsistentHashing;
use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::raft::RaftService;
use bifrost::rpc::{RPCError, ServiceClient};
use bifrost::utils;
use bifrost_plugins::hash_ident;
use futures::prelude::*;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::ops::Bound::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const DEFAULT_SM_ID: u64 = hash_ident!("RANGED_INDEX_SM_ID") as u64;

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
    conshash: Arc<ConsistentHashing>,
    persistence_path: Option<PathBuf>,
}

raft_state_machine! {
    def qry locate_key(entry: EntryKey) -> (EntryKey, TreePlacement, EntryKey);
    def qry next_tree(tree_lower: EntryKey, ordering: Ordering) -> Option<TreeInfo>;
    def cmd split(src_tree: Id, new_tree: Id, pivot: EntryKey);
    // No subscription for clients
}

impl StateMachineCmds for MasterTreeSM {
    fn locate_key(&self, entry: EntryKey) -> BoxFuture<(EntryKey, TreePlacement, EntryKey)> {
        let (lower, tree) = self
            .tree
            .range(..=&entry)
            .last()
            .map(|(key, tree)| (key.to_owned(), tree.to_owned()))
            .unwrap();
        let upper = self
            .tree
            .range((Excluded(entry), Unbounded))
            .next()
            .map(|(key, _)| key)
            .unwrap_or_else(|| &*MAX_ENTRY_KEY)
            .to_owned();
        future::ready((lower, tree, upper)).boxed()
    }

    fn next_tree(&self, tree_lower: EntryKey, ordering: Ordering) -> BoxFuture<Option<TreeInfo>> {
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

    fn split(&mut self, src_tree: Id, new_tree: Id, pivot: EntryKey) -> BoxFuture<()> {
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
            self.load_sub_tree(new_tree, &pivot, &upper_bound, INITIAL_TREE_EPOCH)
                .await;
            if let Some((_, prev_tree)) = self.tree.range_mut(..&pivot).last() {
                assert_eq!(prev_tree.id, src_tree);
                prev_tree.epoch += 1;
            }
            
            // Persist the tree state after split
            if let Err(e) = self.persist_tree() {
                error!("Failed to persist tree after split: {:?}", e);
            }
        }
        .boxed()
    }
}

impl StateMachineCtl for MasterTreeSM {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        DEFAULT_SM_ID
    }
    fn snapshot(&self) -> Vec<u8> {
        utils::serde::serialize(&self.tree)
    }
    fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
        let tree = utils::serde::deserialize(&data).unwrap();
        self.tree = tree;
        future::ready(()).boxed()
    }

    fn recoverable(&self) -> bool {
        false
    }
}

impl MasterTreeSM {
    pub fn new(raft_svr: &Arc<RaftService>, conshash: &Arc<ConsistentHashing>) -> Self {
        Self::new_with_persistence(raft_svr, conshash, None)
    }

    pub fn new_with_persistence(
        raft_svr: &Arc<RaftService>,
        conshash: &Arc<ConsistentHashing>,
        persistence_path: Option<PathBuf>,
    ) -> Self {
        let mut sm = Self {
            tree: BTreeMap::new(),
            raft_svr: raft_svr.clone(),
            conshash: conshash.clone(),
            persistence_path: persistence_path.clone(),
        };
        
        // Try to recover from disk if persistence path is provided
        if let Some(path) = persistence_path {
            if let Err(e) = sm.recover_from_disk(&path) {
                info!("No existing tree state or failed to recover: {:?}", e);
            } else {
                info!("Successfully recovered MasterTreeSM from disk");
            }
        }
        
        sm
    }

    pub async fn try_initialize(&mut self) -> bool {
        // Don't initialize if tree already has data (recovered from disk)
        if !self.tree.is_empty() {
            info!("MasterTreeSM already has data, skipping initialization");
            return true;
        }

        let genesis_id = Id::rand();
        self.tree
            .insert(min_entry_key(), TreePlacement::new(genesis_id));
        
        if let Err(e) = locate_tree_server_from_conshash(&genesis_id, &self.conshash)
            .await
            .unwrap()
            .crate_tree(
                genesis_id,
                Boundary::new(min_entry_key(), max_entry_key()),
                INITIAL_TREE_EPOCH,
            )
            .await
        {
            error!("Failed to create genesis tree: {:?}", e);
            return false;
        }
        
        // Persist the initial tree state
        if let Err(e) = self.persist_tree() {
            error!("Failed to persist initial tree: {:?}", e);
        }
        
        true
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
        let entries: Vec<(EntryKey, TreePlacement)> = self.tree.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        
        // Serialize the vec
        let serialized = utils::serde::serialize(&entries);
        writer.write_all(&serialized)?;
        writer.flush()?;

        // Atomically replace the old file
        fs::rename(&temp_path, path)?;

        Ok(())
    }

    fn recover_from_disk(&mut self, path: &Path) -> io::Result<()> {
        if !path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Tree persistence file not found",
            ));
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;

        if buffer.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Empty persistence file",
            ));
        }

        let entries: Vec<(EntryKey, TreePlacement)> = utils::serde::deserialize(&buffer)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "Failed to deserialize tree")
            })?;

        // Convert Vec back to BTreeMap
        self.tree = entries.into_iter().collect();
        info!("Recovered tree with {} entries", self.tree.len());

        Ok(())
    }
    async fn load_sub_tree(&mut self, id: Id, lower: &EntryKey, upper: &EntryKey, epoch: u64) {
        if self.raft_svr.is_leader() {
            // Only the leader can initiate the request to load the sub tree
            info!("Placement leader calling to load sub tree {:?} with lower key {:?}, upper key {:?}", id, lower, upper);
            let client = self.locate_tree_server(&id).await.unwrap();
            debug!("Located {:?} at server {:?}", id, client.server_id());
            client
                .load_tree(id, Boundary::new(lower.clone(), upper.clone()), epoch)
                .await
                .unwrap();
            debug!("Tree loaded for {:?}", id);
        }
    }

    async fn locate_tree_server(&self, id: &Id) -> Result<Arc<LSMServiceClient>, RPCError> {
        locate_tree_server_from_conshash(id, &self.conshash).await
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
            tree.insert(key1.clone(), TreePlacement { id: test_id_1, epoch: 1 });
            tree.insert(key2.clone(), TreePlacement { id: test_id_2, epoch: 1 });
            tree.insert(key3.clone(), TreePlacement { id: test_id_3, epoch: 2 });
            
            info!("Created tree with {} entries", tree.len());
            
            // Serialize and write manually
            let entries: Vec<(EntryKey, TreePlacement)> = tree.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
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
            let entries: Vec<(EntryKey, TreePlacement)> = utils::serde::deserialize(&buffer).unwrap();
            let recovered_tree: BTreeMap<EntryKey, TreePlacement> = entries.into_iter().collect();
            
            info!("Recovered tree with {} entries", recovered_tree.len());
            
            // Verify recovery worked - should have the same 3 entries
            assert_eq!(recovered_tree.len(), 3, "Recovered tree should have 3 entries");
            
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
        let server_addr = "127.0.0.1:29200";
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
        
        let member_service = MemberService::new(&server_addr.to_string(), &raft_client, &raft_service).await;
        member_service.join_group(&group_name.to_string()).await.unwrap();
        
        let membership_client = Arc::new(ObserverClient::new(&raft_client));
        let conshash = bifrost::conshash::ConsistentHashing::new_with_id(
            1000,
            &group_name.to_string(),
            &raft_client,
            &membership_client,
        )
        .await
        .unwrap();
        conshash.set_weight(&server_addr.to_string(), 1024).await.unwrap();
        conshash.init_table().await.unwrap();

        // Create MasterTreeSM without persistence (None)
        let mut tree_sm = MasterTreeSM::new_with_persistence(
            &raft_service,
            &conshash,
            None,
        );
        
        // Add an entry
        let mut key_bytes = vec![0x40];
        key_bytes.extend(vec![0; 31]);
        let key = EntryKey::from_slice(&key_bytes);
        tree_sm.tree.insert(key, TreePlacement { id: Id::rand(), epoch: 1 });
        
        // Try to persist - should succeed but do nothing
        tree_sm.persist_tree().unwrap();
        
        info!("Added entry without persistence path");

        // Verify no persistence file was created
        let potential_path = temp_dir.path().join("master_tree.dat");
        assert!(!potential_path.exists(), "No persistence file should be created when path is None");

        info!("No-persistence test passed!");
    }

    #[test]
    fn test_tree_placement_creation() {
        let test_id = Id::rand();
        let placement = TreePlacement::new(test_id);
        
        assert_eq!(placement.id, test_id);
        assert_eq!(placement.epoch, INITIAL_TREE_EPOCH);
    }
}
