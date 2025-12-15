use crate::ram::cell::{ReadError, WriteError};
use crate::ram::schema::{Field, Schema};
use crate::ram::types::*;
use crate::{client::AsyncClient, ram::cell::OwnedCell};
use bifrost::rpc::RPCError;
use bifrost_hasher::hash_str;
use std::sync::Arc;

use super::Feature;

const MAX_CAS_RETRIES: u32 = 100;

const HASH_SCHEMA: &'static str = "HASH_INDEX_SCHEMA";
const HASH_INDEX_FIELD: &'static str = "CELL_ID";

lazy_static! {
    pub static ref HASH_INDEX_SCHEMA_ID: u32 = key_hash(HASH_SCHEMA) as u32;
    pub static ref HASH_INDEX_FIELD_ID: u64 = hash_str(HASH_INDEX_FIELD);
}

pub struct HashIndexer {
    neb_client: Arc<AsyncClient>,
}

impl HashIndexer {
    pub fn new(neb_client: &Arc<AsyncClient>) -> Self {
        HashIndexer {
            neb_client: neb_client.clone(),
        }
    }

    pub async fn add_index(&self, cell_id: &Id, index_id: &Id) -> Result<(), WriteError> {
        debug!(
            "Attempting to add index for cell_id: {:?}, index_id: {:?}",
            cell_id, index_id
        );

        // Retry loop for compare-and-swap
        for retry in 0..MAX_CAS_RETRIES {
            // Read the current cell
            match self.neb_client.read_cell(*index_id).await {
                Ok(Ok(mut cell)) => {
                    // Cell exists - update it with CAS
                    let version = cell.header.version;
                    let ids_val = &mut cell[*HASH_INDEX_FIELD_ID];

                    if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids_val {
                        // Check if cell_id is already in the array
                        if ids.contains(cell_id) {
                            debug!("Cell {:?} already in index {:?}", cell_id, index_id);
                            return Ok(());
                        }

                        // Add the cell_id to the array
                        ids.push(*cell_id);
                        let new_value = OwnedValue::PrimArray(OwnedPrimArray::Id(ids.clone()));

                        // Use compare-and-swap to update the field atomically
                        match self
                            .neb_client
                            .compare_version_and_set_field(
                                *index_id,
                                version,
                                *HASH_INDEX_FIELD_ID,
                                new_value,
                            )
                            .await
                        {
                            Ok(Ok(_)) => {
                                debug!(
                                    "Successfully added cell {:?} to index {:?}",
                                    cell_id, index_id
                                );
                                return Ok(());
                            }
                            Ok(Err(WriteError::CellVersionMismatch)) => {
                                debug!("CAS retry {} for add_index", retry + 1);
                                continue; // Retry
                            }
                            Ok(Err(e)) => return Err(e),
                            Err(e) => return Err(WriteError::NetworkingError),
                        }
                    } else {
                        return Err(WriteError::DataMismatchSchema(
                            Field::new_unindexed_array(HASH_INDEX_FIELD, Type::Id),
                            ids_val.clone(),
                        ));
                    }
                }
                Ok(Err(ReadError::CellDoesNotExisted)) => {
                    // Cell doesn't exist - create it
                    let mut map = OwnedMap::new();
                    map.insert_key_id(
                        *HASH_INDEX_FIELD_ID,
                        OwnedValue::PrimArray(OwnedPrimArray::Id(vec![*cell_id])),
                    );
                    let cell = OwnedCell::new_with_id(
                        *HASH_INDEX_SCHEMA_ID,
                        index_id,
                        OwnedValue::Map(map),
                    );

                    match self.neb_client.write_cell(cell).await {
                        Ok(Ok(_)) => {
                            debug!(
                                "Created new index cell {:?} with cell_id {:?}",
                                index_id, cell_id
                            );
                            return Ok(());
                        }
                        Ok(Err(WriteError::CellAlreadyExisted)) => {
                            debug!("Cell was created concurrently, retrying");
                            continue; // Someone else created it, retry
                        }
                        Ok(Err(e)) => return Err(e),
                        Err(_) => return Err(WriteError::NetworkingError),
                    }
                }
                Ok(Err(e)) => return Err(WriteError::ReadError(e)),
                Err(_) => return Err(WriteError::NetworkingError),
            }
        }

        warn!(
            "Max CAS retries exceeded for add_index({:?}, {:?})",
            cell_id, index_id
        );
        Err(WriteError::CellVersionMismatch)
    }

    pub async fn remove_index(&self, cell_id: &Id, index_id: &Id) -> Result<(), WriteError> {
        debug!(
            "Attempting to remove index for cell_id: {:?}, index_id: {:?}",
            cell_id, index_id
        );

        // Retry loop for compare-and-swap
        for retry in 0..MAX_CAS_RETRIES {
            // Read the current cell
            match self.neb_client.read_cell(*index_id).await {
                Ok(Ok(mut cell)) => {
                    // Cell exists - update or remove it with CAS
                    let version = cell.header.version;
                    let ids_val = &mut cell[*HASH_INDEX_FIELD_ID];

                    if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids_val {
                        // Check if cell_id is in the array
                        if !ids.contains(cell_id) {
                            debug!(
                                "Cell {:?} not in index {:?}, nothing to remove",
                                cell_id, index_id
                            );
                            return Ok(());
                        }

                        // Remove the cell_id from the array
                        ids.retain(|id| *id != *cell_id);

                        if ids.is_empty() {
                            // If array is now empty, remove the entire index cell
                            match self.neb_client.remove_cell(*index_id).await {
                                Ok(Ok(_)) => {
                                    debug!("Removed empty index cell {:?}", index_id);
                                    return Ok(());
                                }
                                Ok(Err(WriteError::CellDoesNotExisted)) => {
                                    debug!("Index cell {:?} was already removed", index_id);
                                    return Ok(());
                                }
                                Ok(Err(e)) => return Err(e),
                                Err(_) => return Err(WriteError::NetworkingError),
                            }
                        } else {
                            // Update the field with the new array (without the removed cell_id)
                            let new_value = OwnedValue::PrimArray(OwnedPrimArray::Id(ids.clone()));

                            match self
                                .neb_client
                                .compare_version_and_set_field(
                                    *index_id,
                                    version,
                                    *HASH_INDEX_FIELD_ID,
                                    new_value,
                                )
                                .await
                            {
                                Ok(Ok(_)) => {
                                    debug!(
                                        "Successfully removed cell {:?} from index {:?}",
                                        cell_id, index_id
                                    );
                                    return Ok(());
                                }
                                Ok(Err(WriteError::CellVersionMismatch)) => {
                                    debug!("CAS retry {} for remove_index", retry + 1);
                                    continue; // Retry
                                }
                                Ok(Err(e)) => return Err(e),
                                Err(_) => return Err(WriteError::NetworkingError),
                            }
                        }
                    } else {
                        return Err(WriteError::DataMismatchSchema(
                            Field::new_unindexed_array(HASH_INDEX_FIELD, Type::Id),
                            ids_val.clone(),
                        ));
                    }
                }
                Ok(Err(ReadError::CellDoesNotExisted)) => {
                    debug!(
                        "Index cell {:?} does not exist, nothing to remove",
                        index_id
                    );
                    return Ok(());
                }
                Ok(Err(e)) => return Err(WriteError::ReadError(e)),
                Err(_) => return Err(WriteError::NetworkingError),
            }
        }

        warn!(
            "Max CAS retries exceeded for remove_index({:?}, {:?})",
            cell_id, index_id
        );
        Err(WriteError::CellVersionMismatch)
    }

    pub async fn query(
        &self,
        index_id: Id,
        field_id: u64,
        value: &OwnedValue,
    ) -> Result<Result<Vec<Id>, ReadError>, RPCError> {
        let read_res = self.neb_client.read_cell(index_id).await;
        let mut result = Vec::new();
        match read_res {
            Ok(Ok(cell)) => {
                let cell_ids_val = &cell[*HASH_INDEX_FIELD_ID];
                if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = cell_ids_val {
                    for id in ids {
                        // Now we need to check each of the cell id that they have
                        // a field matching the field id and have exact the same value
                        let cell_res = self
                            .neb_client
                            .read_cell_select(*id, &vec![field_id], false)
                            .await;
                        if let Ok(Ok(cell)) = &cell_res {
                            let field_val = &cell[0usize];
                            if field_val == value {
                                result.push(*id);
                            } else {
                                debug!(
                                    "Cell {:?} has field {:?} with value {:?}, but expected {:?}",
                                    id, field_id, field_val, value
                                );
                            }
                        }
                    }
                }
                return Ok(Ok(result));
            }
            Ok(Err(ReadError::CellDoesNotExisted)) => {
                return Ok(Ok(vec![]));
            }
            Ok(Err(e)) => {
                return Ok(Err(e));
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
}

pub fn hash_index_schema() -> Schema {
    Schema::new_with_id(
        *HASH_INDEX_SCHEMA_ID,
        &HASH_SCHEMA.to_string(),
        None,
        Field::new_schema(vec![Field::new_unindexed_array(HASH_INDEX_FIELD, Type::Id)]),
        false,
        false,
    )
}

pub struct HashedIndexClient {
    pub client: Arc<AsyncClient>,
    pub indexer: HashIndexer,
}

impl HashedIndexClient {
    pub fn new(client: &Arc<AsyncClient>) -> Self {
        let indexer = HashIndexer::new(&client);
        HashedIndexClient {
            client: client.clone(),
            indexer,
        }
    }

    pub async fn insert(&self, hash_id: &Id, cell_id: &Id) -> Result<(), WriteError> {
        self.indexer.add_index(cell_id, hash_id).await
    }

    pub async fn query(
        &self,
        index_id: Id,
        field_id: u64,
        value: &OwnedValue,
    ) -> Result<Result<Vec<Id>, ReadError>, RPCError> {
        self.indexer.query(index_id, field_id, value).await
    }
}

pub fn get_hash_id(schema: u32, field: u64, hash_feat: Feature) -> Id {
    Id::from_obj(&(schema, field, hash_feat))
}

pub fn get_hash_id_from_value<V: Value>(schema: u32, field: u64, value: &V) -> Id {
    let hash_feat = value.hash();
    get_hash_id(schema, field, hash_feat)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::AsyncClient;
    use crate::ram::schema::Field;
    use crate::server::{NebServer, ServerOptions, Service};
    use std::sync::Arc;
    use tokio::task::JoinSet;

    /// Helper function to create a test server
    async fn create_test_server(name: &str) -> (Arc<NebServer>, Arc<AsyncClient>) {
        let _ = env_logger::try_init();
        let server_addr = format!("127.0.0.1:{}", 5500 + rand::random::<u16>() % 1000);
        let server_group = format!("hash_index_test_{}", name);

        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 1,
                total_size: 16 * 1024 * 1024,
                tiered_config: None,
                backup_storage: None,
                wal_storage: None,
                undo_log_storage: None,
                raft_storage: None,
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
            AsyncClient::new(
                &server.rpc,
                &server.membership,
                &vec![server_addr],
                &server_group,
            )
            .await
            .unwrap(),
        );

        // Initialize hash index schema
        let _ = client.new_schema_with_id(hash_index_schema()).await;

        (server, client)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_add_index_new_cell() {
        let (_server, client) = create_test_server("add_index_new").await;
        let indexer = HashIndexer::new(&client);

        let cell_id = Id::rand();
        let index_id = Id::rand();

        // Add index to a non-existent cell (should create it)
        let result = indexer.add_index(&cell_id, &index_id).await;
        assert!(result.is_ok(), "Failed to add index: {:?}", result);

        // Verify the cell was created with the correct data
        let cell = client.read_cell(index_id).await.unwrap().unwrap();
        let ids = &cell[*HASH_INDEX_FIELD_ID];
        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids {
            assert_eq!(ids.len(), 1);
            assert_eq!(ids[0], cell_id);
        } else {
            panic!("Expected Id array, got {:?}", ids);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_add_index_existing_cell() {
        let (_server, client) = create_test_server("add_index_existing").await;
        let indexer = HashIndexer::new(&client);

        let cell_id_1 = Id::rand();
        let cell_id_2 = Id::rand();
        let index_id = Id::rand();

        // Add first cell
        indexer.add_index(&cell_id_1, &index_id).await.unwrap();

        // Add second cell to the same index
        indexer.add_index(&cell_id_2, &index_id).await.unwrap();

        // Verify both cells are in the index
        let cell = client.read_cell(index_id).await.unwrap().unwrap();
        let ids = &cell[*HASH_INDEX_FIELD_ID];
        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids {
            assert_eq!(ids.len(), 2);
            assert!(ids.contains(&cell_id_1));
            assert!(ids.contains(&cell_id_2));
        } else {
            panic!("Expected Id array, got {:?}", ids);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_add_index_duplicate() {
        let (_server, client) = create_test_server("add_index_duplicate").await;
        let indexer = HashIndexer::new(&client);

        let cell_id = Id::rand();
        let index_id = Id::rand();

        // Add same cell twice
        indexer.add_index(&cell_id, &index_id).await.unwrap();
        indexer.add_index(&cell_id, &index_id).await.unwrap();

        // Verify only one entry exists (no duplicates)
        let cell = client.read_cell(index_id).await.unwrap().unwrap();
        let ids = &cell[*HASH_INDEX_FIELD_ID];
        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids {
            assert_eq!(ids.len(), 1);
            assert_eq!(ids[0], cell_id);
        } else {
            panic!("Expected Id array, got {:?}", ids);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_remove_index() {
        let (_server, client) = create_test_server("remove_index").await;
        let indexer = HashIndexer::new(&client);

        let cell_id = Id::rand();
        let index_id = Id::rand();

        // Add and then remove
        indexer.add_index(&cell_id, &index_id).await.unwrap();
        indexer.remove_index(&cell_id, &index_id).await.unwrap();

        // Verify the index cell was removed (empty array means cell is deleted)
        let result = client.read_cell(index_id).await;
        assert!(
            matches!(result, Ok(Err(ReadError::CellDoesNotExisted))),
            "Expected cell to be deleted, got {:?}",
            result
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_remove_index_multiple_entries() {
        let (_server, client) = create_test_server("remove_multiple").await;
        let indexer = HashIndexer::new(&client);

        let cell_id_1 = Id::rand();
        let cell_id_2 = Id::rand();
        let cell_id_3 = Id::rand();
        let index_id = Id::rand();

        // Add three cells
        indexer.add_index(&cell_id_1, &index_id).await.unwrap();
        indexer.add_index(&cell_id_2, &index_id).await.unwrap();
        indexer.add_index(&cell_id_3, &index_id).await.unwrap();

        // Remove one
        indexer.remove_index(&cell_id_2, &index_id).await.unwrap();

        // Verify two remain
        let cell = client.read_cell(index_id).await.unwrap().unwrap();
        let ids = &cell[*HASH_INDEX_FIELD_ID];
        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids {
            assert_eq!(ids.len(), 2);
            assert!(ids.contains(&cell_id_1));
            assert!(ids.contains(&cell_id_3));
            assert!(!ids.contains(&cell_id_2));
        } else {
            panic!("Expected Id array, got {:?}", ids);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_remove_nonexistent() {
        let (_server, client) = create_test_server("remove_nonexistent").await;
        let indexer = HashIndexer::new(&client);

        let cell_id = Id::rand();
        let index_id = Id::rand();

        // Remove from non-existent index (should succeed silently)
        let result = indexer.remove_index(&cell_id, &index_id).await;
        assert!(
            result.is_ok(),
            "Remove from non-existent index should succeed"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_query_empty() {
        let (_server, client) = create_test_server("query_empty").await;
        let indexer = HashIndexer::new(&client);

        let index_id = Id::rand();
        let field_id = 123u64;
        let value = OwnedValue::I64(42);

        // Query non-existent index
        let result = indexer.query(index_id, field_id, &value).await;
        assert!(result.is_ok());
        let ids = result.unwrap().unwrap();
        assert_eq!(ids.len(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_adds() {
        let (_server, client) = create_test_server("concurrent_adds").await;
        let indexer = Arc::new(HashIndexer::new(&client));
        let index_id = Id::rand();

        // Spawn 10 concurrent tasks adding different cells to the same index
        let mut tasks = JoinSet::new();
        let cell_ids: Vec<Id> = (0..10).map(|_| Id::rand()).collect();

        for cell_id in cell_ids.iter() {
            let indexer = indexer.clone();
            let cell_id = *cell_id;
            let index_id = index_id;
            tasks.spawn(async move { indexer.add_index(&cell_id, &index_id).await });
        }

        // Wait for all tasks to complete
        while let Some(result) = tasks.join_next().await {
            assert!(result.is_ok(), "Task failed: {:?}", result);
            assert!(result.unwrap().is_ok(), "Add index failed");
        }

        // Verify all 10 cells are in the index
        let cell = client.read_cell(index_id).await.unwrap().unwrap();
        let ids = &cell[*HASH_INDEX_FIELD_ID];
        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids {
            assert_eq!(ids.len(), 10, "Expected 10 cells, got {}", ids.len());
            for cell_id in cell_ids.iter() {
                assert!(
                    ids.contains(cell_id),
                    "Cell {:?} not found in index",
                    cell_id
                );
            }
        } else {
            panic!("Expected Id array, got {:?}", ids);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_removes() {
        let (_server, client) = create_test_server("concurrent_removes").await;
        let indexer = Arc::new(HashIndexer::new(&client));
        let index_id = Id::rand();

        // Add 20 cells first
        let cell_ids: Vec<Id> = (0..20).map(|_| Id::rand()).collect();
        for cell_id in cell_ids.iter() {
            indexer.add_index(cell_id, &index_id).await.unwrap();
        }

        // Concurrently remove 10 of them
        let mut tasks = JoinSet::new();
        for cell_id in cell_ids.iter().take(10) {
            let indexer = indexer.clone();
            let cell_id = *cell_id;
            let index_id = index_id;
            tasks.spawn(async move { indexer.remove_index(&cell_id, &index_id).await });
        }

        // Wait for all removals to complete
        while let Some(result) = tasks.join_next().await {
            assert!(result.is_ok(), "Task failed: {:?}", result);
            assert!(result.unwrap().is_ok(), "Remove index failed");
        }

        // Verify 10 cells remain
        let cell = client.read_cell(index_id).await.unwrap().unwrap();
        let ids = &cell[*HASH_INDEX_FIELD_ID];
        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids {
            assert_eq!(ids.len(), 10, "Expected 10 cells, got {}", ids.len());
            // Verify the remaining 10 are the ones we didn't remove
            for cell_id in cell_ids.iter().skip(10) {
                assert!(
                    ids.contains(cell_id),
                    "Cell {:?} should still be in index",
                    cell_id
                );
            }
            // Verify the removed 10 are gone
            for cell_id in cell_ids.iter().take(10) {
                assert!(
                    !ids.contains(cell_id),
                    "Cell {:?} should have been removed",
                    cell_id
                );
            }
        } else {
            panic!("Expected Id array, got {:?}", ids);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_mixed_operations() {
        let (_server, client) = create_test_server("concurrent_mixed").await;
        let indexer = Arc::new(HashIndexer::new(&client));
        let index_id = Id::rand();

        // Spawn mixed add/remove operations concurrently
        let mut tasks = JoinSet::new();
        let cell_ids: Vec<Id> = (0..20).map(|_| Id::rand()).collect();

        // Add all cells
        for (i, cell_id) in cell_ids.iter().enumerate() {
            let indexer = indexer.clone();
            let cell_id = *cell_id;
            let index_id = index_id;

            if i % 2 == 0 {
                // Even indices: add
                tasks.spawn(async move { indexer.add_index(&cell_id, &index_id).await });
            } else {
                // Odd indices: add then remove (to test concurrent add/remove)
                tasks.spawn(async move {
                    indexer.add_index(&cell_id, &index_id).await?;
                    indexer.remove_index(&cell_id, &index_id).await
                });
            }
        }

        // Wait for all operations
        while let Some(result) = tasks.join_next().await {
            assert!(result.is_ok(), "Task panicked: {:?}", result);
            assert!(result.unwrap().is_ok(), "Operation failed");
        }

        // Verify only even-indexed cells remain
        let cell = client.read_cell(index_id).await.unwrap().unwrap();
        let ids = &cell[*HASH_INDEX_FIELD_ID];
        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids {
            assert_eq!(ids.len(), 10, "Expected 10 cells, got {}", ids.len());
            for (i, cell_id) in cell_ids.iter().enumerate() {
                if i % 2 == 0 {
                    assert!(
                        ids.contains(cell_id),
                        "Even cell {:?} should be in index",
                        cell_id
                    );
                } else {
                    assert!(
                        !ids.contains(cell_id),
                        "Odd cell {:?} should not be in index",
                        cell_id
                    );
                }
            }
        } else {
            panic!("Expected Id array, got {:?}", ids);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stress_cas_retries() {
        let (_server, client) = create_test_server("stress_cas").await;
        let indexer = Arc::new(HashIndexer::new(&client));
        let index_id = Id::rand();

        // Create very high contention: 50 concurrent operations on the same index
        let mut tasks = JoinSet::new();
        for i in 0..50 {
            let indexer = indexer.clone();
            let cell_id = Id::rand();
            let index_id = index_id;

            tasks.spawn(async move {
                // Add
                indexer.add_index(&cell_id, &index_id).await?;
                // Immediate remove (high contention)
                if i % 3 == 0 {
                    indexer.remove_index(&cell_id, &index_id).await?;
                }
                Ok::<_, WriteError>(())
            });
        }

        // Wait for all operations
        let mut success_count = 0;
        while let Some(result) = tasks.join_next().await {
            if result.is_ok() && result.unwrap().is_ok() {
                success_count += 1;
            }
        }

        // With CAS retries, all operations should eventually succeed
        assert_eq!(
            success_count, 50,
            "Not all operations succeeded despite CAS retries"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_hashed_index_client() {
        let (_server, client) = create_test_server("client_test").await;
        let hashed_client = HashedIndexClient::new(&client);

        let cell_id = Id::rand();
        let hash_id = Id::rand();

        // Test insert via client
        let result = hashed_client.insert(&hash_id, &cell_id).await;
        assert!(result.is_ok(), "Client insert failed: {:?}", result);

        // Verify via direct read
        let cell = client.read_cell(hash_id).await.unwrap().unwrap();
        let ids = &cell[*HASH_INDEX_FIELD_ID];
        if let OwnedValue::PrimArray(OwnedPrimArray::Id(ids)) = ids {
            assert_eq!(ids.len(), 1);
            assert_eq!(ids[0], cell_id);
        } else {
            panic!("Expected Id array, got {:?}", ids);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_hash_id_functions() {
        let schema_id = 123u32;
        let field_id = 456u64;
        let hash_feat: Feature = [1, 2, 3, 4, 5, 6, 7, 8];

        let hash_id_1 = get_hash_id(schema_id, field_id, hash_feat);
        let hash_id_2 = get_hash_id(schema_id, field_id, hash_feat);

        // Same inputs should produce same hash ID
        assert_eq!(hash_id_1, hash_id_2);

        // Different inputs should produce different hash IDs
        let hash_id_3 = get_hash_id(schema_id + 1, field_id, hash_feat);
        assert_ne!(hash_id_1, hash_id_3);
    }
}
