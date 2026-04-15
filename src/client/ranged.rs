//! Ranged index query client for distributed range queries
//!
//! This module provides `RangedClient` for performing distributed range queries
//! across the Nebuchadnezzar cluster's B-tree based ranged index.

use std::sync::Arc;

use bifrost::conshash::ConsistentHashing;
use bifrost::raft::client::AsRaftPlaneClient;
use bifrost::rpc::RPCError;

use crate::index::entry::EntryKey;
use crate::index::ranged::client::cursor::ClientCursor;
use crate::index::ranged::client::RangedIndexerClient;
use crate::index::ranged::tree::btree::Ordering;
use crate::index::ranged::tree::service::{Range, RangeTerm};
use crate::index::Feature;
use crate::ram::types::Id;

/// Client for distributed ranged index queries
///
/// Provides range query capabilities using the distributed B-tree index.
///
/// # Example
/// ```ignore
/// // Get ranged client from AsyncClient
/// let ranged = client.ranged();
///
/// // Scan all documents of a schema
/// let mut cursor = ranged.scan_schema(schema_id, 100).await?;
/// while let Some(id) = cursor.next().await? {
///     let doc = client.read_cell(id).await?;
/// }
///
/// // Range query on a specific field
/// let mut cursor = ranged.range_query(
///     schema_id,
///     field_id,
///     Some(&min_feature),  // start
///     Some(&max_feature),  // end
///     ScanOrder::Forward,
///     100,
/// ).await?;
/// ```
pub struct RangedClient {
    inner: Arc<RangedIndexerClient>,
}

/// Scan ordering direction
#[derive(Debug, Clone, Copy)]
pub enum ScanOrder {
    /// Forward (ascending) order
    Forward,
    /// Backward (descending) order  
    Backward,
}

impl From<ScanOrder> for Ordering {
    fn from(order: ScanOrder) -> Self {
        match order {
            ScanOrder::Forward => Ordering::Forward,
            ScanOrder::Backward => Ordering::Backward,
        }
    }
}

impl RangedClient {
    /// Create a new ranged client
    pub fn new<C>(conshash: Arc<ConsistentHashing>, raft_client: Arc<C>) -> Self
    where
        C: AsRaftPlaneClient + 'static,
    {
        Self {
            inner: Arc::new(RangedIndexerClient::new(&conshash, &raft_client)),
        }
    }

    pub fn new_for_database<C>(
        conshash: Arc<ConsistentHashing>,
        raft_client: Arc<C>,
        group_name: &str,
        database_name: &str,
    ) -> Self
    where
        C: AsRaftPlaneClient + 'static,
    {
        Self {
            inner: Arc::new(RangedIndexerClient::new_for_database(
                &conshash,
                &raft_client,
                group_name,
                database_name,
            )),
        }
    }

    /// Scan all documents in a schema
    ///
    /// Returns a cursor that iterates over all document IDs in the schema.
    ///
    /// # Arguments
    /// * `schema_id` - Schema ID to scan
    /// * `buffer_size` - Number of IDs to fetch per batch
    pub async fn scan_schema(
        &self,
        schema_id: u32,
        buffer_size: u16,
    ) -> Result<Option<RangedCursor>, RPCError> {
        let key = EntryKey::for_schema(schema_id);
        let range = Range::new_inclusive_opened(key, Ordering::Forward);
        let pattern = Some(self.schema_pattern(schema_id));

        match RangedIndexerClient::seek(&self.inner, range, buffer_size, pattern).await? {
            Some(cursor) => Ok(Some(RangedCursor { inner: cursor })),
            None => Ok(None),
        }
    }

    /// Scan all documents in a schema in specified order
    pub async fn scan_schema_ordered(
        &self,
        schema_id: u32,
        order: ScanOrder,
        buffer_size: u16,
    ) -> Result<Option<RangedCursor>, RPCError> {
        let key = EntryKey::for_schema(schema_id);
        let range = Range::new_inclusive_opened(key, order.into());
        let pattern = Some(self.schema_pattern(schema_id));

        match RangedIndexerClient::seek(&self.inner, range, buffer_size, pattern).await? {
            Some(cursor) => Ok(Some(RangedCursor { inner: cursor })),
            None => Ok(None),
        }
    }

    /// Range query on a specific field
    ///
    /// # Arguments
    /// * `schema_id` - Schema ID
    /// * `field_id` - Field ID to query
    /// * `start_feature` - Start of range (None for open start)
    /// * `end_feature` - End of range (None for open end)
    /// * `order` - Scan direction
    /// * `buffer_size` - Number of IDs per batch
    pub async fn range_query(
        &self,
        schema_id: u32,
        field_id: u64,
        start_feature: Option<&Feature>,
        end_feature: Option<&Feature>,
        order: ScanOrder,
        buffer_size: u16,
    ) -> Result<Option<RangedCursor>, RPCError> {
        let start = match start_feature {
            Some(f) => {
                RangeTerm::Inclusive(EntryKey::for_schema_field_feature(schema_id, field_id, f))
            }
            None => RangeTerm::Open,
        };
        let end = match end_feature {
            Some(f) => {
                RangeTerm::Inclusive(EntryKey::for_schema_field_feature(schema_id, field_id, f))
            }
            None => RangeTerm::Open,
        };

        let range = Range {
            start,
            end,
            ordering: order.into(),
        };

        match RangedIndexerClient::seek(&self.inner, range, buffer_size, None).await? {
            Some(cursor) => Ok(Some(RangedCursor { inner: cursor })),
            None => Ok(None),
        }
    }

    /// Seek to a specific key and iterate from there
    ///
    /// # Arguments
    /// * `key` - Starting entry key
    /// * `order` - Scan direction
    /// * `buffer_size` - Number of IDs per batch
    pub async fn seek(
        &self,
        key: EntryKey,
        order: ScanOrder,
        buffer_size: u16,
    ) -> Result<Option<RangedCursor>, RPCError> {
        let range = Range::new_inclusive_opened(key, order.into());

        match RangedIndexerClient::seek(&self.inner, range, buffer_size, None).await? {
            Some(cursor) => Ok(Some(RangedCursor { inner: cursor })),
            None => Ok(None),
        }
    }

    /// Insert an entry into the ranged index
    pub async fn insert(&self, key: &EntryKey) -> Result<bool, RPCError> {
        self.inner.insert(key).await
    }

    /// Delete an entry from the ranged index
    pub async fn delete(&self, key: &EntryKey) -> Result<bool, RPCError> {
        self.inner.delete(key).await
    }

    /// Get statistics for all trees
    pub async fn stats(
        &self,
    ) -> Result<Vec<crate::index::ranged::tree::service::TreeStat>, RPCError> {
        self.inner.tree_stats().await
    }

    /// Create schema scan pattern
    fn schema_pattern(&self, schema_id: u32) -> Vec<u8> {
        use byteorder::{BigEndian, WriteBytesExt};
        use std::io::Cursor;

        let mut pattern = vec![0u8; crate::index::SCHEMA_SCAN_PATT_SIZE as usize];
        let mut cursor = Cursor::new(&mut pattern[..]);
        cursor.write_u32::<BigEndian>(schema_id).unwrap();
        // field_id = 0 for schema scan
        cursor.write_u32::<BigEndian>(0).unwrap();
        // feature = 0 for schema scan
        pattern
    }
}

/// Cursor for iterating over ranged query results
pub struct RangedCursor {
    inner: ClientCursor,
}

impl RangedCursor {
    /// Get the next document ID
    pub async fn next(&mut self) -> Result<Option<Id>, RPCError> {
        self.inner.next().await
    }

    /// Get the current document ID without advancing
    pub fn current(&self) -> Option<&Id> {
        self.inner.current()
    }

    /// Get all IDs in the current buffer
    pub fn current_block(&self) -> &Vec<Id> {
        self.inner.current_block()
    }

    /// Advance to the next block of results
    ///
    /// Returns true if there are more results
    pub async fn next_block(&mut self) -> Result<bool, RPCError> {
        self.inner.next_block().await
    }

    /// Current position in the buffer
    pub fn position(&self) -> usize {
        self.inner.pos
    }
}
