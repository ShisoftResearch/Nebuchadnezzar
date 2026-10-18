//! Ranged index query client for distributed range queries
//!
//! This module provides `RangedClient` for performing distributed range queries
//! across the Nebuchadnezzar cluster's B-tree based ranged index.

use crate::index::verification::Verify;
use std::sync::Arc;

use bifrost::conshash::ConsistentHashing;
use bifrost::raft::client::AsRaftPlaneClient;
use bifrost::rpc::RPCError;

use crate::index::entry::EntryKey;
use crate::index::ranged::client::cursor::ClientCursor;
use crate::index::ranged::client::RangedIndexerClient;
use crate::index::ranged::tree::btree::Ordering;
use crate::index::ranged::tree::service::Range;
use crate::index::Feature;
use crate::ram::schema::SchemaUid;
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
/// let mut cursor = ranged.scan_schema(schema_id, 100, Verify::Exists).await?;
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
///     Verify::Exists,
/// ).await?;
/// ```
pub struct RangedClient {
    inner: Arc<RangedIndexerClient>,
    reader: crate::query::data_client::read::CellReader,
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
    pub fn new<C>(conshash: Arc<ConsistentHashing>, raft_client: Arc<C>, group_name: &str) -> Self
    where
        C: AsRaftPlaneClient + 'static,
    {
        Self {
            reader: crate::query::data_client::read::CellReader::new(
                conshash.clone(),
                group_name,
                group_name,
            ),
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
            reader: crate::query::data_client::read::CellReader::new(
                conshash.clone(),
                group_name,
                database_name,
            ),
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
        schema_id: SchemaUid,
        buffer_size: u16,
        verify: Verify,
    ) -> Result<Option<RangedCursor>, RPCError> {
        let key = EntryKey::for_schema(schema_id);
        let range = Range::new_inclusive_opened(key, Ordering::Forward);
        let pattern = Some(self.schema_pattern(schema_id));

        match RangedIndexerClient::seek(&self.inner, range, buffer_size, pattern).await? {
            Some(cursor) => Ok(Some(
                RangedCursor::new(
                    cursor,
                    self.reader.clone(),
                    vec![crate::index::verification::IndexCheck::Schema(schema_id)],
                    verify,
                )
                .await?,
            )),
            None => Ok(None),
        }
    }

    /// Scan all documents in a schema in specified order
    pub async fn scan_schema_ordered(
        &self,
        schema_id: SchemaUid,
        order: ScanOrder,
        buffer_size: u16,
        verify: Verify,
    ) -> Result<Option<RangedCursor>, RPCError> {
        let key = EntryKey::for_schema(schema_id);
        let range = Range::new_inclusive_opened(key, order.into());
        let pattern = Some(self.schema_pattern(schema_id));

        match RangedIndexerClient::seek(&self.inner, range, buffer_size, pattern).await? {
            Some(cursor) => Ok(Some(
                RangedCursor::new(
                    cursor,
                    self.reader.clone(),
                    vec![crate::index::verification::IndexCheck::Schema(schema_id)],
                    verify,
                )
                .await?,
            )),
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
        schema_id: SchemaUid,
        field_id: u64,
        start_feature: Option<&Feature>,
        end_feature: Option<&Feature>,
        order: ScanOrder,
        buffer_size: u16,
        verify: Verify,
    ) -> Result<Option<RangedCursor>, RPCError> {
        let range = crate::query::data_client::ValueRange {
            start: start_feature
                .map(|feature| crate::query::data_client::ValueRangeTerm::Inclusive(*feature))
                .unwrap_or(crate::query::data_client::ValueRangeTerm::Open),
            end: end_feature
                .map(|feature| crate::query::data_client::ValueRangeTerm::Inclusive(*feature))
                .unwrap_or(crate::query::data_client::ValueRangeTerm::Open),
        }
        .to_key_range(schema_id, field_id, order.into());
        let checks = vec![
            crate::index::verification::IndexCheck::Schema(schema_id),
            crate::index::verification::IndexCheck::Ranged(range.clone()),
        ];

        match RangedIndexerClient::seek(&self.inner, range, buffer_size, None).await? {
            Some(cursor) => Ok(Some(
                RangedCursor::new(cursor, self.reader.clone(), checks, verify).await?,
            )),
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
        verify: Verify,
    ) -> Result<Option<RangedCursor>, RPCError> {
        let range = Range::new_inclusive_opened(key, order.into());
        let checks = vec![crate::index::verification::IndexCheck::Ranged(
            range.clone(),
        )];

        match RangedIndexerClient::seek(&self.inner, range, buffer_size, None).await? {
            Some(cursor) => Ok(Some(
                RangedCursor::new(cursor, self.reader.clone(), checks, verify).await?,
            )),
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
    fn schema_pattern(&self, schema_id: SchemaUid) -> Vec<u8> {
        use byteorder::{BigEndian, WriteBytesExt};
        use std::io::Cursor;

        let mut pattern = vec![0u8; crate::index::SCHEMA_SCAN_PATT_SIZE as usize];
        let mut cursor = Cursor::new(&mut pattern[..]);
        cursor.write_u32::<BigEndian>(schema_id.get()).unwrap();
        // field_id = 0 for schema scan
        cursor.write_u32::<BigEndian>(0).unwrap();
        // feature = 0 for schema scan
        pattern
    }
}

/// Cursor for iterating over ranged query results
pub struct RangedCursor {
    inner: ClientCursor,
    reader: crate::query::data_client::read::CellReader,
    checks: Vec<crate::index::verification::IndexCheck>,
    verify: Verify,
    ids: Vec<Id>,
    cells: Vec<Option<crate::ram::cell::OwnedCell>>,
    pos: usize,
}

impl RangedCursor {
    pub(crate) async fn new(
        inner: ClientCursor,
        reader: crate::query::data_client::read::CellReader,
        checks: Vec<crate::index::verification::IndexCheck>,
        verify: Verify,
    ) -> Result<Self, RPCError> {
        let mut cursor = Self {
            inner,
            reader,
            checks,
            verify,
            ids: Vec::new(),
            cells: Vec::new(),
            pos: 0,
        };
        cursor.refresh().await?;
        Ok(cursor)
    }

    async fn refresh(&mut self) -> Result<bool, RPCError> {
        loop {
            if self.verify == Verify::Exists {
                self.ids = self.reader.existing_ids(self.inner.current_block()).await?;
                self.cells.clear();
            } else {
                let cells = self
                    .reader
                    .read_cells_from_ids_checked(
                        self.inner.current_block(),
                        &vec![],
                        &dovahkiin::expr::serde::Expr::nothing(),
                        &dovahkiin::expr::serde::Expr::nothing(),
                        &self.checks,
                    )
                    .await;
                self.ids = cells.iter().map(|cell| cell.id()).collect();
                self.cells = cells.into_iter().map(Some).collect();
            }
            self.pos = 0;
            if !self.ids.is_empty() {
                return Ok(true);
            }
            if !self.inner.next_block().await? {
                return Ok(false);
            }
        }
    }

    pub async fn next(&mut self) -> Result<Option<Id>, RPCError> {
        if self.pos >= self.ids.len() && !self.next_block().await? {
            return Ok(None);
        }
        let id = self.ids.get(self.pos).copied();
        self.pos += usize::from(id.is_some());
        Ok(id)
    }

    /// Return a cell that passed predicate verification.
    pub async fn next_cell(&mut self) -> Result<Option<crate::ram::cell::OwnedCell>, RPCError> {
        loop {
            if self.pos >= self.ids.len() && !self.next_block().await? {
                return Ok(None);
            }
            let index = self.pos;
            self.pos += 1;
            if self.verify == Verify::Predicate {
                return Ok(self.cells.get_mut(index).and_then(Option::take));
            }
            // Cell results always verify the predicate, even on an existence cursor.
            let mut cells = self
                .reader
                .read_cells_from_ids_checked(
                    &[self.ids[index]],
                    &vec![],
                    &dovahkiin::expr::serde::Expr::nothing(),
                    &dovahkiin::expr::serde::Expr::nothing(),
                    &self.checks,
                )
                .await;
            if let Some(cell) = cells.pop() {
                return Ok(Some(cell));
            }
        }
    }

    pub fn current(&self) -> Option<&Id> {
        self.ids.get(self.pos)
    }
    pub fn current_block(&self) -> &Vec<Id> {
        &self.ids
    }

    pub async fn next_block(&mut self) -> Result<bool, RPCError> {
        self.ids.clear();
        self.cells.clear();
        self.pos = 0;
        if !self.inner.next_block().await? {
            return Ok(false);
        }
        self.refresh().await
    }

    pub fn position(&self) -> usize {
        self.pos
    }
}
