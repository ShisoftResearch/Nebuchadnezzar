use std::{mem, sync::Arc};

use bifrost::{conshash::ConsistentHashing, raft::client::RaftClient, rpc::RPCError};
use dovahkiin::{
    expr::serde::Expr,
    types::{OwnedValue, SharedValue},
};
use futures::stream::{FuturesUnordered, StreamExt};
use itertools::Itertools;

use crate::{
    client::{client_by_server_id, client_by_server_name},
    index::{
        entry::{MAX_FEATURE, MIN_FEATURE},
        ranged::{client::cursor::ClientCursor, lsm::btree::Ordering},
        EntryKey, IndexerClients, SCHEMA_SCAN_PATT_SIZE,
    },
    ram::cell::{OwnedCell, ReadError},
};

const SCAN_BUFFER_SIZE: u16 = 64;

pub struct IndexedDataClient {
    conshash: Arc<ConsistentHashing>,
    index_clients: Arc<IndexerClients>,
}

pub struct DataCursor<'a> {
    index_cursor: Option<ClientCursor>,
    buffer: Vec<OwnedCell>,
    projection: Vec<u64>,
    selection: Expr,
    proc: Expr,
    client: &'a IndexedDataClient,
    pos: usize,
}

#[derive(PartialEq, Eq, Serialize, Deserialize, Clone, Copy)]
pub enum Comparator {
    Eq,
    Less,
    LessEq,
    Greater,
    GreaterEq,
}

impl IndexedDataClient {
    pub fn new(conshash: &Arc<ConsistentHashing>, raft_client: &Arc<RaftClient>) -> Self {
        Self {
            conshash: conshash.clone(),
            index_clients: Arc::new(IndexerClients::new(conshash, raft_client)),
        }
    }
    pub async fn range_index_scan<'a, 'b>(
        &'a self,
        schema: u32,
        field: u64,
        key: SharedValue<'b>,
        projection: Vec<u64>, // Column array
        selection: Expr,      // Checker expression
        proc: Expr,
        comp: Comparator,
        ordering: Ordering,
    ) -> Result<DataCursor<'a>, RPCError> {
        let schema_lower_bound = EntryKey::for_schema_field_feature(schema, field, &MIN_FEATURE);
        let schema_higher_bound = EntryKey::for_schema_field_feature(schema, field, &MAX_FEATURE);
        let schema_search_key = EntryKey::for_schema_field_feature(schema, field, &key.feature());
        let end_key;
        let start_key;
        match (comp, ordering) {
            (Comparator::Eq, _) => {
                start_key = schema_search_key.clone();
                end_key = schema_search_key;
            }
            (Comparator::Less, Ordering::Forward) => {
                start_key = schema_lower_bound;
                end_key = schema_search_key.less();
            }
            (Comparator::Less, Ordering::Backward) => {
                start_key = schema_search_key.less();
                end_key = schema_lower_bound;
            }
            (Comparator::LessEq, Ordering::Forward) => {
                start_key = schema_lower_bound;
                end_key = schema_search_key;
            }
            (Comparator::LessEq, Ordering::Backward) => {
                start_key = schema_search_key;
                end_key = schema_lower_bound;
            }
            (Comparator::Greater, Ordering::Forward) => {
                start_key = schema_search_key.greater();
                end_key = schema_higher_bound;
            }
            (Comparator::Greater, Ordering::Backward) => {
                start_key = schema_higher_bound;
                end_key = schema_search_key.greater();
            }
            (Comparator::GreaterEq, Ordering::Forward) => {
                start_key = schema_search_key;
                end_key = schema_higher_bound;
            }
            (Comparator::GreaterEq, Ordering::Backward) => {
                start_key = schema_higher_bound;
                end_key = schema_search_key;
            }
        }
        let index_cursor = self.index_clients.range_seek(
            &start_key,
            ordering,
            SCAN_BUFFER_SIZE,
            None,
            Some(end_key),
        ).await?;
        Ok(self.new_cursor(index_cursor, projection, selection, proc).await)
    }
    pub async fn scan_all<'a>(
        &'a self,
        schema: u32,
        projection: Vec<u64>, // Column array
        selection: Expr,      // Checker expression
        proc: Expr,
        ordering: Ordering,
    ) -> Result<DataCursor<'a>, RPCError> {
        let key = EntryKey::for_schema(schema);
        let index_cursor = self
            .index_clients
            .range_seek(
                &key,
                ordering,
                SCAN_BUFFER_SIZE,
                Some(SCHEMA_SCAN_PATT_SIZE),
                None,
            )
            .await?;
        Ok(self.new_cursor(index_cursor, projection, selection, proc).await)
    }
    async fn new_cursor<'a>(
        &'a self,
        index_cursor: Option<ClientCursor>,
        projection: Vec<u64>,
        selection: Expr,
        proc: Expr,
    ) -> DataCursor<'a> {
        let mut cursor = DataCursor {
            index_cursor,
            projection,
            selection,
            proc,
            client: self,
            buffer: vec![],
            pos: 0,
        };
        cursor.refresh_batch().await;
        cursor
    }
}

impl<'a> DataCursor<'a> {
    pub async fn next(&mut self) -> Result<Option<OwnedCell>, RPCError> {
        if self.buffer.len() <= self.pos {
            if self.next_block().await? {
                if !self.refresh_batch().await {
                    return Ok(None);
                }
            } else {
                return Ok(None);
            }
        }
        if self.buffer.is_empty() {
            return Ok(None);
        }
        let cell = mem::take(&mut self.buffer[self.pos]);
        self.pos += 1;
        return Ok(Some(cell));
    }

    pub async fn next_block(&mut self) -> Result<bool, RPCError> {
        if let Some(cursor) = &mut self.index_cursor {
            if cursor.next_block().await? {
                return Ok(true);
            }
        }
        // If cannot get next block, set the index cursor to none
        // We are done with this cursor
        self.index_cursor = None;
        self.buffer = vec![];
        self.pos = 0;
        return Ok(false);
    }

    pub async fn refresh_batch(&mut self) -> bool {
        if self.index_cursor.is_some() {
            let mut all_cells = vec![];
            loop {
                {
                    let cursor = self.index_cursor.as_ref().unwrap();
                    let mut tasks = cursor
                        .current_block()
                        .iter()
                        .enumerate()
                        .filter_map(|(i, id)| {
                            self.client
                                .conshash
                                .get_server_id_by(id)
                                .map(|sid| (i, sid, id))
                        })
                        .group_by(|(_i, sid, _id)| *sid)
                        .into_iter()
                        .map(|(sid, pairs)| {
                            let mut ids = vec![];
                            let mut idx = vec![];
                            for (i, _, id) in pairs {
                                idx.push(i);
                                ids.push(*id);
                            }
                            let projection = self.projection.clone();
                            let selection = self.selection.clone();
                            let proc = self.proc.clone();
                            let server_name = self.client.conshash.to_server_name(sid);
                            async move {
                                match client_by_server_name(sid, server_name).await {
                                    Ok(client) => {
                                        let read_res = client
                                            .read_all_cells_proced(ids, projection, selection, proc)
                                            .await
                                            .map(|v| v.into_iter().zip(idx).collect_vec());
                                        match read_res {
                                            Ok(cells) => Ok(cells
                                                .into_iter()
                                                .filter_map(|(c, i)| c.ok().map(|c| (c, i)))
                                                .collect_vec()),
                                            Err(e) => return Err(e),
                                        }
                                    }
                                    Err(e) => return Err(e),
                                }
                            }
                        })
                        .collect::<FuturesUnordered<_>>();
                    while let Some(task_res) = tasks.next().await {
                        if let Ok(mut cells) = task_res {
                            all_cells.append(&mut cells);
                        }
                    }
                }
                if !all_cells.is_empty() {
                    break;
                } else {
                    let cursor = self.index_cursor.as_mut().unwrap();
                    match cursor.next_block().await {
                        Ok(true) => continue,
                        _ => {
                            self.buffer = vec![];
                            self.pos = 0;
                            return false;
                        }
                    }
                }
            }
            all_cells.sort_by(|(_, i1), (_, i2)| i1.cmp(i2));
            self.buffer = all_cells.into_iter().map(|(c, _)| c).collect_vec();
            self.pos = 0;
            true
        } else {
            self.buffer = vec![];
            self.pos = 0;
            false
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        index::ranged::lsm::btree::Ordering,
        ram::{
            cell::OwnedCell,
            schema::{Field, IndexType, Schema},
        },
        server::*,
    };
    use bifrost_hasher::hash_str;
    use dovahkiin::{expr::serde::Expr, integrated::lisp::*, types::*};

    #[tokio::test(flavor = "multi_thread")]
    async fn scan_all() {
        const DATA_1: &'static str = "DATA_1";
        const DATA_2: &'static str = "DATA_2";
        let _ = env_logger::try_init();
        let server_addr = String::from("127.0.0.1:6701");
        let server_group = String::from("indexed_scan_all_test");
        let server = NebServer::new_from_opts(
            &ServerOptions {
                chunk_count: 8,
                memory_size: 512 * 1024 * 1024,
                backup_storage: None,
                wal_storage: None,
                index_enabled: true,
                services: vec![Service::Cell, Service::Query],
            },
            &server_addr,
            &server_group,
        )
        .await;
        // Require schema to be scannable to insert special scan key to the range indexer
        let fields = Field::new(
            "*",
            Type::Map,
            false,
            false,
            Some(vec![
                Field::new(
                    DATA_1,
                    Type::U64,
                    false,
                    false,
                    None,
                    vec![IndexType::Ranged],
                ),
                Field::new(DATA_2, Type::U32, false, false, None, vec![]),
            ]),
            vec![],
        );
        let schema_id_1 = 123;
        let schema_id_2 = 234;
        let schema_1 = Schema::new_with_id(
            schema_id_1,
            &String::from("schema_1"),
            None,
            fields.clone(),
            false,
            true, // Scannable
        );
        let schema_2 = Schema::new_with_id(
            schema_id_2,
            &String::from("schema_2"),
            None,
            fields,
            false,
            true, // Scannable
        );
        let client = server.data_client(&vec![server_addr]).await.unwrap();
        client.new_schema_with_id(schema_1).await.unwrap().unwrap();
        client.new_schema_with_id(schema_2).await.unwrap().unwrap();
        let num = 1024;
        for i in 0..num {
            let id = Id::new(1, i);
            let mut value = OwnedValue::Map(OwnedMap::new());
            value[DATA_1] = OwnedValue::U64(i);
            value[DATA_2] = OwnedValue::U32((i * 2) as u32);
            let cell = OwnedCell::new_with_id(schema_id_1, &id, value);
            client.write_cell(cell).await.unwrap().unwrap();
        }
        let idx_data_client = server.indexed_data_client();
        {
            let mut cursor = idx_data_client
                .scan_all(
                    schema_id_1,
                    vec![],
                    Expr::nothing(),
                    Expr::nothing(),
                    Ordering::Forward,
                )
                .await
                .unwrap();
            for i in 0..num {
                let id = Id::new(1, i);
                let cell = cursor.next().await.unwrap().unwrap();
                assert_eq!(id, cell.id());
                assert_eq!(*cell[DATA_1].u64().unwrap(), i);
                assert_eq!(*cell[DATA_2].u32().unwrap(), (i * 2) as u32);
                debug!("Checked cell id {:?} from index", id);
            }
            let out_of_range_item = cursor.next().await.unwrap();
            if let Some(cell) = out_of_range_item {
                panic!("Should not have any more cell. Got id {:?}", cell.id());
            }
        }
        for i in 0..num {
            let id = Id::new(2, i);
            let mut value = OwnedValue::Map(OwnedMap::new());
            value[DATA_1] = OwnedValue::U64(i);
            value[DATA_2] = OwnedValue::U32((i * 3) as u32);
            let cell = OwnedCell::new_with_id(schema_id_2, &id, value);
            client.write_cell(cell).await.unwrap().unwrap();
        }
        {
            let mut cursor = idx_data_client
                .scan_all(
                    schema_id_2,
                    vec![],
                    Expr::nothing(),
                    Expr::nothing(),
                    Ordering::Forward,
                )
                .await
                .unwrap();
            for i in 0..num {
                let id = Id::new(2, i);
                let cell = cursor.next().await.unwrap().unwrap();
                assert_eq!(id, cell.id());
                assert_eq!(*cell[DATA_1].u64().unwrap(), i);
                assert_eq!(*cell[DATA_2].u32().unwrap(), (i * 3) as u32);
                debug!("Checked cell id {:?} from index", id);
            }
            let out_of_range_item = cursor.next().await.unwrap();
            if let Some(cell) = out_of_range_item {
                panic!("Should not have any more cell. Got id {:?}", cell.id());
            }
        }
        {
            let mut cursor = idx_data_client
                .scan_all(
                    schema_id_1,
                    vec![],
                    Expr::nothing(),
                    Expr::nothing(),
                    Ordering::Forward,
                )
                .await
                .unwrap();
            for i in 0..num {
                let id = Id::new(1, i);
                let cell = cursor.next().await.unwrap().unwrap();
                assert_eq!(id, cell.id());
            }
        }
        {
            // Testing selection
            let select_expr = parse_to_serde_expr("(and (>= DATA_1 10u64) (< DATA_1 100u64))")
                .unwrap()[0]
                .clone();
            let mut cursor = idx_data_client
                .scan_all(
                    schema_id_1,
                    vec![],
                    select_expr,
                    Expr::nothing(),
                    Ordering::Forward,
                )
                .await
                .unwrap();
            // Start from 10 to 100 due to the selection expression
            for i in 10..100 {
                let id = Id::new(1, i);
                let cell = cursor.next().await.unwrap().unwrap();
                assert_eq!(id, cell.id());
                assert_eq!(*cell[DATA_1].u64().unwrap(), i);
                assert_eq!(*cell[DATA_2].u32().unwrap(), (i * 2) as u32);
                debug!("Checked cell id {:?} from index", id);
            }
            let out_of_range_item = cursor.next().await.unwrap();
            if let Some(cell) = out_of_range_item {
                panic!("Should not have any more cell. Got id {:?}", cell.id());
            }
        }
        {
            info!("Testing selection 2");
            let select_expr = parse_to_serde_expr("(or (= DATA_1 100u64) (= DATA_1 1000u64))")
                .unwrap()[0]
                .clone();
            let mut cursor = idx_data_client
                .scan_all(
                    schema_id_1,
                    vec![],
                    select_expr,
                    Expr::nothing(),
                    Ordering::Forward,
                )
                .await
                .unwrap();
            // 100 and 1000 due to the selection expression
            for i in vec![100, 1000] {
                let id = Id::new(1, i);
                let cell = cursor.next().await.unwrap().unwrap();
                assert_eq!(id, cell.id());
                assert_eq!(*cell[DATA_1].u64().unwrap(), i);
                assert_eq!(*cell[DATA_2].u32().unwrap(), (i * 2) as u32);
                debug!("-> Checked cell id {:?} from index", id);
            }
            let out_of_range_item = cursor.next().await.unwrap();
            if let Some(cell) = out_of_range_item {
                panic!("Should not have any more cell. Got id {:?}", cell.id());
            }
        }
        {
            info!("Testing processing");
            let proc_expr = parse_to_serde_expr("(+ DATA_1 (u64 DATA_2))").unwrap()[0].clone();
            let mut cursor = idx_data_client
                .scan_all(
                    schema_id_1,
                    vec![],
                    Expr::nothing(),
                    proc_expr,
                    Ordering::Forward,
                )
                .await
                .unwrap();
            for i in 0..num {
                let id = Id::new(1, i);
                let cell = cursor.next().await.unwrap().unwrap();
                assert_eq!(id, cell.id());
                assert_eq!(*cell.data.u64().unwrap(), i + (i * 2));
                debug!("-> Checked cell id {:?} from index", id);
            }
            let out_of_range_item = cursor.next().await.unwrap();
            if let Some(cell) = out_of_range_item {
                panic!("Should not have any more cell. Got id {:?}", cell.id());
            }
        }
    }
}
