use bifrost::conshash::{CHError, ConsistentHashing};
use bifrost::membership::client::ObserverClient;
use bifrost::raft;
use bifrost::raft::client::{ClientError, RaftClient};
use bifrost::raft::state_machine::master::ExecError;
use bifrost::rpc::{RPCClient, RPCError, Server as RPCServer, DEFAULT_CLIENT_POOL};
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use itertools::Itertools;
use std::cell::Cell as StdCell;
use std::collections::HashMap;
use std::io;
use std::mem;
use std::sync::Arc;

use crate::ram::cell::{CellHeader, OwnedCell, ReadError, WriteError};
use crate::ram::schema::sm::client::SMClient as SchemaClient;
use crate::ram::schema::sm::generate_sm_id;
use crate::ram::schema::{DelSchemaError, NewSchemaError, Schema};
use crate::ram::types::Id;
use crate::server::{cell_rpc as plain_server, transactions as txn_server, CONS_HASH_ID};

use self::transaction::*;

static TRANSACTION_MAX_RETRY: u32 = 1000;

#[cfg(test)]
mod tests;
pub mod transaction;

#[derive(Debug)]
pub enum NebClientError {
    RaftClientError(ClientError),
    ConsistentHashtableError(CHError),
}

pub struct AsyncClient {
    pub conshash: Arc<ConsistentHashing>,
    pub raft_client: Arc<RaftClient>,
    pub schema_client: SchemaClient,
}

pub fn client_by_rpc_client(rpc: &Arc<RPCClient>) -> Arc<plain_server::AsyncServiceClient> {
    plain_server::AsyncServiceClient::new(plain_server::DEFAULT_SERVICE_ID, rpc)
}

impl AsyncClient {
    pub async fn new<'a>(
        subscription_server: &Arc<RPCServer>,
        membership: &Arc<ObserverClient>,
        meta_servers: &Vec<String>,
        group: &'a str,
    ) -> Result<Self, NebClientError> {
        match RaftClient::new(meta_servers, raft::DEFAULT_SERVICE_ID).await {
            Ok(raft_client) => {
                RaftClient::prepare_subscription(subscription_server).await;
                assert!(RaftClient::can_callback().await);
                match ConsistentHashing::new_client_with_id(
                    CONS_HASH_ID,
                    group,
                    &raft_client,
                    membership,
                )
                .await
                {
                    Ok(chash) => Ok(Self {
                        conshash: chash,
                        raft_client: raft_client.clone(),
                        schema_client: SchemaClient::new(generate_sm_id(group), &raft_client),
                    }),
                    Err(err) => Err(NebClientError::ConsistentHashtableError(err)),
                }
            }
            Err(err) => Err(NebClientError::RaftClientError(err)),
        }
    }
    pub fn locate_server_id(&self, id: &Id) -> Result<u64, RPCError> {
        if id.is_unit_id() {
            return Ok(0);
        }
        match self.conshash.get_server_id(id.higher) {
            Some(n) => Ok(n),
            None => Err(RPCError::IOError(io::Error::new(
                io::ErrorKind::NotFound,
                format!("cannot locate server for id {:?}", id),
            ))),
        }
    }

    pub fn client_by_server_id<'a>(
        &'a self,
        server_id: u64,
    ) -> impl Future<Output = Result<Arc<plain_server::AsyncServiceClient>, RPCError>> + 'a {
        client_by_server_id(&self.conshash, server_id)
    }

    pub async fn locate_plain_server(
        &self,
        id: Id,
    ) -> Result<Arc<plain_server::AsyncServiceClient>, RPCError> {
        let server_id = self.locate_server_id(&id).unwrap();
        debug_assert!(server_id > 0, "Have server id 0 for id {:?}", id);
        self.client_by_server_id(server_id).await
    }

    pub async fn read_cell(&self, id: Id) -> Result<Result<OwnedCell, ReadError>, RPCError> {
        let client = self.locate_plain_server(id).await?;
        client.read_cell(id).await
    }
    pub async fn read_all_cells(
        &self,
        ids: Vec<Id>,
    ) -> Result<Vec<Result<OwnedCell, ReadError>>, RPCError> {
        let mut cells_by_client = ids
            .iter()
            .dedup()
            .group_by(|id| self.locate_server_id(&id).unwrap())
            .into_iter()
            .map(|(server_id, ids)| (server_id, ids.map(|id| *id).collect_vec()))
            .map(|(server_id, ids)| async move {
                if server_id > 0 {
                    let client = self.client_by_server_id(server_id).await.unwrap();
                    (client.read_all_cells(&ids).await, ids)
                } else {
                    (
                        Ok(vec![Err(ReadError::CellIdIsUnitId)]),
                        vec![Id::unit_id()],
                    )
                }
            })
            .collect::<FuturesUnordered<_>>();
        let mut id_cell_map = HashMap::new();
        while let Some((cells, ids)) = cells_by_client.next().await {
            let cells = cells?;
            for (id, cell) in ids.into_iter().zip(cells) {
                id_cell_map.insert(id, Some(cell));
            }
        }
        Ok(ids
            .iter()
            .map(|id| {
                // Use mem::replace to avoid additional cost when hash map shriking by remove
                let id_ref = id_cell_map.get_mut(id).unwrap();
                if cfg!(debug_assertions) && id_ref.is_none() {
                    let msg = format!("Cannot find {:?} for read_all_cells", id);
                    error!("{}", msg);
                    panic!("{}", msg);
                }
                mem::replace(id_ref, None).unwrap()
            })
            .collect())
    }
    pub async fn write_cell(
        &self,
        cell: OwnedCell,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        let client = self.locate_plain_server(cell.id()).await?;
        client.write_cell(cell).await
    }
    pub async fn update_cell(
        &self,
        cell: OwnedCell,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        let client = self.locate_plain_server(cell.id()).await?;
        client.update_cell(cell).await
    }
    pub async fn upsert_cell(
        &self,
        cell: OwnedCell,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        let client = self.locate_plain_server(cell.id()).await?;
        client.upsert_cell(cell).await
    }
    pub async fn remove_cell(&self, id: Id) -> Result<Result<(), WriteError>, RPCError> {
        let client = self.locate_plain_server(id).await?;
        client.remove_cell(id).await
    }
    pub async fn count(&self) -> Result<u64, RPCError> {
        let (members, _) = self.conshash.membership().all_members(true).await.unwrap();
        let mut member_futs: FuturesUnordered<_> = members
            .into_iter()
            .map(|m| async move {
                let client = self.client_by_server_id(m.id).await?;
                Ok(client.count().await?)
            })
            .collect();
        let mut sum = 0;
        while let Some(res) = member_futs.next().await {
            sum += res?;
        }
        Ok(sum)
    }
    pub async fn transaction<'a, TFN, TR, RF>(&self, func: TFN) -> Result<TR, TxnError>
    where
        TFN: Fn(Transaction) -> RF + 'a,
        RF: Future<Output = Result<TR, TxnError>> + 'a,
    {
        let server_name = match self.conshash.rand_server() {
            Some(name) => name,
            None => return Err(TxnError::CannotFindAServer),
        };
        let txn_client = match txn_server::new_async_client(&server_name).await {
            Ok(client) => client,
            Err(e) => return Err(TxnError::IoError(e)),
        };
        let mut txn_id: txn_server::TxnId;
        let mut retried = 0;
        while retried < TRANSACTION_MAX_RETRY {
            txn_id = match txn_client.begin().await {
                Ok(Ok(id)) => id,
                _ => return Err(TxnError::CannotBegin),
            };
            let txn = Transaction {
                tid: txn_id,
                state: Arc::new(StdCell::new(txn_server::TxnState::Started)),
                client: txn_client.clone(),
            };
            let exec_result = func(txn.clone()).await;
            let mut exec_value = None;
            let mut txn_result = Ok(());
            match exec_result {
                Ok(val) => {
                    if txn.state.get() == txn_server::TxnState::Started {
                        txn_result = txn.prepare().await;
                        debug!("PREPARE STATE: {:?}", txn_result);
                    }
                    if txn_result.is_ok() && txn.state.get() == txn_server::TxnState::Prepared {
                        txn_result = txn.commit().await;
                        debug!("COMMIT STATE: {:?}", txn_result);
                    }
                    exec_value = Some(val);
                }
                Err(e) => txn_result = Err(e),
            }
            debug!("TXN CONCLUSION: {:?}", txn_result);
            match txn_result {
                Ok(()) => {
                    return Ok(exec_value.unwrap());
                }
                Err(TxnError::NotRealizable) => {
                    let abort_result = txn.abort().await; // continue the loop to retry
                    debug!("TXN NOT REALIZABLE, ABORT: {:?}", abort_result);
                }
                Err(e) => {
                    // abort will always be an error to achieve early break
                    let abort_result = txn.abort().await;
                    debug!("TXN ERROR, ABORT: {:?}", abort_result);
                    return Err(e);
                }
            }
            retried += 1;
            debug!("Client retry transaction, {:?} times", retried);
        }
        Err(TxnError::TooManyRetry)
    }
    pub async fn new_schema_with_id(
        &self,
        schema: Schema,
    ) -> Result<Result<(), NewSchemaError>, ExecError> {
        self.schema_client.new_schema(&schema).await
    }
    pub async fn new_schema(
        &self,
        mut schema: Schema,
    ) -> Result<Result<u32, NewSchemaError>, ExecError> {
        let schema_id = self.schema_client.next_id().await?;
        schema.id = schema_id;
        self.new_schema_with_id(schema)
            .await
            .map(|r| r.map(|_| schema_id))
    }
    pub async fn del_schema(&self, name: String) -> Result<Result<(), DelSchemaError>, ExecError> {
        self.schema_client.del_schema(&name).await
    }
    pub async fn get_all_schema(&self) -> Result<Vec<Schema>, ExecError> {
        self.schema_client.get_all().await
    }
}

pub async fn client_by_server_id(
    conshash: &Arc<ConsistentHashing>,
    server_id: u64,
) -> Result<Arc<plain_server::AsyncServiceClient>, RPCError> {
    DEFAULT_CLIENT_POOL
        .get_by_id(server_id, move |sid| conshash.to_server_name(sid))
        .await
        .map_err(|e| RPCError::IOError(e))
        .map(|c| client_by_rpc_client(&c))
}

pub async fn client_by_server_name(
    server_id: u64,
    server_name: String,
) -> Result<Arc<plain_server::AsyncServiceClient>, RPCError> {
    DEFAULT_CLIENT_POOL
        .get_by_id(server_id, move |sid| server_name)
        .await
        .map_err(|e| RPCError::IOError(e))
        .map(|c| client_by_rpc_client(&c))
}
