use bifrost::conshash::{CHError, ConsistentHashing};
use bifrost::raft;
use bifrost::raft::client::{ClientError, RaftClient};
use bifrost::raft::state_machine::callback::server::NotifyError;
use bifrost::raft::state_machine::master::ExecError;
use bifrost::rpc::{RPCClient, RPCError, Server as RPCServer, DEFAULT_CLIENT_POOL};
use bifrost::membership::client::ObserverClient;
use std::cell::Cell as StdCell;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use futures::prelude::*;

use crate::ram::cell::{Cell, CellHeader, ReadError, WriteError};
use crate::ram::schema::sm::client::SMClient as SchemaClient;
use crate::ram::schema::sm::generate_sm_id;
use crate::ram::schema::Schema;
use crate::ram::types::Id;
use crate::server::{cell_rpc as plain_server, transactions as txn_server, CONS_HASH_ID};

use self::transaction::*;

static TRANSACTION_MAX_RETRY: u32 = 1000;

pub mod transaction;
#[cfg(test)]
mod tests;

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
                match ConsistentHashing::new_client_with_id(CONS_HASH_ID, group, &raft_client, membership).await {
                    Ok(chash) => Ok(Self {
                        conshash: chash,
                        raft_client: raft_client.clone(),
                        schema_client: SchemaClient::new(
                            generate_sm_id(group),
                            &raft_client,
                        ),
                    }),
                    Err(err) => Err(NebClientError::ConsistentHashtableError(err)),
                }
            }
            Err(err) => Err(NebClientError::RaftClientError(err)),
        }
    }
    pub async fn locate_server_id(&self, id: &Id) -> Result<u64, RPCError> {
        match self.conshash.get_server_id(id.higher) {
            Some(n) => Ok(n),
            None => Err(RPCError::IOError(io::Error::new(
                io::ErrorKind::NotFound,
                format!("cannot locate server for id {:?}", id),
            ))),
        }
    }

    pub async fn client_by_server_id(
        &self,
        server_id: u64,
    ) -> Result<Arc<plain_server::AsyncServiceClient>, RPCError> {
        DEFAULT_CLIENT_POOL
            .get_by_id(server_id, move |sid| self.conshash.to_server_name(sid)).await
            .map_err(|e| RPCError::IOError(e))
            .map(|c| client_by_rpc_client(&c))
    }

    pub async fn locate_plain_server(
        &self,
        id: Id,
    ) -> Result<Arc<plain_server::AsyncServiceClient>, RPCError> {
        let server_id = self.locate_server_id(&id).await.unwrap();
        self.client_by_server_id(server_id).await
    }

    pub async fn read_cell(&self, id: Id) -> Result<Result<Cell, ReadError>, RPCError> {
        let client = self.locate_plain_server(id).await?;
        client.read_cell(id).await
    }
    pub async fn write_cell(
        &self,
        cell: Cell,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        let client = self.locate_plain_server(cell.id()).await?;
        client.write_cell(cell).await
    }
    pub async fn update_cell(
        &self,
        cell: Cell,
    ) -> Result<Result<CellHeader, WriteError>, RPCError> {
        let client = self.locate_plain_server(cell.id()).await?;
        client.update_cell(cell).await
    }
    pub async fn upsert_cell(
        &self,
        cell: Cell,
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
            .map(|m| {
                async move {
                    let client = self.client_by_server_id(m.id).await?;
                    Ok(client.count().await?)
                }
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
        RF: Future<Output = Result<TR, TxnError>> + 'a
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
    ) -> Result<Result<(), NotifyError>, ExecError> {
        self.schema_client.new_schema(&schema).await
    }
    pub async fn new_schema(
        &self,
        mut schema: Schema,
    ) -> Result<(u32, Option<NotifyError>), ExecError> {
        let schema_id = self.schema_client.next_id().await?;
        schema.id = schema_id;
        self.new_schema_with_id(schema).await.map(|r| {
            let error = match r {
                Ok(_) => None,
                Err(e) => Some(e),
            };
            (schema_id, error)
        })
    }
    pub async fn del_schema(&self, name: String) -> Result<Result<(), NotifyError>, ExecError> {
        self.schema_client.del_schema(&name).await
    }
    pub async fn get_all_schema(&self) -> Result<Vec<Schema>, ExecError> {
        self.schema_client.get_all().await
    }
}