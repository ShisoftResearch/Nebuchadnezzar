use bifrost::conshash::{CHError, ConsistentHashing};
use bifrost::raft;
use bifrost::raft::client::{ClientError, RaftClient};
use bifrost::raft::state_machine::callback::server::NotifyError;
use bifrost::raft::state_machine::master::ExecError;
use bifrost::rpc::{RPCClient, RPCError, Server as RPCServer, DEFAULT_CLIENT_POOL};
use std::cell::Cell as StdCell;
use std::io;
use std::sync::Arc;
use futures::future::BoxFuture;
use futures::Future;

use crate::ram::cell::{Cell, CellHeader, ReadError, WriteError};
use crate::ram::schema::client::SMClient as SchemaClient;
use crate::ram::schema::Schema;
use crate::ram::types::Id;
use crate::server::{cell_rpc as plain_server, transactions as txn_server, CONS_HASH_ID};

use self::transaction::*;

static TRANSACTION_MAX_RETRY: u32 = 1000;

pub mod transaction;

#[derive(Debug)]
pub enum NebClientError {
    RaftClientError(ClientError),
    ConsistentHashtableError(CHError),
}

struct AsyncClient {
    pub conshash: Arc<ConsistentHashing>,
    pub raft_client: Arc<RaftClient>,
    pub schema_client: SchemaClient,
}

pub fn client_by_rpc_client(rpc: &Arc<RPCClient>) -> Arc<plain_server::AsyncServiceClient> {
    plain_server::AsyncServiceClient::new(plain_server::DEFAULT_SERVICE_ID, rpc)
}

impl AsyncClient {
    pub fn new<'a>(
        subscription_server: &Arc<RPCServer>,
        meta_servers: &Vec<String>,
        group: &'a str,
    ) -> Result<Self, NebClientError> {
        match RaftClient::new(meta_servers, raft::DEFAULT_SERVICE_ID) {
            Ok(raft_client) => {
                RaftClient::prepare_subscription(subscription_server);
                assert!(RaftClient::can_callback());
                match ConsistentHashing::new_client_with_id(CONS_HASH_ID, group, &raft_client) {
                    Ok(chash) => Ok(Self {
                        conshash: chash,
                        raft_client: raft_client.clone(),
                        schema_client: SchemaClient::new(
                            schema_sm::generate_sm_id(group),
                            &raft_client,
                        ),
                    }),
                    Err(err) => Err(NebClientError::ConsistentHashtableError(err)),
                }
            }
            Err(err) => Err(NebClientError::RaftClientError(err)),
        }
    }
    pub fn locate_server_id(&self, id: &Id) -> Result<u64, RPCError> {
        match self.conshash.get_server_id(id.higher) {
            Some(n) => Ok(n),
            None => Err(RPCError::IOError(io::Error::new(
                io::ErrorKind::NotFound,
                format!("cannot locate server for id {:?}", id),
            ))),
        }
    }

    pub fn client_by_server_id(
        this: Arc<Self>,
        server_id: u64,
    ) -> impl Future<Item = Arc<plain_server::AsyncServiceClient>, Error = RPCError> {
        DEFAULT_CLIENT_POOL
            .get_by_id_async(server_id, move |sid| this.conshash.to_server_name(sid))
            .map_err(|e| RPCError::IOError(e))
            .map(|c| client_by_rpc_client(&c))
    }

    pub fn locate_plain_server(
        this: Arc<Self>,
        id: Id,
    ) -> impl Future<Item = Arc<plain_server::AsyncServiceClient>, Error = RPCError> {
        let server_id = this.locate_server_id(&id).unwrap();
        Self::client_by_server_id(this, server_id)
    }
}

impl Service for AsyncClient {
    fn read_cell(&self, id: Id) -> BoxFuture<Result<Result<Cell, ReadError>, RPCError>> {
        async {
            let client = self.locate_plain_server(id).await?;
            client.read_cell(id).await
        }.boxed()
    }
    fn write_cell(
        &self,
        cell: Cell,
    ) -> BoxFuture<Result<Result<CellHeader, WriteError>, RPCError>> {
        async {
            let client = self.locate_plain_server(cell.id()).await?;
            client.write_cell(cell).await
        }.boxed()
    }
    fn update_cell(
        &self,
        cell: Cell,
    ) -> BoxFuture<Result<Result<CellHeader, WriteError>, RPCError>> {
        async {
            let client = self.locate_plain_server(cell.id()).await?;
            client.update_cell(cell).await
        }.boxed()
    }
    fn upsert_cell(
        &self,
        cell: Cell,
    ) -> BoxFuture<Result<Result<CellHeader, WriteError>, RPCError>> {
        async {let client = self::locate_plain_server(cell.id()).await?;
            client.upsert_cell(cell).await
        }.boxed()
    }
    fn remove_cell(&self, id: Id) -> BoxFuture<Result<Result<(), WriteError>, RPCError>> {
        async {
            let client = self.locate_plain_server(this, id).await?;
            client.remove_cell(id).await
        }.boxed()
    }
    fn count(&self) -> BoxFuture<Result<u64, RPCError>> {
        async {
            let (members, _) = self.conshash.membership().all_members(true).await
                .map_err(|e| RPCError::IOError(io::Error::new(io::ErrorKind::Other, e)))?
                .unwrap();
            let mut sum = 0;
            for m in members {
                let client = self.client_by_server_id(m.id)?;
                let count = client.count().await?.unwrap();
                sum += count
            }
            Ok(sum)
        }
    }
    fn transaction<TFN, TR>(&self, func: TFN) -> BoxFuture<Result<TR, TxnError>>
    where
        TFN: Fn(&Transaction) -> Result<TR, TxnError>,
        TR: 'static,
        TFN: 'static,
    {
        async {
            let server_name = match this.conshash.rand_server() {
                Some(name) => name,
                None => return Err(TxnError::CannotFindAServer),
            };
            let txn_client = match txn_server::new_async_client(&server_name) {
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
                    state: StdCell::new(txn_server::TxnState::Started),
                    client: txn_client.clone(),
                };
                let exec_result = func(&txn);
                let mut exec_value = None;
                let mut txn_result = Ok(());
                match exec_result {
                    Ok(val) => {
                        if txn.state.get() == txn_server::TxnState::Started {
                            txn_result = txn.prepare();
                            debug!("PREPARE STATE: {:?}", txn_result);
                        }
                        if txn_result.is_ok() && txn.state.get() == txn_server::TxnState::Prepared {
                            txn_result = txn.commit();
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
                        let abort_result = txn.abort(); // continue the loop to retry
                        debug!("TXN NOT REALIZABLE, ABORT: {:?}", abort_result);
                    }
                    Err(e) => {
                        // abort will always be an error to achieve early break
                        let abort_result = txn.abort();
                        debug!("TXN ERROR, ABORT: {:?}", abort_result);
                        return Err(e);
                    }
                }
                retried += 1;
                debug!("Client retry transaction, {:?} times", retried);
            }
            Err(TxnError::TooManyRetry)
        }.boxed()
    }
    fn new_schema_with_id(
        &self,
        schema: Schema,
    ) -> Result<Result<(), NotifyError>, ExecError> {
        self.schema_client.new_schema(&schema).boxed()
    }
    fn new_schema(
        &self,
        mut schema: Schema,
    ) -> BoxFuture<Result<(u32, Option<NotifyError>), ExecError>> {
        async {
            let schema_id = self.schema_client.next_id().await?.unwrap();
            schema.id = schema_id;
            self.new_schema_with_id(schema).await.map(|r| {
                let error = match r {
                    Ok(_) => None,
                    Err(e) => Some(e),
                };
                (schema_id, error)
            })
        }.boxed()
    }
    fn del_schema(&self, name: String) -> BoxFuture<Result<Result<(), NotifyError>, ExecError>> {
        self.schema_client.del_schema(&name).boxed()
    }
    fn get_all_schema(&self) -> BoxFuture<Result<Vec<Schema>, ExecError>> {
        async {
            Ok(self.schema_client.get_all().await?.unwrap())
        }.boxed()
    }
}