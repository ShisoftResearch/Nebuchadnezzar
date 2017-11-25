use std::sync::Arc;
use std::io;
use std::cell::{Cell as StdCell};
use bifrost::conshash::{ConsistentHashing, CHError};
use bifrost::raft::client::{RaftClient, ClientError};
use bifrost::raft;
use bifrost::rpc::{RPCError, DEFAULT_CLIENT_POOL, Server as RPCServer};
use bifrost::raft::state_machine::master::ExecError;
use bifrost::raft::state_machine::callback::server::{NotifyError};

use server::{transactions as txn_server, cell_rpc as plain_server};
use ram::types::Id;
use ram::cell::{Cell, Header, ReadError, WriteError};
use ram::schema::{sm as schema_sm};
use ram::schema::sm::client::{SMClient as SchemaClient};
use ram::schema::Schema;
use self::transaction::*;

static TRANSACTION_MAX_RETRY: u32 = 500;

pub mod transaction;

#[derive(Debug)]
pub enum NebClientError {
    RaftClientError(ClientError),
    ConsistentHashtableError(CHError)
}

pub struct Client {
    pub conshash: Arc<ConsistentHashing>,
    pub raft_client: Arc<RaftClient>,
    pub schema_client: SchemaClient
}

impl Client {
    pub fn new<'a>(subscription_server: &Arc<RPCServer>, meta_servers: &Vec<String>, group: &'a str) -> Result<Client, NebClientError> {
        match RaftClient::new(meta_servers, raft::DEFAULT_SERVICE_ID) {
            Ok(raft_client) => {
                RaftClient::prepare_subscription(subscription_server);
                assert!(RaftClient::can_callback());
                match ConsistentHashing::new_client(group, &raft_client) {
                    Ok(chash) => Ok(Client {
                        conshash: chash,
                        raft_client: raft_client.clone(),
                        schema_client: SchemaClient::new(schema_sm::generate_sm_id(group), &raft_client)
                    }),
                    Err(err) => Err(NebClientError::ConsistentHashtableError(err))
                }
            },
            Err(err) => Err(NebClientError::RaftClientError(err))
        }
    }
    pub fn locate_server_address(&self, id: &Id) -> Result<String, RPCError> {
        match self.conshash.get_server(id.higher) {
            Some(n) => Ok(n),
            None => Err(RPCError::IOError(io::Error::new(io::ErrorKind::NotFound, "cannot locate")))
        }
    }
    pub fn locate_plain_server(&self, id: &Id) -> Result<Arc<plain_server::SyncServiceClient>, RPCError> {
        let address = self.locate_server_address(id)?;
        let client = match DEFAULT_CLIENT_POOL.get(&address) {
            Ok(c) => c,
            Err(e) => return Err(RPCError::IOError(e))
        };
        Ok(plain_server::SyncServiceClient::new(plain_server::DEFAULT_SERVICE_ID, &client))
    }
    pub fn read_cell(&self, id: &Id) -> Result<Result<Cell, ReadError>, RPCError> {
        let client = self.locate_plain_server(id)?;
        client.read_cell(id)
    }
    pub fn write_cell(&self, cell: &Cell) -> Result<Result<Header, WriteError>, RPCError> {
        let client = self.locate_plain_server(&cell.id())?;
        client.write_cell(cell)
    }
    pub fn update_cell(&self, cell: &Cell) -> Result<Result<Header, WriteError>, RPCError> {
        let client = self.locate_plain_server(&cell.id())?;
        client.update_cell(cell)
    }
    pub fn remove_cell(&self, id: &Id) -> Result<Result<(), WriteError>, RPCError> {
        let client = self.locate_plain_server(id)?;
        client.remove_cell(id)
    }
    pub fn transaction<TFN, TR>(&self, func: TFN) -> Result<TR, TxnError>
        where TFN: Fn(&Transaction) -> Result<TR, TxnError> {
        let server_name = match self.conshash.rand_server() {
            Some(name) => name,
            None => return Err(TxnError::CannotFindAServer)
        };
        let txn_client = match txn_server::new_client(&server_name) {
            Ok(client) => client,
            Err(e) => return Err(TxnError::IoError(e))
        };
        let mut txn_id: txn_server::TxnId;
        let mut retried = 0;
        while retried < TRANSACTION_MAX_RETRY {
            txn_id = match txn_client.begin() {
                Ok(Ok(id)) => id,
                _ => return Err(TxnError::CannotBegin)
            };
            let txn = Transaction{
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
                },
                Err(e) => {
                    txn_result = Err(e)
                }
            }
            debug!("TXN CONCLUSION: {:?}", txn_result);
            match txn_result {
                Ok(()) => {
                    return Ok(exec_value.unwrap());
                },
                Err(TxnError::NotRealizable) => {
                    let abort_result = txn.abort();  // continue the loop to retry
                    debug!("TXN NOT REALIZABLE, ABORT: {:?}", abort_result);
                },
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
    }
    pub fn new_schema_with_id(&self, schema: &Schema) -> Result<Result<(), NotifyError>, ExecError> {
        self.schema_client.new_schema(schema)
    }
    pub fn new_schema(&self, schema: &mut Schema) -> Result<Result<(), NotifyError>, ExecError> {
        let schema_id = self.schema_client.next_id()?.unwrap();
        schema.id = schema_id;
        self.new_schema_with_id(schema)
    }
    pub fn del_schema(&self, schema_id: &String) -> Result<Result<(), NotifyError>, ExecError> {
        self.schema_client.del_schema(schema_id)
    }
    pub fn get_all_schema(&self) -> Result<Vec<Schema>, ExecError> {
        Ok(self.schema_client.get_all()?.unwrap())
    }
}