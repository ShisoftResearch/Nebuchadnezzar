use std::sync::Arc;
use std::io;
use bifrost::conshash::{ConsistentHashing, CHError};
use bifrost::raft::client::{RaftClient, ClientError};
use bifrost::raft;
use bifrost::rpc::{RPCError, DEFAULT_CLIENT_POOL};
use bifrost::raft::state_machine::master::ExecError;

use server::{transactions as txn_server, cell_rpc as plain_server};
use ram::types::Id;
use ram::cell::{Cell, ReadError, WriteError};
use ram::schema::sm::client::{SMClient as SchemaClient};
use ram::schema::sm::{DEFAULT_SM_ID};
use ram::schema::Schema;
use self::transaction::*;

static TRANSACTION_MAX_RETRY: u32 = 50;

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
    pub fn new(meta_servers: &Vec<String>, group: &String) -> Result<Client, NebClientError> {
        match RaftClient::new(meta_servers, raft::DEFAULT_SERVICE_ID) {
            Ok(raft_client) => {
                match ConsistentHashing::new_client(group, &raft_client) {
                    Ok(chash) => Ok(Client {
                        conshash: chash,
                        raft_client: raft_client.clone(),
                        schema_client: SchemaClient::new(DEFAULT_SM_ID, &raft_client)
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
    fn write_cell(&self, cell: &Cell) -> Result<Result<Cell, WriteError>, RPCError> {
        let client = self.locate_plain_server(&cell.id())?;
        client.write_cell(cell)
    }
    fn update_cell(&self, cell: &Cell) -> Result<Result<Cell, WriteError>, RPCError> {
        let client = self.locate_plain_server(&cell.id())?;
        client.update_cell(cell)
    }
    fn remove_cell(&self, id: &Id) -> Result<Result<(), WriteError>, RPCError> {
        let client = self.locate_plain_server(id)?;
        client.remove_cell(id)
    }
    fn transaction<TFN>(&self, mut func: TFN) -> Result<(), TxnError>
        where TFN: FnMut(&mut Transaction) -> Result<(), TxnError> {
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
            let mut txn = Transaction{
                tid: txn_id,
                state: txn_server::TxnState::Started,
                client: txn_client.clone()
            };
            let mut exec_result = func(&mut txn);
            if exec_result.is_ok() && txn.state == txn_server::TxnState::Started {
                exec_result = txn.prepare();
            }
            if exec_result.is_ok() && txn.state == txn_server::TxnState::Prepared {
                exec_result = txn.commit();
            }
            match exec_result {
                Ok(()) => {
                    return Ok(());
                },
                Err(TxnError::NotRealizable) =>{
                    txn.abort();  // continue the loop to retry
                },
                Err(e) => {
                    txn.abort()?; // abort will always be an error to achieve early break
                }
            }
            retried += 1;
        }
        Err(TxnError::TooManyRetry)
    }
    fn new_schema(&self, schema: &mut Schema) -> Result<(), ExecError> {
        let schema_id = self.schema_client.next_id()?.unwrap();
        schema.id = schema_id;
        self.schema_client.new_schema(schema)?;
        Ok(())
    }
    fn del_schema(&self, schema_id: &String) -> Result<(), ExecError> {
        self.schema_client.del_schema(schema_id)?;
        Ok(())
    }
    fn get_all_schema(&self) -> Result<Vec<Schema>, ExecError> {
        Ok(self.schema_client.get_all()?.unwrap())
    }
}