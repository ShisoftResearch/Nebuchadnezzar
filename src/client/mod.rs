use std::sync::Arc;
use bifrost::conshash::{ConsistentHashing, CHError};
use bifrost::raft::client::{RaftClient, ClientError};
use bifrost::raft;

use server::{transactions as txn_server};
use ram::types::Id;
use ram::cell::{Cell, ReadError, WriteError};
use self::transaction::*;

static TRANSACTION_MAX_RETRY: u32 = 50;

pub mod transaction;
pub mod plain;

pub enum NebClientError {
    RaftClientError(ClientError),
    ConsistentHashtableError(CHError)
}

pub struct Client {
    pub conshash: Arc<ConsistentHashing>,
}

impl Client {
    pub fn new(meta_servers: &Vec<String>, group: &String) -> Result<Client, NebClientError> {
        match RaftClient::new(meta_servers, raft::DEFAULT_SERVICE_ID) {
            Ok(raft_client) => {
                match ConsistentHashing::new_client(group, &raft_client) {
                    Ok(chash) => Ok(Client {conshash: chash}),
                    Err(err) => Err(NebClientError::ConsistentHashtableError(err))
                }
            },
            Err(err) => Err(NebClientError::RaftClientError(err))
        }
    }
    pub fn read_cell(&self, key: &Id) -> Result<Cell, ReadError> {
        unimplemented!()
    }
    fn write_cell(&self, cell: &Cell) -> Result<Cell, WriteError> {
         unimplemented!()
    }
    fn update_cell(&self, cell: &Cell) -> Result<Cell, WriteError> {
        unimplemented!()
    }
    fn remove_cell(&self, key: &Id) -> Result<(), WriteError> {
        unimplemented!()
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
                    txn.abort();
                },
                Err(e) => {
                    txn.abort()?; // abort will always be an error to achieve early break
                }
            }
            retried += 1;
        }
        Err(TxnError::TooManyRetry)
    }
}