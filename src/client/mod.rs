use std::sync::Arc;
use bifrost::conshash::{ConsistentHashing, CHError};
use bifrost::raft::client::{RaftClient, ClientError};
use bifrost::raft;

use server::{transactions as txn_server};
use ram::types::Id;
use ram::cell::{Cell, ReadError, WriteError};
use self::transaction::*;

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
    fn transaction<TFN>(&self, func: TFN) -> Result<(), TxnError>
        where TFN: Fn(Transaction) -> Result<(), TxnError> {
        let server_name = match self.conshash.rand_server() {
            Some(name) => name,
            None => return Err(TxnError::CannotFindAServer)
        };
        let txn_client = match txn_server::new_client(&server_name) {
            Ok(client) => client,
            Err(e) => return Err(TxnError::IoError(e))
        };
        let mut txn_id: txn_server::TxnId;
        while true {
            txn_id = match txn_client.begin() {
                Ok(Ok(id)) => id,
                _ => return Err(TxnError::CannotBegin)
            };
            let txn = Transaction{
                tid: txn_id,
                client: txn_client.clone()
            };
            let exec_result = func(txn);
            match exec_result {
                Ok(()) => return Ok(()),
                Err(TxnError::NotRealizable) => continue,
                Err(e) => return Err(e)
            }
        }
        Ok(())
    }
}