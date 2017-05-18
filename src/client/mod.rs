use std::sync::Arc;
use bifrost::conshash::{ConsistentHashing, CHError};
use bifrost::raft::client::{RaftClient, ClientError};
use bifrost::raft;

use ram::types::Id;
use ram::cell::{Cell, ReadError, WriteError};

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
    fn transaction<TFN>(&self, func: TFN) -> Result<(), transaction::TxnError>
        where TFN: Fn(transaction::Transaction) {
        unimplemented!()
    }
}