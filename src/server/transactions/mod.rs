use bifrost::vector_clock::{VectorClock, StandardVectorClock, ServerVectorClock};
use std::sync::Arc;
use bifrost::utils::time::get_time;
use rand::Rng;

mod manager;
mod data_site;

// Peer have a clock, meant to update with other servers in the cluster
pub struct Peer {
    pub clock: ServerVectorClock
}

impl Peer {
    pub fn new(server_address: &String) -> Arc<Peer> {
        Arc::new(Peer {
            clock: ServerVectorClock::new(server_address)
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct TransactionId {
    pub timestamp: i64,
    pub rand_int: u64
}

impl TransactionId {
    pub fn next() -> TransactionId {
        let mut rng = ::rand::thread_rng();
        TransactionId {
            timestamp: get_time(),
            rand_int: rng.gen::<u64>()
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum TransactionExecResult<A, E> {
    Rejected,
    Accepted(A),
    Error(E),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DataSiteResponse<T> {
    payload: T,
    clock: StandardVectorClock
}
