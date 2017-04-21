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

pub type TransactionId = StandardVectorClock;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum TransactionExecResult<A, E> {
    Rejected,
    Wait,
    Accepted(A),
    Error(E),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DataSiteResponse<T> {
    payload: T,
    clock: StandardVectorClock
}

impl <T> DataSiteResponse <T> {
    pub fn new(peer: &Arc<Peer>, data: T) -> DataSiteResponse<T> {
        DataSiteResponse {
            payload: data,
            clock: peer.clock.to_clock()
        }
    }
}
