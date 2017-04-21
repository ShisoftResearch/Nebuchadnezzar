use bifrost::vector_clock::{VectorClock, StandardVectorClock, ServerVectorClock};
use std::sync::Arc;

mod manager;
mod site;

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