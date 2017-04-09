use std::collections::{BTreeMap, HashMap};
use bifrost::vector_clock::{VectorClock, StandardVectorClock, ServerVectorClock};
use parking_lot::RwLock;
use ram::cell::Cell;
use ram::types::Id;

pub type MVCellIndex = BTreeMap<StandardVectorClock, Cell>;

struct Transaction {
    start_time: u64,
    clock: StandardVectorClock,
}

struct Transactions_ {
    pub server_clock: StandardVectorClock,
    pub cell_versions: HashMap<Id, MVCellIndex>
}
