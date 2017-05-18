use std::sync::Arc;
use bifrost::conshash::ConsistentHashing;

pub mod transaction;
pub mod plain;

pub struct Client {
    pub consh: Option<Arc<ConsistentHashing>>,
}