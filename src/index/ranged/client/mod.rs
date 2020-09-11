use super::lsm::service::AsyncServiceClient;
use super::sm::client::SMClient;
use std::sync::Arc;
use super::lsm::btree::Ordering;
use crate::index::EntryKey;

pub mod cursor;

pub struct RangedQueryClient {
}

impl RangedQueryClient {
    pub async fn seek(&self, key: &EntryKey, ordering: Ordering) -> Option<cursor::ClientCursor> {
        unimplemented!()
    }
}