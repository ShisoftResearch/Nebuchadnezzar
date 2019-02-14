use bifrost::rpc::*;
use index::{EntryKey, Ordering};
use index::lsmtree::service::inner::TreeServiceInner;
use smallvec::SmallVec;
use index::lsmtree::tree::LSMTree;
use client::AsyncClient;
use std::path::Component::CurDir;

service! {
    rpc seek(key: Vec<u8>, ordering: Ordering) -> u64;
    rpc next(id: u64) -> Option<bool>;
    rpc current(id: u64) -> Option<Option<Vec<u8>>>;
    rpc complete(id: u64) -> bool;
}


pub struct LSMTreeService {
    inner: TreeServiceInner
}

dispatch_rpc_service_functions!(LSMTreeService);

impl Service for LSMTreeService {
    fn seek(&self, key: Vec<u8>, ordering: Ordering) -> Box<Future<Item=u64, Error=()>> {
        box future::ok(self.inner.seek(SmallVec::from(key), ordering))
    }

    fn next(&self, id: u64) -> Box<Future<Item=Option<bool>, Error=()>> {
        box future::ok(self.inner.next(&id))
    }

    fn current(&self, id: u64) -> Box<Future<Item=Option<Option<Vec<u8>>>, Error=()>> {
        box future::ok(self.inner.current(&id))
    }

    fn complete(&self, id: u64) -> Box<Future<Item=bool, Error=()>> {
        box future::ok(self.inner.complete(&id))
    }
}

impl LSMTreeService {
    pub fn new(neb_client: &Arc<AsyncClient>) -> Arc<Self> {
        Arc::new(Self {
            inner: TreeServiceInner::new(neb_client)
        })
    }
}