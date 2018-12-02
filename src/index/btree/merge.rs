use super::*;

pub struct BPlusTreeMergingPage {
    page: RwLockReadGuard<ExtNode>,
    mapper: Arc<ExtNodeCacheMap>,
    pages: Rc<RefCell<Vec<Id>>>,
}

impl MergingPage for BPlusTreeMergingPage {
    fn next(&self) -> Box<MergingPage> {
        unimplemented!()
    }

    fn keys(&self) -> &[EntryKey] {
        &self.page.keys[..self.page.len]
    }
}

impl MergeableTree for BPlusTree {
    fn prepare_level_merge(&self) -> Box<MergingTreeGuard> {
        unimplemented!()
    }

    fn elements(&self) -> usize {
        self.len()
    }
}

pub struct BPlusTreeMergingTreeGuard {
    cache: Arc<ExtNodeCacheMap>,
    txn: RefCell<Txn>,
    last_node: Arc<Node>,
    last_node_ref: TxnValRef,
    storage: Arc<AsyncClient>,
    pages: Rc<RefCell<Vec<Id>>>,
}

impl MergingTreeGuard for BPlusTreeMergingTreeGuard {
    fn remove_pages(&self, pages: &[&[EntryKey]]) {
        unimplemented!()
    }

    fn last_page(&self) -> Box<MergingPage> {
        unimplemented!()
    }
}

impl Drop for BPlusTreeMergingTreeGuard {
    fn drop(&mut self) {
        self.txn.borrow_mut().commit();
    }
}
