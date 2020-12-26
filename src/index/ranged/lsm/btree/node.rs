use super::*;
use futures::FutureExt;
use std::any::TypeId;
use std::ptr;
use std::sync::atomic::Ordering::{Release, Acquire, AcqRel};

pub struct EmptyNode {
    pub left: Option<NodeCellRef>,
    pub right: NodeCellRef,
}

pub enum NodeData<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    External(Box<ExtNode<KS, PS>>),
    Internal(Box<InNode<KS, PS>>),
    Empty(Box<EmptyNode>),
    None,
}

impl<KS, PS> NodeData<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub fn is_none(&self) -> bool {
        match self {
            &NodeData::None => true,
            _ => false,
        }
    }
    pub fn is_empty(&self) -> bool {
        match self {
            &NodeData::Empty(ref _n) => true,
            &NodeData::External(ref n) => n.len == 0,
            &NodeData::Internal(ref n) => n.len == 0,
            &NodeData::None => unreachable!(),
        }
    }
    pub fn is_empty_node(&self) -> bool {
        match self {
            &NodeData::Empty(ref _n) => true,
            _ => false,
        }
    }

    pub fn search(&self, key: &EntryKey) -> usize {
        match self {
            &NodeData::Internal(ref innode) => innode.search(key),
            &NodeData::External(ref extnode) => extnode.search(key),
            _ => 0,
        }
    }

    // search may panic if another thread is writing, this will return error if panic occurred
    pub fn search_unwindable(
        &self,
        key: &EntryKey,
    ) -> Result<usize, Box<dyn Any + Send + 'static>> {
        match self {
            &NodeData::Internal(ref innode) => innode.search_unwindable(key),
            &NodeData::External(ref extnode) => extnode.search_unwindable(key),
            _ => Ok(0),
        }
    }

    pub fn remove(&mut self, pos: usize) {
        match self {
            &mut NodeData::External(ref mut node) => node.remove_at(pos),
            &mut NodeData::Internal(ref mut node) => node.remove_at(pos),
            &mut NodeData::None | &mut NodeData::Empty(_) => unreachable!(self.type_name()),
        }
    }
    pub fn is_ext(&self) -> bool {
        match self {
            &NodeData::External(_) => true,
            &NodeData::Internal(_) => false,
            &NodeData::None | &NodeData::Empty(_) => panic!(self.type_name()),
        }
    }
    pub fn is_internal(&self) -> bool {
        match self {
            &NodeData::External(_) => false,
            &NodeData::Internal(_) => true,
            &NodeData::Empty(_) => false,
            &NodeData::None => panic!(self.type_name()),
        }
    }

    pub fn keys(&self) -> &[EntryKey] {
        if self.is_ext() {
            &self.extnode().keys.as_slice_immute()[..self.len()]
        } else {
            &self.innode().keys.as_slice_immute()[..self.len()]
        }
    }

    pub fn ptrs(&self) -> &[NodeCellRef] {
        if self.is_ext() {
            unreachable!()
        } else {
            &self.innode().ptrs.as_slice_immute()[..self.len() + 1]
        }
    }

    pub fn ptrs_mut(&mut self) -> &mut [NodeCellRef] {
        if self.is_ext() {
            unreachable!()
        } else {
            &mut self.innode_mut().ptrs.as_slice()[..]
        }
    }

    pub fn first_key(&self) -> &EntryKey {
        if self.is_empty() && !self.is_empty_node() {
            &*MIN_ENTRY_KEY
        } else {
            &self.keys()[0]
        }
    }

    pub fn last_key(&self) -> &EntryKey {
        if self.is_empty() && !self.is_empty_node() {
            &*MIN_ENTRY_KEY
        } else {
            &self.keys()[self.len() - 1]
        }
    }

    pub fn len(&self) -> usize {
        match self {
            &NodeData::Internal(ref node) => node.len,
            &NodeData::External(ref node) => node.len,
            &NodeData::Empty(_) => 0,
            &NodeData::None => unreachable!(),
        }
    }

    // check if the node will be half full after an item have been removed
    pub fn is_half_full(&self) -> bool {
        if self.is_none() {
            true
        } else {
            let len = self.len();
            if len == 0 {
                return false;
            }
            len >= KS::slice_len() / 2 && len > 1
        }
    }

    pub fn cannot_merge(&self) -> bool {
        self.len() > KS::slice_len() / 2
    }

    pub fn innode_mut(&mut self) -> &mut InNode<KS, PS> {
        match self {
            &mut NodeData::Internal(ref mut n) => n,
            _ => unreachable!(self.type_name()),
        }
    }
    pub fn extnode(&self) -> &ExtNode<KS, PS> {
        match self {
            &NodeData::External(ref node) => node,
            _ => unreachable!(self.type_name()),
        }
    }
    pub fn ext_id(&self) -> Id {
        match self {
            &NodeData::External(ref node) => node.id,
            &NodeData::None => Id::unit_id(),
            &NodeData::Empty(_) => Id::unit_id(),
            &NodeData::Internal(_) => unreachable!(self.type_name()),
        }
    }
    pub fn innode(&self) -> &InNode<KS, PS> {
        match self {
            &NodeData::Internal(ref n) => n,
            _ => unreachable!(self.type_name()),
        }
    }
    pub fn type_name(&self) -> &'static str {
        match self {
            &NodeData::Internal(_) => "internal",
            &NodeData::External(_) => "external",
            &NodeData::None => "none",
            &NodeData::Empty(_) => "empty",
        }
    }
    pub fn key_at_right_node(&self, key: &EntryKey) -> Option<&NodeCellRef> {
        if self.is_empty() || (self.len() > 0 && self.right_bound() <= key) {
            let right_node = read_unchecked::<KS, PS>(self.right_ref().unwrap());
            if !right_node.is_none()
                && (self.is_empty() || (right_node.len() > 0 && right_node.first_key() <= key))
            {
                trace!(
                    "found key to put to right page {:?}/{:?}",
                    key,
                    if right_node.is_empty() {
                        min_entry_key()
                    } else {
                        right_node.first_key().clone()
                    }
                );
                return Some(self.right_ref().unwrap());
            }
        }
        return None;
    }

    pub fn left_ref_mut(&mut self) -> Option<&mut NodeCellRef> {
        match self {
            &mut NodeData::External(ref mut n) => Some(&mut n.prev),
            &mut NodeData::Empty(ref mut n) => n.left.as_mut(),
            _ => None,
        }
    }

    pub fn left_ref(&self) -> Option<&NodeCellRef> {
        match self {
            &NodeData::External(ref n) => Some(&n.prev),
            &NodeData::Empty(ref n) => n.left.as_ref(),
            _ => None,
        }
    }

    pub fn right_ref_mut(&mut self) -> Option<&mut NodeCellRef> {
        match self {
            &mut NodeData::External(ref mut n) => Some(&mut n.next),
            &mut NodeData::Internal(ref mut n) => Some(&mut n.right),
            &mut NodeData::Empty(ref mut n) => Some(&mut n.right),
            &mut NodeData::None => None,
        }
    }

    pub fn right_ref(&self) -> Option<&NodeCellRef> {
        match self {
            &NodeData::External(ref n) => Some(&n.next),
            &NodeData::Internal(ref n) => Some(&n.right),
            &NodeData::Empty(ref n) => Some(&n.right),
            &NodeData::None => None,
        }
    }

    pub fn has_vaild_right_node(&self) -> bool {
        self.right_ref().is_some()
    }

    pub fn right_bound(&self) -> &EntryKey {
        match self {
            &NodeData::External(ref n) => &n.right_bound,
            &NodeData::Internal(ref n) => &n.right_bound,
            _ => panic!(self.type_name()),
        }
    }

    pub fn right_bound_mut(&mut self) -> &mut EntryKey {
        match self {
            &mut NodeData::External(ref mut n) => &mut n.right_bound,
            &mut NodeData::Internal(ref mut n) => &mut n.right_bound,
            _ => panic!(self.type_name()),
        }
    }
}

pub fn write_targeted<KS, PS>(
    mut search_page: NodeWriteGuard<KS, PS>,
    key: &EntryKey,
) -> NodeWriteGuard<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    loop {
        // check if node empty or key out of bound
        if search_page.is_empty() || search_page.right_bound() <= key {
            let right_node_ref = search_page.right_ref().unwrap();
            if right_node_ref.is_default() {
                // right node is none, should pick current one if not empty node
                debug_assert!(!search_page.is_empty_node());
                return search_page;
            }
            let right_node = write_node(right_node_ref);
            trace!(
                "Shifting to right {} node for {:?}, first key {:?}",
                right_node.type_name(),
                key,
                if !right_node.is_none() && right_node.len() > 0 {
                    Some(right_node.first_key())
                } else {
                    None
                }
            );
            search_page = right_node;
        } else {
            return search_page;
        }
    }
}

//0x00007fb854907828

const LATCH_FLAG: usize = !(!0 >> 1);

pub trait AnyNode: Any + Send + Sync + 'static {
    fn persist(
        &self,
        node_ref: &NodeCellRef,
        deletion: &DeletionSet,
        neb: &Arc<crate::client::AsyncClient>,
    ) -> BoxFuture<()>;
    unsafe fn take_all_refs(&self) -> Vec<NodeCellRef>;
}

pub struct Node<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    cc: AtomicUsize,
    data: UnsafeCell<NodeData<KS, PS>>,
}

impl<KS, PS> Node<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub fn new(data: NodeData<KS, PS>) -> Self {
        Node {
            data: UnsafeCell::new(data),
            cc: AtomicUsize::new(0),
        }
    }

    pub fn with_internal(innode: Box<InNode<KS, PS>>) -> Self {
        Self::new(NodeData::Internal(innode))
    }

    pub fn with_external(extnode: Box<ExtNode<KS, PS>>) -> Self {
        Self::new(NodeData::External(extnode))
    }

    pub fn none_ref() -> NodeCellRef {
        NodeCellRef::default()
    }

    pub fn new_external(id: Id, right_bound: EntryKey) -> Self {
        Self::with_external(ExtNode::new(id, right_bound))
    }

    pub fn version(&self) -> usize {
        node_version(self.cc.load(Acquire))
    }
}

pub fn node_version(cc_num: usize) -> usize {
    cc_num & (!LATCH_FLAG)
}

pub fn write_node<KS, PS>(node: &NodeCellRef) -> NodeWriteGuard<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    if node.is_default() {
        return NodeWriteGuard {
            data: ptr::null_mut(),
            cc: ptr::null_mut(),
            version: 0,
            node_ref: NodeCellRef::default(),
        };
    }
    // trace!("acquiring node write lock");
    let node_deref = node.deref();
    let cc = &node_deref.cc;
    let backoff = crossbeam::utils::Backoff::new();
    loop {
        let cc_num = cc.load(Acquire);
        let expected = cc_num & (!LATCH_FLAG);
        debug_assert_eq!(expected & LATCH_FLAG, 0);
        if cc.compare_and_swap(expected, cc_num | LATCH_FLAG, AcqRel) == expected {
            return NodeWriteGuard {
                data: node_deref.data.get(),
                cc: &node_deref.cc as *const AtomicUsize,
                version: node_version(cc_num),
                node_ref: node.clone(),
            };
        }
        backoff.spin();
    }
}

pub fn is_node_locked<KS, PS>(node: &NodeCellRef) -> bool
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let node_deref = node.deref::<KS, PS>();
    let cc = &node_deref.cc;
    let cc_num = cc.load(Acquire);
    let expected = cc_num & (!LATCH_FLAG);
    cc_num == expected
}

pub fn read_node<'a, KS, PS, F: FnMut(&NodeReadHandler<KS, PS>) -> R + 'a, R: 'a>(
    node: &NodeCellRef,
    mut func: F,
) -> R
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    let mut handler = read_unchecked(node);
    if node.is_default() || handler.is_none() {
        return func(&handler);
    }
    let cc = &node.deref::<KS, PS>().cc;
    let backoff = crossbeam::utils::Backoff::new();
    loop {
        let cc_num = cc.load(Acquire);
        if cc_num & LATCH_FLAG == LATCH_FLAG {
            // trace!("read have a latch, retry {:b}", cc_num);
            backoff.spin();
            continue;
        }
        handler.version = cc_num & (!LATCH_FLAG);
        let res = func(&handler);
        let new_cc_num = cc.load(Acquire);
        // Check the version. If not found need to retry
        if new_cc_num == cc_num {
            return res;
        }
    }
}

pub fn read_unchecked<KS, PS>(node: &NodeCellRef) -> NodeReadHandler<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    NodeReadHandler {
        version: 0,
        node_ref: node.clone(),
        mark: PhantomData,
    }
}

pub struct NodeWriteGuard<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub data: *mut NodeData<KS, PS>,
    pub cc: *const AtomicUsize,
    pub version: usize,
    node_ref: NodeCellRef,
}

impl<KS, PS> Deref for NodeWriteGuard<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    type Target = NodeData<KS, PS>;

    fn deref(&self) -> &<Self as Deref>::Target {
        assert!(!self.data.is_null());
        unsafe { &*self.data }
    }
}

impl<KS, PS> DerefMut for NodeWriteGuard<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    fn deref_mut(&mut self) -> &mut <Self as Deref>::Target {
        assert!(!self.data.is_null());
        unsafe { &mut *self.data }
    }
}

impl<KS, PS> Drop for NodeWriteGuard<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    fn drop(&mut self) {
        // cope with null pointer
        if self.cc as usize != 0 {
            let cc = unsafe { &*self.cc };
            let cc_num = cc.load(Acquire);
            debug_assert_eq!(cc_num & LATCH_FLAG, LATCH_FLAG);
            cc.store((cc_num & (!LATCH_FLAG)) + 1, Release);
        }
    }
}

impl<KS, PS> Default for NodeWriteGuard<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    fn default() -> Self {
        NodeWriteGuard {
            data: 0 as *mut NodeData<KS, PS>,
            cc: 0 as *const AtomicUsize,
            version: 0,
            node_ref: Node::<KS, PS>::none_ref(),
        }
    }
}

impl<KS, PS> NodeWriteGuard<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub fn node_ref(&self) -> &NodeCellRef {
        &self.node_ref
    }

    pub fn is_ref_none(&self) -> bool {
        self.node_ref.is_default()
    }

    // make an empty node as empty node, right node pointer covered
    pub fn make_empty_node(&mut self, update_right: bool) {
        if self.is_empty_node() {
            return;
        }
        let data = &mut (**self);
        let left_node = data.left_ref().cloned();
        let right_node = data.right_ref().cloned().unwrap();
        // check if have left node, if so then update the right node left pointer
        if update_right && left_node.is_some() {
            let mut right_guard = write_node::<KS, PS>(&right_node);
            *right_guard.left_ref_mut().unwrap() = left_node.clone().unwrap();
        }
        if let NodeData::External(ex_node) = data {
            make_deleted::<KS, PS>(&ex_node.id)
        }
        let empty = EmptyNode {
            left: left_node,
            right: right_node,
        };
        *data = NodeData::Empty(box empty)
    }

    pub fn extnode_mut_no_persist(&mut self) -> &mut ExtNode<KS, PS> {
        match { unsafe { &mut *self.data } } {
            &mut NodeData::External(ref mut node) => return node,
            _ => unreachable!(self.type_name().to_owned()),
        }
    }

    pub fn extnode_mut(&mut self, tree: &BPlusTree<KS, PS>) -> &mut ExtNode<KS, PS> {
        external::make_changed(&self.node_ref, tree);
        self.extnode_mut_no_persist()
    }
}

unsafe impl<KS, PS> Send for NodeWriteGuard<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
}

unsafe impl<KS, PS> Sync for NodeWriteGuard<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
}

unsafe impl<KS, PS> Sync for Node<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
}

impl<KS, PS> AnyNode for Node<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    fn persist(
        &self,
        node_ref: &NodeCellRef,
        deletion: &DeletionSet,
        neb: &Arc<crate::client::AsyncClient>,
    ) -> BoxFuture<()> {
        let mut guard = write_node::<KS, PS>(node_ref);
        let guard_ref = &mut *guard;
        let cell = match guard_ref {
            &mut NodeData::External(ref mut node) => Some(node.to_cell(&*deletion)),
            &mut NodeData::Empty(_) => None,
            _ => panic!(
                "Cannot persist internal or other type of nodes, type {}",
                guard_ref.type_name()
            ),
        };
        let neb = neb.clone();
        async move {
            if let Some(cell) = cell {
                let cell_id = cell.id();
                trace!("Updating node cell {:?}", cell_id);
                match neb.upsert_cell(cell).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        warn!("Cell node update error for {:?}, error: {:?}", cell_id, e);
                    }
                    Err(e) => {
                        error!(
                            "Cell node insertion error for {:?}, error: {:?}",
                            cell_id, e
                        );
                    }
                }
                trace!("Cell {:?} updated", cell_id);
            }
        }
        .boxed()
    }

    unsafe fn take_all_refs(&self) -> Vec<NodeCellRef> {
        let node = self.data.get().as_mut().unwrap();
        let mut res = vec![];
        if !node.is_none() && !node.is_empty() && node.is_internal() {
            for ptr in node.ptrs_mut() {
                if !ptr.is_default() {
                    res.push(mem::take(ptr));
                }
            }
        }
        node.left_ref_mut().map(|l| if !l.is_default() {
            res.push(mem::take(l))
        });
        node.right_ref_mut().map(|r| if !r.is_default() {
            res.push(mem::take(r))
        });
        res
    }
}

impl dyn AnyNode {
    pub fn is_type<T: AnyNode>(&self) -> bool {
        TypeId::of::<T>() == self.type_id()
    }
}

pub struct NodeReadHandler<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub version: usize,
    node_ref: NodeCellRef,
    mark: PhantomData<(KS, PS)>,
}

impl<KS, PS> Deref for NodeReadHandler<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    type Target = NodeData<KS, PS>;

    fn deref(&self) -> &<Self as Deref>::Target {
        unsafe { &*self.node_ref.deref().data.get() }
    }
}

impl<KS, PS> NodeReadHandler<KS, PS>
where
    KS: Slice<EntryKey> + Debug + 'static,
    PS: Slice<NodeCellRef> + 'static,
{
    pub fn node_ref(&self) -> &NodeCellRef {
        &self.node_ref
    }
}

pub struct RemoveStatus {
    pub item_found: bool,
    pub removed: bool,
}

pub fn insert_into_split<T, S>(
    item: T,
    x: &mut S,
    y: &mut S,
    xlen: &mut usize,
    ylen: &mut usize,
    pos: usize,
) where
    S: Slice<T>,
    T: Default,
{
    trace!(
        "insert into split left len {}, right len {}, pos {}",
        xlen,
        ylen,
        pos
    );
    if pos < *xlen {
        trace!("insert into left part, pos: {}", pos);
        x.insert_at(item, pos, xlen);
    } else {
        let right_pos = pos - *xlen;
        trace!("insert into right part, pos: {}", right_pos);
        y.insert_at(item, right_pos, ylen);
    }
}
