use super::*;
use std::sync::atomic::fence;
use std::sync::atomic::Ordering::AcqRel;
use std::sync::atomic::Ordering::Acquire;
use std::sync::atomic::Ordering::Release;

pub enum InsertSearchResult {
    External,
    Internal(NodeCellRef),
    RightNode(NodeCellRef),
}

pub enum InsertToNodeResult {
    NoSplit,
    Split(NodeWriteGuard, NodeCellRef, Option<EntryKey>),
    SplitParentChanged,
}

pub enum RemoveSearchResult {
    External,
    RightNode(NodeCellRef),
    Internal(NodeCellRef),
}

pub enum SubNodeStatus {
    ExtNodeEmpty,
    InNodeEmpty,
    Relocate(usize, usize),
    Merge(usize, usize),
    Ok,
}

pub struct NodeSplit {
    pub new_right_node: NodeCellRef,
    pub left_node_latch: NodeWriteGuard,
    pub pivot: EntryKey,
    pub parent_latch: NodeWriteGuard,
}

pub struct RebalancingNodes {
    pub left_guard: NodeWriteGuard,
    pub left_ref: NodeCellRef,
    pub right_guard: NodeWriteGuard,
    pub right_right_guard: NodeWriteGuard, // for external pointer modification
    pub parent: NodeWriteGuard,
    pub parent_pos: usize,
}

pub struct RemoveResult {
    pub rebalancing: Option<RebalancingNodes>,
    pub empty: bool,
    pub removed: bool,
}

pub enum NodeData {
    External(Box<ExtNode>),
    Internal(Box<InNode>),
    Empty(Box<EmptyNode>),
    None,
}

pub struct EmptyNode {
    pub left: Option<NodeCellRef>,
    pub right: NodeCellRef,
}

impl NodeData {
    pub fn is_none(&self) -> bool {
        match self {
            &NodeData::None => true,
            _ => false,
        }
    }
    pub fn is_empty(&self) -> bool {
        match self {
            &NodeData::Empty(ref n) => true,
            &NodeData::External(ref n) => n.len == 0,
            &NodeData::Internal(ref n) => n.len == 0,
            &NodeData::None => unreachable!(),
        }
    }
    pub fn is_empty_node(&self) -> bool {
        match self {
            &NodeData::Empty(ref n) => true,
            _ => false
        }
    }
    pub fn search(&self, key: &EntryKey) -> usize {
        let len = self.len();
        if self.is_ext() {
            self.extnode().search(key)
        } else {
            self.innode().search(key)
        }
    }

    pub fn remove(&mut self, pos: usize) {
        match self {
            &mut NodeData::External(ref mut node) => node.remove_at(pos),
            &mut NodeData::Internal(ref mut node) => node.remove_at(pos),
            &mut NodeData::None | &mut NodeData::Empty(_) => unreachable!(),
        }
    }
    pub fn is_ext(&self) -> bool {
        match self {
            &NodeData::External(_) => true,
            &NodeData::Internal(_) => false,
            &NodeData::None | &NodeData::Empty(_) => panic!(),
        }
    }
    pub fn keys(&self) -> &[EntryKey] {
        if self.is_ext() {
            &self.extnode().keys
        } else {
            &self.innode().keys
        }
    }

    pub fn first_key(&self) -> &EntryKey {
        &self.keys()[0]
    }

    pub fn last_key(&self) -> &EntryKey {
        &self.keys()[self.len() - 1]
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
            len >= NUM_KEYS / 2 && len > 1
        }
    }

    pub fn cannot_merge(&self) -> bool {
        self.len() > NUM_KEYS / 2
    }

    pub fn extnode_mut(&mut self) -> &mut ExtNode {
        match self {
            &mut NodeData::External(ref mut node) => node,
            _ => unreachable!(self.type_name()),
        }
    }
    pub fn innode_mut(&mut self) -> &mut InNode {
        match self {
            &mut NodeData::Internal(ref mut n) => n,
            _ => unreachable!(self.type_name()),
        }
    }
    pub fn extnode(&self) -> &ExtNode {
        match self {
            &NodeData::External(ref node) => node,
            _ => unreachable!(self.type_name()),
        }
    }
    pub fn ext_id(&self) -> Id {
        match self {
            &NodeData::External(ref node) => node.id,
            &NodeData::None => Id::unit_id(),
            &NodeData::Internal(_) | &NodeData::Empty(_) => unreachable!(self.type_name()),
        }
    }
    pub fn innode(&self) -> &InNode {
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
        if self.is_empty_node() || self.len() > 0 && self.last_key() < key {
            let right_node = self.right_ref().unwrap().read_unchecked();
            if !right_node.is_none() && (self.is_empty_node() || right_node.len() > 0 && right_node.first_key() <= key) {
                debug!(
                    "found key to put to right page {:?}/{:?}",
                    key, right_node.first_key()
                );
                return Some(self.right_ref().unwrap())
            }
        }
        return None;
    }

    pub fn left_ref_mut(&mut self) -> Option<&mut NodeCellRef> {
        match self {
            &mut NodeData::External(ref mut n) => Some(&mut n.prev),
            _ => None,
        }
    }

    pub fn left_ref(&self) -> Option<&NodeCellRef> {
        match self {
            &NodeData::External(ref n) => Some(&n.prev),
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
}

pub fn write_key_page(
    search_page: NodeWriteGuard,
    search_ref: &NodeCellRef,
    key: &EntryKey,
) -> (NodeWriteGuard, NodeCellRef) {
    if search_page.is_empty_node() || search_page.len() > 0 && search_page.last_key() < key {
        let right_ref = search_page.right_ref().unwrap();
        let right_node = write_node(right_ref);
        if !right_node.is_none() && (search_page.is_empty_node() || right_node.len() > 0 && right_node.first_key() <= key) {
            debug!("Will write {:?} from left {} node to right page, start with {:?}, keys {:?}",
                   key, search_page.type_name(),
                   if right_node.is_empty_node() { smallvec!(0) } else { right_node.first_key().clone() },
                   if right_node.is_empty_node() { &[] } else { right_node.keys() });
            return write_key_page(right_node, right_ref, key);
        }
    }
    return (search_page, search_ref.clone());
}

const LATCH_FLAG: usize = !(!0 >> 1);

pub struct Node {
    data: UnsafeCell<NodeData>,
    cc: AtomicUsize,
}

impl Node {
    pub fn new(data: NodeData) -> Self {
        Node {
            data: UnsafeCell::new(data),
            cc: AtomicUsize::new(0),
        }
    }

    pub fn internal(innode: InNode) -> Self {
        Self::new(NodeData::Internal(box innode))
    }

    pub fn external(extnode: ExtNode) -> Self {
        Self::new(NodeData::External(box extnode))
    }

    pub fn none() -> Self {
        Self::new(NodeData::None)
    }
    pub fn none_ref() -> NodeCellRef {
        Arc::new(Node::none())
    }
    pub fn new_external(id: Id) -> Self {
        Self::external(ExtNode::new(id))
    }

    pub fn read_unchecked(&self) -> &NodeData {
        unsafe { &*self.data.get() }
    }

    pub fn version(&self) -> usize {
        node_version(self.cc.load(SeqCst))
    }
}

pub fn node_version(cc_num: usize) -> usize {
    cc_num & (!LATCH_FLAG)
}

pub fn write_node(node: &NodeCellRef) -> NodeWriteGuard {
    // debug!("acquiring node write lock");
    let cc = &node.cc;
    loop {
        let cc_num = cc.load(Relaxed);
        let expected = cc_num & (!LATCH_FLAG);
        debug_assert_eq!(expected & LATCH_FLAG, 0);
        match cc.compare_exchange_weak(expected, cc_num | LATCH_FLAG, Acquire, Relaxed) {
            Ok(num) if num == expected => {
                return NodeWriteGuard {
                    data: node.data.get(),
                    cc: &node.cc as *const AtomicUsize,
                    version: node_version(cc_num),
                    node_ref: node.clone()
                }
            }
            _ => {}
        }
    }
}

pub fn read_node<'a, F: FnMut(&NodeReadHandler) -> R + 'a, R: 'a>(node: &NodeCellRef, mut func: F) -> R {
    let mut handler = NodeReadHandler {
        ptr: node.data.get(),
        version: 0,
        node_ref: node.clone()
    };
    let cc = &node.cc;
    loop {
        let cc_num = cc.load(SeqCst);
        if cc_num & LATCH_FLAG == LATCH_FLAG {
            // debug!("read have a latch, retry {:b}", cc_num);
            continue;
        }
        handler.version = cc_num & (!LATCH_FLAG);
        let res = func(&handler);
        let new_cc_num = cc.load(SeqCst);
        if new_cc_num == cc_num {
            return res;
        }
        // debug!("read version changed, retry {:b}", cc_num);
    }
}

pub struct NodeWriteGuard {
    pub data: *mut NodeData,
    pub cc: *const AtomicUsize,
    pub version: usize,
    node_ref: NodeCellRef
}

impl Deref for NodeWriteGuard {
    type Target = NodeData;

    fn deref(&self) -> &<Self as Deref>::Target {
        debug_assert_ne!(self.data as usize, 0);
        unsafe { &*self.data }
    }
}

impl DerefMut for NodeWriteGuard {
    fn deref_mut(&mut self) -> &mut <Self as Deref>::Target {
        debug_assert_ne!(self.data as usize, 0);
        unsafe { &mut *self.data }
    }
}

impl Drop for NodeWriteGuard {
    fn drop(&mut self) {
        // cope with null pointer
        if self.cc as usize != 0 {
            let cc = unsafe { &*self.cc };
            let cc_num = cc.load(SeqCst);
            debug_assert_eq!(cc_num & LATCH_FLAG, LATCH_FLAG);
            cc.store((cc_num & (!LATCH_FLAG)) + 1, Release);
        }
    }
}

impl Default for NodeWriteGuard {
    fn default() -> Self {
        NodeWriteGuard {
            data: 0 as *mut NodeData,
            cc: 0 as *const AtomicUsize,
            version: 0,
            node_ref: Node::none_ref()
        }
    }
}

unsafe impl Sync for Node {}

pub struct NodeReadHandler {
    pub ptr: *const NodeData,
    pub version: usize,
    node_ref: NodeCellRef,
}

impl Deref for NodeReadHandler {
    type Target = NodeData;

    fn deref(&self) -> &<Self as Deref>::Target {
        unsafe { &*self.ptr }
    }
}

pub struct RemoveStatus {
    pub item_found: bool,
    pub removed: bool,
}

impl Default for Node {
    fn default() -> Self {
        Node::none()
    }
}

pub fn insert_into_split<T, S>(
    item: T,
    x: &mut S,
    y: &mut S,
    xlen: &mut usize,
    ylen: &mut usize,
    pos: usize,
) where
    S: Slice<Item = T> + BTreeSlice<T>,
    T: Default,
{
    debug!(
        "insert into split left len {}, right len {}, pos {}",
        xlen, ylen, pos
    );
    if pos < *xlen {
        debug!("insert into left part, pos: {}", pos);
        x.insert_at(item, pos, xlen);
    } else {
        let right_pos = pos - *xlen;
        debug!("insert into right part, pos: {}", right_pos);
        y.insert_at(item, right_pos, ylen);
    }
}
