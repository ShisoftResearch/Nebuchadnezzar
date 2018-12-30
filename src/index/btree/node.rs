use super::*;
use std::sync::atomic::fence;
use std::sync::atomic::Ordering::AcqRel;
use std::sync::atomic::Ordering::Acquire;
use std::sync::atomic::Ordering::Release;

pub enum NodeData<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    External(Box<ExtNode<KS, PS>>),
    Internal(Box<InNode<KS, PS>>),
    Empty(Box<EmptyNode>),
    None,
}

pub struct EmptyNode {
    pub left: Option<NodeCellRef>,
    pub right: NodeCellRef,
}

impl <KS, PS>NodeData<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
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
            &self.extnode().keys.as_slice_immute()
        } else {
            &self.innode().keys.as_slice_immute()
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
            len >= KS::slice_len() / 2 && len > 1
        }
    }

    pub fn cannot_merge(&self) -> bool {
        self.len() > KS::slice_len() / 2
    }

    pub fn extnode_mut(&mut self) -> &mut ExtNode<KS, PS> {
        match self {
            &mut NodeData::External(ref mut node) => node,
            _ => unreachable!(self.type_name()),
        }
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
        if self.is_empty() || self.len() > 0 && self.last_key() < key {
            let right_node = read_unchecked::<KS, PS>(self.right_ref().unwrap());
            if !right_node.is_none() && (self.is_empty() || right_node.len() > 0 && right_node.first_key() <= key) {
                debug!(
                    "found key to put to right page {:?}/{:?}",
                    key,
                    if right_node.is_empty() { smallvec!(0) } else { right_node.first_key().clone() }
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

pub fn write_non_empty<KS, PS>(search_page: NodeWriteGuard<KS, PS>) -> NodeWriteGuard<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    if !search_page.is_none() && search_page.is_empty() {
        return write_non_empty(write_node(search_page.right_ref().unwrap()))
    }
    return search_page;
}

pub fn write_key_page<KS, PS>(
    search_page: NodeWriteGuard<KS, PS>,
    key: &EntryKey,
) -> NodeWriteGuard<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    if search_page.is_empty() || search_page.len() > 0 && search_page.last_key() < key {
        let right_ref = search_page.right_ref().unwrap();
        let right_node = write_non_empty(write_node(right_ref));
        if !right_node.is_none() && (search_page.is_empty() || right_node.len() > 0 && right_node.first_key() <= key) {
            debug!("Will write {:?} from left {} node to right page, start with {:?}, keys {:?}",
                   key, search_page.type_name(),
                   if right_node.is_empty() { smallvec!(0) } else { right_node.first_key().clone() },
                   if right_node.is_empty() { &[] } else { right_node.keys() });
            return write_key_page(right_node, key);
        }
    }
    return search_page;
}

const LATCH_FLAG: usize = !(!0 >> 1);

pub struct Node<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    data: UnsafeCell<NodeData<KS, PS>>,
    cc: AtomicUsize,
}

impl <KS, PS>Node<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    pub fn new(data: NodeData<KS, PS>) -> Self {
        Node {
            data: UnsafeCell::new(data),
            cc: AtomicUsize::new(0),
        }
    }

    pub fn internal(innode: InNode<KS, PS>) -> Self {
        Self::new(NodeData::Internal(box innode))
    }

    pub fn external(extnode: ExtNode<KS, PS>) -> Self {
        Self::new(NodeData::External(box extnode))
    }

    pub fn none() -> Self {
        Self::new(NodeData::None)
    }
    pub fn none_ref() -> NodeCellRef {
        NodeCellRef::new(Node::<KS, PS>::none())
    }
    pub fn new_external(id: Id) -> Self {
        Self::external(ExtNode::new(id))
    }
    pub fn version(&self) -> usize {
        node_version(self.cc.load(SeqCst))
    }
}

pub fn node_version(cc_num: usize) -> usize {
    cc_num & (!LATCH_FLAG)
}

pub fn write_node<KS, PS>(node: &NodeCellRef) -> NodeWriteGuard<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    // debug!("acquiring node write lock");
    let node_deref = node.deref();
    let cc = &node_deref.cc;
    loop {
        let cc_num = cc.load(Relaxed);
        let expected = cc_num & (!LATCH_FLAG);
        debug_assert_eq!(expected & LATCH_FLAG, 0);
        match cc.compare_exchange_weak(expected, cc_num | LATCH_FLAG, Acquire, Relaxed) {
            Ok(num) if num == expected => {
                return NodeWriteGuard {
                    data: node_deref.data.get(),
                    cc: &node_deref.cc as *const AtomicUsize,
                    version: node_version(cc_num),
                    node_ref: node.clone()
                }
            }
            _ => {}
        }
    }
}

pub fn read_node<'a, KS, PS, F: FnMut(&NodeReadHandler<KS, PS>) -> R + 'a, R: 'a>(node: &NodeCellRef, mut func: F) -> R
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    let mut handler = read_unchecked(node);
    let cc = &node.deref::<KS, PS>().cc;
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

pub fn read_unchecked<KS, PS>(node: &NodeCellRef) -> NodeReadHandler<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    NodeReadHandler {
        ptr: node.deref().data.get(),
        version: 0,
        node_ref: node.clone()
    }
}

pub struct NodeWriteGuard<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    pub data: *mut NodeData<KS, PS>,
    pub cc: *const AtomicUsize,
    pub version: usize,
    node_ref: NodeCellRef
}

impl <KS, PS> Deref for NodeWriteGuard<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    type Target = NodeData<KS, PS>;

    fn deref(&self) -> &<Self as Deref>::Target {
        debug_assert_ne!(self.data as usize, 0);
        unsafe { &*self.data }
    }
}

impl <KS, PS> DerefMut for NodeWriteGuard<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    fn deref_mut(&mut self) -> &mut <Self as Deref>::Target {
        debug_assert_ne!(self.data as usize, 0);
        unsafe { &mut *self.data }
    }
}

impl <KS, PS> Drop for NodeWriteGuard<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
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

impl <KS, PS> Default for NodeWriteGuard<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    fn default() -> Self {
        NodeWriteGuard {
            data: 0 as *mut NodeData<KS, PS>,
            cc: 0 as *const AtomicUsize,
            version: 0,
            node_ref: Node::<KS, PS>::none_ref()
        }
    }
}

impl <KS, PS>NodeWriteGuard<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    pub fn node_ref(&self) -> &NodeCellRef {
        &self.node_ref
    }
}

unsafe impl <KS, PS> Sync for Node<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static {}

pub struct NodeReadHandler<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    pub ptr: *const NodeData<KS, PS>,
    pub version: usize,
    node_ref: NodeCellRef,
}

impl <KS, PS> Deref for NodeReadHandler<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
    type Target = NodeData<KS, PS>;

    fn deref(&self) -> &<Self as Deref>::Target {
        unsafe { &*self.ptr }
    }
}

pub struct RemoveStatus {
    pub item_found: bool,
    pub removed: bool,
}

impl <KS, PS> Default for Node<KS, PS>
    where KS: Slice<EntryKey> + Debug + 'static,
          PS: Slice<NodeCellRef> + 'static
{
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
    S: Slice<T>,
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
