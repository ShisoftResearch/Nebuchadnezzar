use super::*;

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
    Internal(NodeCellRef, SubNodeStatus),
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
    pub parent_latch: Option<NodeWriteGuard>,
}

pub enum NodeSplitResult {
    Split(NodeSplit),
    Retry,
}

pub enum NodeData {
    External(Box<ExtNode>),
    Internal(Box<InNode>),
    None,
}

impl NodeData {
    pub fn is_none(&self) -> bool {
        match self {
            &NodeData::None => true,
            _ => false,
        }
    }
    pub fn search(&self, key: &EntryKey) -> usize {
        let len = self.len();
        if self.is_ext() {
            self.extnode().keys[..len]
                .binary_search(key)
                .unwrap_or_else(|i| i)
        } else {
            self.innode().keys[..len]
                .binary_search(key)
                .map(|i| i + 1)
                .unwrap_or_else(|i| i)
        }
    }

    pub fn remove(&mut self, pos: usize) {
        match self {
            &mut NodeData::External(ref mut node) => node.remove_at(pos),
            &mut NodeData::Internal(ref mut node) => node.remove_at(pos),
            &mut NodeData::None => unreachable!(),
        }
    }
    pub fn is_ext(&self) -> bool {
        match self {
            &NodeData::External(_) => true,
            &NodeData::Internal(_) => false,
            &NodeData::None => panic!(),
        }
    }
    pub fn first_key(&self) -> EntryKey {
        if self.is_ext() {
            self.extnode().keys[0].to_owned()
        } else {
            self.innode().keys[0].to_owned()
        }
    }
    pub fn len(&self) -> usize {
        if self.is_ext() {
            self.extnode().len
        } else {
            self.innode().len
        }
    }
    pub fn is_half_full(&self) -> bool {
        self.len() >= NUM_KEYS / 2
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
            &NodeData::Internal(ref node) => unreachable!(self.type_name()),
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
        }
    }
    pub fn key_at_right_node(&self, key: &EntryKey) -> Option<&NodeCellRef> {
        return None;
        if self.len() > 0 {
            match self {
                &NodeData::Internal(ref n) => {
                    if &n.keys[n.len - 1] < key {
                        let right_node = n.right.read_unchecked();
                        if !right_node.is_none() {
                            let right_innode = right_node.innode();
                            if right_innode.keys.len() > 0 && &right_innode.keys[0] < key {
                                debug!(
                                    "found key to put to right internal page {:?}/{:?}",
                                    key, &right_innode.keys[0]
                                );
                                return Some(&n.right);
                            }
                        }
                    }
                }
                &NodeData::External(ref n) => {
                    if &n.keys[n.len - 1] < key {
                        let right_node = n.next.read_unchecked();
                        if !right_node.is_none() {
                            let right_extnode = right_node.extnode();
                            if right_extnode.keys.len() > 0 && &right_extnode.keys[0] < key {
                                debug!(
                                    "found key to put to right external page {:?}/{:?}",
                                    key, &right_extnode.keys[0]
                                );
                                return Some(&n.next);
                            }
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
        return None;
    }
}

pub fn write_key_page(search_page: NodeWriteGuard, key: &EntryKey) -> NodeWriteGuard {
    return search_page;
    if search_page.len() > 0 {
        match &*search_page {
            &NodeData::Internal(ref n) => {
                if &n.keys[n.len - 1] < key {
                    let right_ref = &n.right;
                    debug!("Obtain latch for internal write key page");
                    let right_node = right_ref.write();
                    if !right_node.is_none()
                        && right_node.len() > 0
                        && &right_node.innode().keys[0] < key
                    {
                        debug_assert!(!right_node.is_ext());
                        debug!("will write to right internal page");
                        return write_key_page(right_node, key);
                    }
                }
            }
            &NodeData::External(ref n) => {
                if &n.keys[n.len - 1] < key {
                    let right_ref = &n.next;
                    debug!("Obtain latch for external write key page");
                    let right_node = right_ref.write();
                    if !right_node.is_none()
                        && right_node.len() > 0
                        && &right_node.extnode().keys[0] < key
                    {
                        debug_assert!(right_node.is_ext());
                        debug!("will write to right external page");
                        return write_key_page(right_node, key);
                    }
                }
            }
            _ => unreachable!(),
        }
        return search_page;
    } else {
        return search_page;
    }
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

    pub fn write(&self) -> NodeWriteGuard {
        let cc = &self.cc;
        loop {
            let cc_num = cc.load(Relaxed);
            let expected = cc_num & (!LATCH_FLAG);
            if cc.compare_and_swap(expected, cc_num | LATCH_FLAG, Relaxed) == expected {
                return NodeWriteGuard {
                    data: self.data.get(),
                    cc: &self.cc as *const AtomicUsize,
                    version: node_version(cc_num),
                };
            }
            debug!("acquire latch failed, retry {:b}", cc_num);
        }
    }

    pub fn read<'a, F: FnMut(&NodeReadHandler) -> R + 'a, R: 'a>(&'a self, mut func: F) -> R {
        let mut handler = NodeReadHandler {
            ptr: self.data.get(),
            version: 0,
        };
        let cc = &self.cc;
        loop {
            let cc_num = cc.load(Relaxed);
            if cc_num & LATCH_FLAG == LATCH_FLAG {
                debug!("read have a latch, retry {:b}", cc_num);
                continue;
            }
            handler.version = cc_num & (!LATCH_FLAG);
            let res = func(&handler);
            let new_cc_num = cc.load(Relaxed);
            if new_cc_num == cc_num {
                return res;
            }
            debug!("read version changed, retry {:b}", cc_num);
        }
    }

    pub fn read_unchecked(&self) -> &NodeData {
        unsafe { &*self.data.get() }
    }

    pub fn version(&self) -> usize {
        node_version(self.cc.load(Relaxed))
    }
}

pub fn node_version(cc_num: usize) -> usize {
    cc_num & (!LATCH_FLAG)
}

pub struct NodeWriteGuard {
    pub data: *mut NodeData,
    pub cc: *const AtomicUsize,
    pub version: usize,
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
            let cc_num = cc.load(Relaxed);
            cc.store((cc_num & (!LATCH_FLAG)) + 1, Relaxed);
        }
    }
}

impl Default for NodeWriteGuard {
    fn default() -> Self {
        NodeWriteGuard {
            data: 0 as *mut NodeData,
            cc: 0 as *const AtomicUsize,
            version: 0,
        }
    }
}

pub struct NodeReadHandler {
    pub ptr: *const NodeData,
    pub version: usize,
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
