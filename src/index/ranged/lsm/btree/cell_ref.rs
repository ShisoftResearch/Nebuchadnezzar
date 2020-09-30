use super::*;

pub struct NodeCellRef {
    pub inner: Arc<dyn AnyNode>,
}

unsafe impl Send for NodeCellRef {}
unsafe impl Sync for NodeCellRef {}

impl NodeCellRef {
    pub fn new<KS, PS>(node: Node<KS, PS>) -> Self
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
    {
        NodeCellRef {
            inner: Arc::new(node),
        }
    }

    pub fn new_none<KS, PS>() -> Self
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
    {
        Node::<KS, PS>::none_ref()
    }

    #[inline]
    pub fn deref<KS, PS>(&self) -> &Node<KS, PS>
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
    {
        // The only unmatched scenario is the NodeCellRef was constructed by default function
        // Because the size of different type of NodeData are the same, we can still cast them safely
        // for NodeData have a fixed size for all the time
        debug_assert!(
            self.inner.is_type::<Node<KS, PS>>(),
            "Node ref type unmatched, is default: {}",
            self.is_default()
        );
        unsafe { &*(self.inner.deref() as *const dyn AnyNode as *const Node<KS, PS>) }
    }

    pub fn is_default(&self) -> bool {
        self.inner
            .is_type::<Node<DefaultKeySliceType, DefaultPtrSliceType>>()
    }

    pub fn to_string<KS, PS>(&self) -> String
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
    {
        if self.is_default() {
            String::from("<<DEFAULT>>")
        } else {
            let node = read_unchecked::<KS, PS>(self);
            if node.is_none() {
                String::from("<NONE>")
            } else if node.is_empty_node() {
                String::from("<EMPTY>")
            } else {
                format!("{:?}", node.first_key())
            }
        }
    }

    pub fn persist(
        &self,
        deletion: &DeletionSet,
        neb: &Arc<crate::client::AsyncClient>,
    ) -> BoxFuture<()> {
        self.inner.persist(self, deletion, neb)
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Clone for NodeCellRef {
    fn clone(&self) -> Self {
        NodeCellRef {
            inner: self.inner.clone(),
        }
    }
}

impl Default for NodeCellRef {
    fn default() -> Self {
        let data: DefaultNodeDataType = NodeData::None;
        Self::new(Node::new(data))
    }
}