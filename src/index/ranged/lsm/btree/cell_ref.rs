use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use futures::FutureExt;
use futures::prelude::*;

type DefaultKeySliceType = [EntryKey; 0];
type DefaultPtrSliceType = [NodeCellRef; 0];
type DefaultNodeType = Node<DefaultKeySliceType, DefaultPtrSliceType>;

lazy_static! {
    static ref DEFAULT_NODE: DefaultNodeType = Node::new(NodeData::None);
}

pub struct NodeCellRef {
    inner: *mut NodeRefInner<dyn AnyNode>,
}

struct NodeRefInner<T: ?Sized> {
    counter: AtomicUsize,
    obj: T
}

unsafe impl Send for NodeCellRef {}
unsafe impl Sync for NodeCellRef {}
unsafe impl <T: ?Sized> Send for NodeRefInner<T> {}
unsafe impl <T: ?Sized> Sync for NodeRefInner<T> {}

impl NodeCellRef {
    pub fn new<KS, PS>(node: Node<KS, PS>) -> Self
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
    {
        let node_ref: Box<NodeRefInner<dyn AnyNode>> = Box::new(NodeRefInner {
            counter: AtomicUsize::new(1),
            obj: node
        });
        Self {
            inner: Box::into_raw(node_ref),
        }
    }

    pub fn new_none<KS, PS>() -> Self
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
    {
        Self::default()
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
        unsafe {
            if self.is_default() {
                return ((&*DEFAULT_NODE) as *const DefaultNodeType as usize as *const Node<KS, PS>).as_ref().unwrap()
            }
            let inner = self.inner.as_ref().unwrap();
            ((&inner.obj) as *const dyn AnyNode as *const Node<KS, PS>).as_ref().unwrap()
        }
    }

    pub fn is_default(&self) -> bool {
        self.inner as *const NodeRefInner<DefaultNodeType> as usize == 0
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
        if !self.is_default() {
            unsafe {
                return self.inner.as_ref().unwrap().obj.persist(self, deletion, neb).boxed();
            }
        }
        future::ready(()).boxed()
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl Clone for NodeCellRef {
    fn clone(&self) -> Self {
        if !self.is_default() {
            unsafe {
                let inner = self.inner.as_ref().unwrap();
                inner.counter.fetch_add(1, Ordering::Relaxed);
                return NodeCellRef {
                    inner: self.inner
                }
            }
        }
        Self::default()
    }
}

impl Drop for NodeCellRef {
    fn drop(&mut self) {
        if !self.is_default() {
            unsafe {
                let inner = self.inner.as_ref().unwrap();
                let c = inner.counter.fetch_sub(1, Ordering::Relaxed);
                if c == 1 {
                    Box::from_raw(self.inner);
                }
            }
        }
    }
}

impl Default for NodeCellRef {
    fn default() -> Self {
        Self {
            inner: 0usize as *mut NodeRefInner<DefaultNodeType>,
        }
    }
}