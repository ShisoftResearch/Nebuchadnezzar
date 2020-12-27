use super::*;
use futures::prelude::*;
use futures::FutureExt;
use mem::forget;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::VecDeque;

type DefaultKeySliceType = [EntryKey; 0];
type DefaultPtrSliceType = [NodeCellRef; 0];
type DefaultNodeType = Node<DefaultKeySliceType, DefaultPtrSliceType>;

lazy_static! {
    pub static ref DEFAULT_NODE: DefaultNodeType = Node::new(NodeData::None);
    pub static ref DEFAULT_NODE_DATA: NodeData<DefaultKeySliceType, DefaultPtrSliceType> =
        NodeData::None;
}

#[derive(Debug)]
pub struct NodeCellRef {
    inner: *mut NodeRefInner<dyn AnyNode>,
}

struct NodeRefInner<T: ?Sized> {
    #[cfg(debug_assertions)]
    backtrace: String,
    counter: AtomicUsize,
    obj: T,
}

unsafe impl Send for NodeCellRef {}
unsafe impl Sync for NodeCellRef {}
unsafe impl<T: ?Sized> Send for NodeRefInner<T> {}
unsafe impl<T: ?Sized> Sync for NodeRefInner<T> {}

impl NodeCellRef {
    pub fn new<KS, PS>(node: Node<KS, PS>) -> Self
    where
        KS: Slice<EntryKey> + Debug + 'static,
        PS: Slice<NodeCellRef> + 'static,
    {
        let node_ref: Box<NodeRefInner<dyn AnyNode>> = Box::new(NodeRefInner {
            #[cfg(debug_assertions)]
            backtrace: String::from(""),
            counter: AtomicUsize::new(1),
            obj: node,
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
                return ((&*DEFAULT_NODE) as *const DefaultNodeType as usize
                    as *const Node<KS, PS>)
                    .as_ref()
                    .unwrap();
            }
            let inner = self.inner.as_ref().unwrap();
            ((&inner.obj) as *const dyn AnyNode as *const Node<KS, PS>)
                .as_ref()
                .unwrap()
        }
    }

    pub fn address(&self) -> usize {
        self.inner as *const NodeRefInner<DefaultNodeType> as usize
    }

    pub fn is_default(&self) -> bool {
        self.address() == 0
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
                return self
                    .inner
                    .as_ref()
                    .unwrap()
                    .obj
                    .persist(self, deletion, neb)
                    .boxed();
            }
        }
        future::ready(()).boxed()
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }

    pub fn num_references(&self) -> usize {
        unsafe {
            self.inner.as_ref().unwrap().counter.load(Ordering::Acquire)
        }
    }
    
    #[cfg(debug_assertions)]
    pub unsafe fn capture_backtrace(&self) {
        self.inner.as_mut().unwrap().backtrace = format!("{:?}", std::thread::current().id());
    }

    #[cfg(debug_assertions)]
    pub unsafe fn get_backtrace(&self) -> &String {
        &self.inner.as_ref().unwrap().backtrace
    }
}

impl Clone for NodeCellRef {
    fn clone(&self) -> Self {
        if !self.is_default() {
            unsafe {
                let inner = self.inner.as_ref().unwrap();
                inner.counter.fetch_add(1, Ordering::AcqRel);
                return NodeCellRef { inner: self.inner };
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
                let c = inner.counter.fetch_sub(1, Ordering::AcqRel);
                if c == 1 {
                    let mut stack = VecDeque::new();
                    stack.push_front(Box::from_raw(self.inner));
                    let mut freed_ref_count = 0;
                    while let Some(node) = stack.pop_front() {
                        freed_ref_count += 1;
                        let all_sub_refs = node.obj.take_all_refs();
                        trace!("Reference {:?} have {} sub refrences", node.as_ref() as *const _, all_sub_refs.len());
                        for r in all_sub_refs.into_iter() {
                            let sub_ref_rc = r.inner.as_ref().unwrap().counter.fetch_sub(1, Ordering::AcqRel);
                            trace!("Sub ref {:?} have rc {}", r.inner, sub_ref_rc);
                            if sub_ref_rc <= 1 {
                                // Can be eager-dropped
                                stack.push_front(Box::from_raw(r.inner));
                            }
                            forget(r);
                        }
                    }
                    trace!("Freed {} objects with reference counting", freed_ref_count);
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

impl PartialEq for NodeCellRef {
    fn eq(&self, other: &Self) -> bool {
        self.address() == other.address()
    }
}
