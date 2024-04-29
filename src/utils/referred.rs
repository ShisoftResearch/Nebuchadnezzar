use std::{ops::Deref, sync::atomic::AtomicUsize};
use std::sync::atomic::Ordering::Relaxed;
use std::fmt::Debug;

struct ARefInner<T> {
    obj: T,
    rc: AtomicUsize
}

pub struct ARef<T>(*mut ARefInner<T>);

impl <T> ARef<T> {
    pub fn new(obj: T) -> Self {
        let inner = ARefInner {
            obj, rc: AtomicUsize::new(1)
        };
        Self(Box::into_raw(Box::new(inner)))
    }
}

impl <T: Clone> ARef<T> {
    pub fn clone_referred(&self) -> T {
        let inner = unsafe { &*self.0 };
        inner.obj.clone()
    }
}

impl <T: Debug> Debug for ARef<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ARef").field(&self.0).finish()
    }
}

impl <T> Deref for ARef<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        let inner = unsafe { &*self.0 };
        &inner.obj
    }
}

impl <T> Clone for ARef<T> {
    fn clone(&self) -> Self {
        let inner = unsafe { &*self.0 };
        inner.rc.fetch_add(1, Relaxed);
        Self(self.0)
    }
}

impl <T> Drop for ARef<T> {
    fn drop(&mut self) {
        let inner = unsafe { &*self.0 };
        let old_rc = inner.rc.fetch_sub(1, Relaxed);
        if old_rc == 1 {
            unsafe {
                drop(Box::from_raw(self.0));
            }
        }
    }
}