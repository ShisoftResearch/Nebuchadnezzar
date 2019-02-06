macro_rules! make_array {
    ($n: expr, $constructor:expr) => {
        unsafe {
            let mut items: [_; $n] = mem::uninitialized();
            for place in items.iter_mut() {
                ptr::write(place, $constructor);
            }
            items
        }
    };
}

macro_rules! impl_slice_ops {
    ($t: ty, $et: ty, $n: expr) => {
        impl Slice<$et> for $t {
            fn as_slice(&mut self) -> &mut [$et] {
                self
            }
            fn init() -> Box<Self> {
                box make_array!($n, Self::item_default())
            }
            fn slice_len() -> usize {
                $n
            }
        }
    };
}

macro_rules! with_levels {
    ($($sym:ident, $level_size:ident;)*) => {
        $(
            mod $sym {
                use super::*;
                use smallvec::Array;
                use std::fmt;

                pub struct KeySlice {
                    inner: [Key; $level_size]
                }

                pub struct PtrSlice {
                    inner: [Ptr; $level_size + 1]
                }

                impl Slice<Key> for KeySlice {
                    fn as_slice(&mut self) -> &mut [Key] {
                        &mut self.inner
                    }
                    fn init() -> Box<Self> {
                        box Self {
                            inner: make_array!($level_size, Self::item_default())
                        }
                    }
                    fn slice_len() -> usize {
                        $level_size
                    }
                }

                impl Slice<Ptr> for PtrSlice {
                    fn as_slice(&mut self) -> &mut [Ptr] {
                        &mut self.inner
                    }
                    fn init() -> Box<Self> {
                        box Self {
                            inner: make_array!($level_size + 1, Self::item_default())
                        }
                    }
                    fn slice_len() -> usize {
                        $level_size + 1
                    }
                }

                pub type Tree = BPlusTree<KeySlice, PtrSlice>;

                unsafe impl Array for KeySlice {
                    type Item = Key;
                    fn size() -> usize { $level_size }
                    fn ptr(&self) -> *const Key { self.inner.as_ptr() }
                    fn ptr_mut(&mut self) -> *mut Key { self.inner.as_mut_ptr() }
                }
                unsafe impl Array for PtrSlice {
                    type Item = Ptr;
                    fn size() -> usize { $level_size + 1 }
                    fn ptr(&self) -> *const Ptr { self.inner.as_ptr() }
                    fn ptr_mut(&mut self) -> *mut Ptr { self.inner.as_mut_ptr() }
                }

                impl fmt::Debug for KeySlice {
                    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                        f.debug_list().entries(self.inner.iter()).finish()
                    }
                }
            }
        )*

        fn init_lsm_level_trees(neb_client: &Arc<AsyncClient>) -> (LevelTrees, Vec<usize>) {
            let mut trees = LevelTrees::new();
            let mut max_elements_list = Vec::new();
            let mut max_elements = LEVEL_M_MAX_ELEMENTS_COUNT;
            debug!("Initialize level trees...");
            $(
                debug!("Initialize tree {}...", stringify!($sym));
                trees.push(box $sym::Tree::new(neb_client));
                debug!("Tree {} initialized...", stringify!($sym));
                const_assert!($level_size * KEY_SIZE <= MAX_SEGMENT_SIZE);
                max_elements *= LEVEL_ELEMENTS_MULTIPLIER;
                max_elements_list.push(max_elements);
            )*
            debug!("Level trees initialized...");
            return (trees, max_elements_list);
        }
    };
}
