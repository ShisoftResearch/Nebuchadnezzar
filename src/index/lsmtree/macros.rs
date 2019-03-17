macro_rules! with_levels {
    ($($sym:ident, $level_size:expr;)*) => {
        $(
            mod $sym {
                use super::*;
                use smallvec::Array;
                use std::fmt;

                // use boxed slice for the slice itself may too large to been put on stack
                pub struct KeySlice {
                    inner: Box<[Key; $level_size]>
                }

                pub struct PtrSlice {
                    inner: Box<[Ptr; $level_size + 1]>
                }

                impl Slice<Key> for KeySlice {
                    fn as_slice(&mut self) -> &mut [Key] {
                        &mut *self.inner
                    }
                    fn init() -> Self {
                        Self {
                            inner: box make_array!($level_size, Self::item_default())
                        }
                    }
                    fn slice_len() -> usize {
                        $level_size
                    }
                }

                impl Slice<Ptr> for PtrSlice {
                    fn as_slice(&mut self) -> &mut [Ptr] {
                        &mut *self.inner
                    }
                    fn init() -> Self {
                        Self {
                            inner: box make_array!($level_size + 1, Self::item_default())
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

        fn init_lsm_level_trees() -> (LevelTrees, Vec<usize>) {
            let mut trees = LevelTrees::new();
            let mut max_elements_list = Vec::new();
            debug!("Initialize level trees...");
            $(
                debug!("Initialize tree {}...", stringify!($sym));
                trees.push(box $sym::Tree::new());
                debug!("Tree {} initialized...", stringify!($sym));
                const_assert!($level_size * KEY_SIZE <= MAX_SEGMENT_SIZE);
                let level_size = $level_size * $level_size * $level_size;
                max_elements_list.push(level_size);
            )*
            debug!("Level trees initialized...");
            return (trees, max_elements_list);
        }
    };
}
