use std::collections::BTreeMap;
use index::EntryKey;
use index::Slice;
use std::fmt::Debug;
use std::mem;

// LevelTree items cannot been added or removed individually
// Items must been merged from higher level in bulk
// Deletion will be performed when merging to or from higher level tree
// Because tree update will not to be performed in parallel. Unlike memtable, a single r/w lock
// should be sufficient. Thus concurrency control will be simple and efficient.
pub struct LevelTree<S>
    where S: Slice<EntryKey> + SortableSlice<EntryKey>
{
    index: BTreeMap<EntryKey, SSIndex<S>>
}

struct SSIndex<S>
    where S: Slice<EntryKey> + SortableSlice<EntryKey>
{
    slice: S
}

pub trait SortableSlice<T>: Sized + Slice<T>
    where T: Default + Debug + Ord
{
    fn merge_sorted_with(&mut self,  y_slice: &mut [T], xlen: &mut usize, ylen: &mut usize) -> Self {
        let total_len = *xlen + *ylen;
        let mut x_pos = 0;
        let mut y_pos = 0;
        let mut pos = 0;
        let mut new_x = Self::init();
        let mut new_y = Self::init();
        {
            let mut x_slice = self.as_slice();
            let x_slice_len = x_slice.len();
            debug_assert!(*ylen <= x_slice_len);
            let mut new_slice_x = new_x.as_slice();
            let mut new_slice_y = new_y.as_slice();
            let mut new_x_len = 0;
            let mut new_y_len = 0;
            let mut use_slice_at = | slice: &mut[T], pos: &mut usize| {
                let item = mem::replace(&mut slice[*pos], T::default());
                *pos += 1;
                item
            };
            while pos < total_len {
                let item = if x_pos < *xlen && y_pos < *ylen {
                    if x_slice[x_pos] < y_slice[y_pos] {
                        use_slice_at(x_slice, &mut x_pos)
                    } else {
                        use_slice_at(y_slice, &mut y_pos)
                    }
                } else if x_pos < *xlen {
                    use_slice_at(x_slice, &mut x_pos)
                } else if y_pos < *ylen {
                    use_slice_at(y_slice, &mut y_pos)
                } else { unreachable!() };

                if pos < x_slice_len {
                    new_slice_x[pos] = item;
                    new_x_len += 1;
                } else {
                    new_slice_y[pos - x_slice_len] = item;
                    new_y_len += 1;
                }
            }
            *xlen = new_x_len;
            *ylen = new_y_len;
        }
        mem::swap(self, &mut new_x);
        return new_y;
    }
}

#[cfg(test)]
mod test {

}