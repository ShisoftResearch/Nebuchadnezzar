#[macro_export]
macro_rules! impl_btree_level {
    ($items: expr) => {
        impl_slice_ops!([EntryKey; $items], EntryKey, $items);
        impl_slice_ops!([NodeCellRef; $items + 1], NodeCellRef, $items + 1);
    };
}
