pub mod raii_mutex_table;

pub fn upper_power_of_2(mut v: usize) -> usize {
    debug_assert!(v > 0);
    v -= 1;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v += 1;
    return v;
}
