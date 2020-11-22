#[macro_use]
pub mod ring_buffer;
pub mod lru_cache;
pub mod raii_mutex_table;

pub const PAGE_SHIFT: usize = 12; // 4K
pub const PAGE_SIZE: usize = 1 << PAGE_SHIFT;

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
