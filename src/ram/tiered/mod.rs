pub mod clock;
pub mod eviction;
pub mod promotion;
pub mod manager;
pub mod page_fault_tracker;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod bench;

