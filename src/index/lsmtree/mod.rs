#[macro_use]
mod macros;
mod cursor;
mod persistent;
pub mod placement;
pub mod service;
mod split;
#[cfg(test)]
mod test;
pub mod tree;
pub mod client;

pub struct RangedIndexer {}

pub unsafe trait Array {
  /// The type of the array's elements.
  type Item;
  /// Returns the number of items the array can hold.
  fn size() -> usize;
  /// Returns a pointer to the first element of the array.
  fn ptr(&self) -> *const Self::Item;
  /// Returns a mutable pointer to the first element of the array.
  fn ptr_mut(&mut self) -> *mut Self::Item;
}