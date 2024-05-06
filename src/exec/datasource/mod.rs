use futures::Stream;

pub mod repeat;
pub mod tree_index;

pub trait DataSource<T, P>: Stream<Item = T> {
    fn init(params: P) -> Self;
}
