pub mod repeat;

pub trait DataSource<T, P>: Iterator<Item = T> {
    fn init(params: P) -> Self;
}