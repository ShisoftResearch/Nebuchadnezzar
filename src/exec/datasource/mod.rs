pub mod repeat;
pub mod take;

pub trait DataSource<T, P>: Iterator<Item = T> {
    fn init(params: P) -> Self;
}