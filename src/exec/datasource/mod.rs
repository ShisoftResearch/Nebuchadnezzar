use futures::{Future, Stream};

pub mod cell_id;
pub mod repeat;

pub trait DataSource<T, P>: Stream<Item = T> + Sized {
    fn init(params: P) -> impl Future<Output = Result<Self, String>>;
}
