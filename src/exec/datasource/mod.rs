use futures::{Future, Stream};

pub mod repeat;
pub mod cell_id;

pub trait DataSource<T, P>: Stream<Item = T> + Sized {
    fn init(params: P) -> impl Future<Output = Result<Self, String>>;
}

