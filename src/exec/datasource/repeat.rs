use std::{
    pin::Pin,
    task::{Context, Poll},
};

use dovahkiin::expr::Value;
use futures::{Future, Stream};

use super::DataSource;
use futures::future;

pub struct Repeat<T: Clone> {
    data: T,
}

impl<T: Clone> DataSource<T, T> for Repeat<T> {
    fn init(params: T) -> impl Future<Output = Result<Self, String>> {
        future::ready(Result::Ok(Self { data: params }))
    }
}

impl<T: Clone> Stream for Repeat<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(Some(self.data.clone()))
    }
}

pub type RepeatValue<'a> = Repeat<Value<'a>>;
