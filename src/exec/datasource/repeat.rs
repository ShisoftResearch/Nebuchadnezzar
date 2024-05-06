use std::{
    pin::Pin,
    task::{Context, Poll},
};

use dovahkiin::expr::Value;
use futures::Stream;

use super::DataSource;

pub struct Repeat<T: Clone> {
    data: T,
}

impl<T: Clone> DataSource<T, T> for Repeat<T> {
    fn init(params: T) -> Self {
        Self { data: params }
    }
}

impl<T: Clone> Stream for Repeat<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(Some(self.data.clone()))
    }
}

pub type RepeatValue<'a> = Repeat<Value<'a>>;
