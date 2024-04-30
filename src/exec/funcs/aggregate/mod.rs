pub mod all;
pub mod any;
pub mod average;
pub mod count;
pub mod max;
pub mod min;
pub mod sum;

pub trait Aggregator<I, O> {
    fn collect(&mut self, value: I);
    fn fold(&mut self, other: &Self);
    fn finish(self) -> Option<O>;
}
