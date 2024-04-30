pub mod sum;
pub mod max;
pub mod min;
pub mod count;
pub mod any;
pub mod all;
pub mod average;

pub trait Aggregator<I, O> {
    fn collect(&mut self, value: I);
    fn fold(&mut self, other: &Self);
    fn finish(self) -> Option<O>;
}
