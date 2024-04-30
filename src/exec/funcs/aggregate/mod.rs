pub mod sum;
pub mod max;
pub mod min;
pub mod count;
pub mod any;
pub mod all;

pub trait Aggregator<I, O> {
    fn collect(&mut self, value: I);
    fn collect_internal(&mut self, internal: O);
    fn finish(self) -> Option<O>;
}
