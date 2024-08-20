pub mod all;
pub mod any;
pub mod average;
pub mod count;
pub mod find;
pub mod max;
pub mod min;
pub mod sum;

pub trait Aggregator<I, O, F, FI, FO>
where
    F: Fn(&mut Self, FI) -> FO,
{
    fn collect(&mut self, value: I, func: F);
    fn fold(&mut self, other: Self);
    fn finish(self) -> O;
}

pub type NoOp<S> = fn(&mut S, ());
