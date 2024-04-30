pub trait Aggregate<I, O> {
    fn collect(&self, value: I);
    fn finish(&self) -> O;
}