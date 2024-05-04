pub trait Adapter<I, O>: Iterator<Item = O> {
    fn from(input: impl Iterator<Item = I>);
}