pub mod reducer;
pub mod mapper;
pub mod aggregate;

pub trait Function<I, O>: Sync + Send {
    fn exec(input: &[I]) -> Vec<O>;
}