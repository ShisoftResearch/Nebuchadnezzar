pub mod aggregate;
pub mod mapper;
pub mod reducer;

pub trait Function<I, O>: Sync + Send {
    fn exec(input: &[I]) -> Vec<O>;
}
