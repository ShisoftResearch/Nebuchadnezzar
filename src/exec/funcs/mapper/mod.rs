pub mod map;
pub mod filter;
pub mod filter_map;


pub trait Mapper<I, O, F, FI, FO> 
    where F: Fn(FI) -> FO 
{
    fn map(&self, data: impl Iterator<Item = I>, func: F) -> impl Iterator<Item = O>;
}