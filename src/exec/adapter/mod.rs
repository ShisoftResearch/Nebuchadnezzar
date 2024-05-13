pub mod id_cell;
pub mod id_cell_select;
pub mod owned_cell;
pub mod take;

pub trait Adapter<I, O, P>: Iterator<Item = O> + Sized {
    fn from(input: impl Iterator<Item = I> + 'static, params: P) -> Result<Self, String>;
}
