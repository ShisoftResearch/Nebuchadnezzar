pub mod id_cell;
pub mod id_cell_select;
pub mod owned_cell;
pub mod referred_cell;
pub mod borrow_cell_value;
pub mod take;

pub trait Adapter<I, O, P>: Iterator<Item = O> + Sized {
    fn from(input: impl Iterator<Item = I> + 'static, params: P) -> Result<Self, String>;
}
