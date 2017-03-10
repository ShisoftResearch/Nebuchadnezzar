use ram::cell::{Cell, ReadError, WriteError};

service! {
    rpc read_cell(key: (u64, u64)) -> Result<Cell, ReadError>;
    rpc write_cell(cell: Cell) -> Result<Cell, WriteError>;
    rpc update_cell(cell: Cell) -> Result<Cell, WriteError>;
    rpc remove_cell(key: (u64, u64)) -> Result<(), WriteError>;
}