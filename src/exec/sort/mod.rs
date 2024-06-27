pub mod shared_value;

use serde::{Deserialize, Serialize};

pub trait Sort<T> {
    fn extract(&mut self, x: T) -> Result<i64, String>;
    fn compare(x: i64, y: i64) -> i64;
}

#[derive(Serialize, Deserialize)]
pub struct ExtractedData<T: Serialize> {
    data: T,
    tag: i64,
}
