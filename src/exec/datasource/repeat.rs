use super::DataSource;

pub struct Repeat<T: Clone> {
    data: T
}

impl <T: Clone> DataSource<T, T> for Repeat<T> {
    fn init(params: T) -> Self {
        Self { data: params }
    }
}

impl <T: Clone> Iterator for Repeat<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.data.clone())
    }
}