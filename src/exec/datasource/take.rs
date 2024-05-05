use super::DataSource;

pub struct Take<T: Clone> {
    count: usize,
    limit: usize,
    data: T
}

impl <T: Clone> DataSource<T, (T, usize)> for Take<T> {
    fn init(params: (T, usize)) -> Self {
        let (data, limit) = params;
        Self { data, limit, count: 0}
    }
}

impl <T: Clone> Iterator for Take<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count >= self.limit {
            return None;
        }
        Some(self.data.clone())
    }
}