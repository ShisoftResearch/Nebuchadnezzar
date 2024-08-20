use super::Adapter;

pub struct Take<T> {
    current: usize,
    limit: usize,
    iter: Box<dyn Iterator<Item = T>>,
}

impl<T> Adapter<T, T, usize> for Take<T> {
    fn from(input: impl Iterator<Item = T> + 'static, limit: usize) -> Result<Self, String> {
        let iter: Box<dyn Iterator<Item = T>> = Box::new(input);
        return Ok(Self {
            current: 0,
            iter,
            limit,
        });
    }
}

impl<T> Iterator for Take<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.limit {
            return None;
        }
        self.current += 1;
        self.iter.next()
    }
}
