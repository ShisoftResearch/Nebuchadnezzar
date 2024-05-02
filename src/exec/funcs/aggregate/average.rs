// TODO: Use a post processor

// use std::ops::Add;

// use dovahkiin::types::Value;

// use super::Aggregator;

// pub struct Average<T> {
//     accumlator: T,
//     count: u64,
// }

// impl<T: Value + Clone + Add<Output = T>> Aggregator<T, T> for Average<T> {
//     fn collect(&mut self, value: T) {
//         self.accumlator = self.accumlator.clone() + value;
//         self.count += 1;
//     }

//     fn fold(&mut self, other: &Self) {
//         self.accumlator = self.accumlator.clone() + other.accumlator.clone();
//         self.count += other.count;
//     }

//     fn finish(self) -> Option<T> {
//         unimplemented!()
//     }
// }
