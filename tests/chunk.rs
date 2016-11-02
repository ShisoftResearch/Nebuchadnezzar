use neb::ram::chunk;
use env_logger;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std;

#[test]
pub fn round_robin_segment () {
    let num = AtomicU8::new(std::u8::MAX);
    assert_eq!(num.load(Ordering::SeqCst), 255);
    assert_eq!(num.fetch_add(1, Ordering::SeqCst), 255);
    assert_eq!(num.fetch_add(1, Ordering::SeqCst), 0);
    assert_eq!(num.fetch_add(1, Ordering::SeqCst), 1);
}