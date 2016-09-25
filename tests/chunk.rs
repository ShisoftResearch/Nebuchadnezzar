use neb::ram::chunk;
use env_logger;

#[test]
fn create_chunks () {
    let _ = env_logger::init();
    let _ = chunk::init(2, 4 * 1024 * 1024);
}