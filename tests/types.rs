use neb::ram::types;
use neb::ram::chunk;
use std::*;

macro_rules! test_nums {
    (
        $sym:ident, $io:ident, $name:ident
    ) => (
        #[test]
        fn $name () {
            let test_data = vec![$sym::MIN, $sym::MAX, 0, 1, 2, 255];
            let chunk = &chunk::init(1, 2048).list[0];
            for d in test_data {
                types::$io::write(d, chunk.addr);
                assert!(types::$io::read(chunk.addr) == d);
            }
        }
    );
}

#[test]
fn init () {

}

test_nums!(i8, i8_io, i8_test);