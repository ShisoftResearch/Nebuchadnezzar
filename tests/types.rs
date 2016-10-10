use neb::ram::types;
use neb::ram::chunk;

macro_rules! test_nums {
    (
        $t:ident, $io:ident
    ) => (
        mod $t {
            use neb::ram::types;
            use neb::ram::chunk;
            use std;
            #[test]
            fn test () {
                let test_data = vec![std::$t::MIN, std::$t::MAX, 0 as $t, 1 as $t, 2 as $t, 255 as $t];
                let chunk = &chunk::init(1, 2048).list[0];
                for d in test_data {
                    types::$io::write(d, chunk.addr);
                    assert!(types::$io::read(chunk.addr) == d);
                }
            }
        }
    );
}

#[test]
fn init () {

}

test_nums!(i8, i8_io);
test_nums!(i16, i16_io);
test_nums!(i32, i32_io);
test_nums!(i64, i64_io);

test_nums!(u8, u8_io);
test_nums!(u16, u16_io);
test_nums!(u32, u32_io);
test_nums!(u64, u64_io);

test_nums!(isize, isize_io);
test_nums!(usize, usize_io);
test_nums!(f32, f32_io);
test_nums!(f64, f64_io);