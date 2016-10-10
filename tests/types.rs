use neb::ram::types;
use neb::ram::chunk;

pub const CHUNK_SIZE: usize = 2048;

macro_rules! test_nums {
    (
        $t:ident, $io:ident
    ) => (
        mod $t {
            use neb::ram::types;
            use neb::ram::chunk;
            use std;
            use rand;
            use super::CHUNK_SIZE;
            #[test]
            fn test () {
                let rand_num = rand::random::<$t>();
                let test_data = vec![std::$t::MIN, std::$t::MAX, rand_num, 0 as $t, 1 as $t, 2 as $t, 127 as $t];
                let chunk = &chunk::init(1, CHUNK_SIZE).list[0];
                let counts = CHUNK_SIZE / types::$io::size(0) as usize;
                for d in test_data {
                    for i in 0..counts {
                        let addr = chunk.addr + i * types::$io::size(0);
                        types::$io::write(d, addr);
                        assert!(types::$io::read(addr) == d);
                    }
                    for i in 0..counts {
                        let addr = chunk.addr + i * types::$io::size(0);
                        assert!(types::$io::read(addr) == d);
                    }
                }
            }
        }
    );
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

mod pos2d32 {
    use neb::ram::types;
    use neb::ram::chunk;
    use std;
    use rand;
    use super::CHUNK_SIZE;
    #[test]
    fn test () {
        let test_data = vec![
            types::pos2d32 {x: std::f32::MIN, y: std::f32::MAX},
            types::pos2d32 {x: rand::random::<f32>(), y: rand::random::<f32>()}
        ];
        let chunk = &chunk::init(1, CHUNK_SIZE).list[0];
        let counts = CHUNK_SIZE / types::pos2d32_io::size(0) as usize;
        for d in test_data {
            for i in 0..counts {
                let addr = chunk.addr + i * types::pos2d32_io::size(0);
                types::pos2d32_io::write(&d, addr);
                assert!(types::pos2d32_io::read(addr) == d);
            }
            for i in 0..counts {
                let addr = chunk.addr + i * types::pos2d32_io::size(0);
                assert!(types::pos2d32_io::read(addr) == d);
            }
        }
    }
}

mod pos2d64 {
    use neb::ram::types;
    use neb::ram::chunk;
    use std;
    use rand;
    use super::CHUNK_SIZE;
    #[test]
    fn test () {
        let test_data = vec![
            types::pos2d64 {x: std::f64::MIN, y: std::f64::MAX},
            types::pos2d64 {x: rand::random::<f64>(), y: rand::random::<f64>()}
        ];
        let chunk = &chunk::init(1, CHUNK_SIZE).list[0];
        let counts = CHUNK_SIZE / types::pos2d64_io::size(0) as usize;
        for d in test_data {
            for i in 0..counts {
                let addr = chunk.addr + i * types::pos2d64_io::size(0);
                types::pos2d64_io::write(&d, addr);
                assert!(types::pos2d64_io::read(addr) == d);
            }
            for i in 0..counts {
                let addr = chunk.addr + i * types::pos2d64_io::size(0);
                assert!(types::pos2d64_io::read(addr) == d);
            }
        }
    }
}

mod pos3d32 {
    use neb::ram::types;
    use neb::ram::chunk;
    use std;
    use rand;
    use super::CHUNK_SIZE;
    #[test]
    fn test () {
        let test_data = vec![
            types::pos3d32 {x: std::f32::MIN, y: std::f32::MAX, z: rand::random::<f32>()},
            types::pos3d32 {x: rand::random::<f32>(), y: rand::random::<f32>(), z: rand::random::<f32>()}
        ];
        let chunk = &chunk::init(1, CHUNK_SIZE).list[0];
        let counts = CHUNK_SIZE / types::pos3d32_io::size(0) as usize;
        for d in test_data {
            for i in 0..counts {
                let addr = chunk.addr + i * types::pos3d32_io::size(0);
                types::pos3d32_io::write(&d, addr);
                assert!(types::pos3d32_io::read(addr) == d);
            }
            for i in 0..counts {
                let addr = chunk.addr + i * types::pos3d32_io::size(0);
                assert!(types::pos3d32_io::read(addr) == d);
            }
        }
    }
}

mod pos3d64 {
    use neb::ram::types;
    use neb::ram::chunk;
    use std;
    use rand;
    use super::CHUNK_SIZE;
    #[test]
    fn test () {
        let test_data = vec![
            types::pos3d64 {x: std::f64::MIN, y: std::f64::MAX, z: rand::random::<f64>()},
            types::pos3d64 {x: rand::random::<f64>(), y: rand::random::<f64>(), z: rand::random::<f64>()}
        ];
        let chunk = &chunk::init(1, CHUNK_SIZE).list[0];
        let counts = CHUNK_SIZE / types::pos3d64_io::size(0) as usize;
        for d in test_data {
            for i in 0..counts {
                let addr = chunk.addr + i * types::pos3d64_io::size(0);
                types::pos3d64_io::write(&d, addr);
                assert!(types::pos3d64_io::read(addr) == d);
            }
            for i in 0..counts {
                let addr = chunk.addr + i * types::pos3d64_io::size(0);
                assert!(types::pos3d64_io::read(addr) == d);
            }
        }
    }
}

mod uuid {
    use neb::ram::types;
    use neb::ram::chunk;
    use uuid::Uuid;
    use uuid;
    use super::CHUNK_SIZE;
    #[test]
    fn test () {
        let test_data = vec![
            Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap(),
            Uuid::new_v4(),
            Uuid::new_v5(&uuid::NAMESPACE_DNS, "foo")
        ];
        let chunk = &chunk::init(1, CHUNK_SIZE).list[0];
        let counts = CHUNK_SIZE / types::uuid_io::size(0) as usize;
        for d in test_data {
            for i in 0..counts {
                let addr = chunk.addr + i * types::uuid_io::size(0);
                types::uuid_io::write(&d, chunk.addr);
                assert!(types::uuid_io::read(chunk.addr) == d);
            }
            for i in 0..counts {
                let addr = chunk.addr + i * types::uuid_io::size(0);
                assert!(types::uuid_io::read(chunk.addr) == d);
            }
        }
    }
}

mod string {
    use neb::ram::types;
    use neb::ram::chunk;
    use std::string::String;
    use super::CHUNK_SIZE;
    #[test]
    fn test () {
        let test_data = vec![
            "", "ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ", "中文测试文本", "Hello Test", "💖"
        ];
        let chunk = &chunk::init(1, CHUNK_SIZE).list[0];
        let mut addr = chunk.addr;
        for d in test_data.clone() {
            let test_str = String::from(d);
            types::string_io::write(&test_str, addr);
            let len = types::string_io::size(addr);
            assert!(types::string_io::read(addr) == test_str);
            addr += len;
        }
        addr = chunk.addr;
        for d in test_data.clone() {
            let test_str = String::from(d);
            let len = types::string_io::size(addr);
            assert!(types::string_io::read(addr) == test_str);
            addr += len;
        }
    }
}