use rand::{OsRng, Rng};
use parking_lot::Mutex;

pub struct RandGen {
    generator: OsRng
}

impl RandGen {
    fn new() -> RandGen {
        RandGen {
            generator: match OsRng::new() {
                Ok(g) => g,
                Err(e) => panic!("Failed to obtain OS RNG: {}", e)
            }
        }
    }
    fn next(&mut self) -> u64 {
        self.generator.next_u64()
    }
    fn next_two(&mut self) -> (u64, u64) {
        (self.next(), self.next())
    }
}

lazy_static! {
    pub static ref RAND_GEN: Mutex<RandGen> = Mutex::new(RandGen::new());
}

pub fn next() -> u64 {
    RAND_GEN.lock().next()
}

pub fn next_two() -> (u64, u64) {
    RAND_GEN.lock().next_two()
}