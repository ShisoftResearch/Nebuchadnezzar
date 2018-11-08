mod level;

const LEVEL_M: usize = super::btree::NUM_KEYS;
const LEVEL_1: usize = LEVEL_M * 10;
const LEVEL_2: usize = LEVEL_1 * 10;
const LEVEL_3: usize = LEVEL_2 * 10;
const LEVEL_4: usize = LEVEL_3 * 10;
// TODO: debug assert the last one will not overflow MAX_SEGMENT_SIZE

macro_rules! with_level {
    () => {};
}