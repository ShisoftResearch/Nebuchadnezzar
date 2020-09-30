macro_rules! make_array {
    ($n: expr) => {
        unsafe {
            mem::zeroed()
        }
    };
}

macro_rules! impl_slice_ops {
    ($t: ty, $et: ty, $n: expr) => {
        impl Slice<$et> for $t {
            fn as_slice(&mut self) -> &mut [$et] {
                self
            }
            fn init() -> Self {
                make_array!($n)
            }
            fn slice_len() -> usize {
                $n
            }
        }
    };
}
