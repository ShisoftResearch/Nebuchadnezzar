macro_rules! make_array {
    ($n: expr) => {
        unsafe { mem::zeroed() }
    };
}

macro_rules! impl_slice_ops {
    ($t: ty, $et: ty, $n: expr) => {
        impl Slice<$et> for $t {
            const SLICE_LEN: usize = $n;
            fn as_slice(&mut self) -> &mut [$et] {
                self
            }
            // Overrides the trait default, which manufactures a mutable
            // reference from a shared one (undefined behavior).
            fn as_slice_immute(&self) -> &[$et] {
                self
            }
            fn init() -> Self {
                make_array!($n)
            }
        }
    };
}
