macro_rules! make_array {
    ($n: expr, $constructor:expr) => {
        unsafe {
            let mut items: [_; $n] = mem::zeroed();
            for place in items.iter_mut() {
                std::ptr::write(place, $constructor);
            }
            items
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
                make_array!($n, Self::item_default())
            }
            fn slice_len() -> usize {
                $n
            }
        }
    };
}
