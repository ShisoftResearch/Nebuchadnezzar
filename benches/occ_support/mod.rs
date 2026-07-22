pub mod fixture;
pub mod metrics;
pub mod workloads;

pub const DEFAULT_BASE_PORT: u16 = 39_400;

pub fn base_port() -> u16 {
    std::env::var("NEB_OCC_BENCH_BASE_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(DEFAULT_BASE_PORT)
}
