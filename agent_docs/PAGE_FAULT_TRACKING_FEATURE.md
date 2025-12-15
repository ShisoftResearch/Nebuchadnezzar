# Page Fault Tracking Feature

## Overview

The `page_fault_tracking` feature enables zero-overhead memory access tracking for tiered memory management using mprotect/SIGSEGV signal handlers.

## When to Enable

- **Tests**: Disabled by default to avoid signal handler conflicts
- **Production**: Can be enabled for zero-overhead performance (optional)

## How It Works

### With Feature Disabled (Default)

```rust
// Direct reference marking during reads
1. Read operation marks segment as referenced immediately
2. Small overhead (~1 atomic operation) but no signal handlers
3. Safer for tests and debugging
```

### With Feature Enabled

```rust
// Zero-overhead tracking via signal handlers
1. CLOCK clears reference bit → calls mprotect(PROT_NONE)
2. First access triggers SIGSEGV → handler sets reference bit + unprotects
3. Subsequent accesses have zero overhead until CLOCK re-arms
```

## Usage

### Disable (Default)

```bash
cargo build  # Direct reference marking (no signal handlers)
cargo test   # Direct reference marking (no signal handlers)
```

### Enable

```bash
cargo build --features page_fault_tracking
cargo test --features page_fault_tracking -- --test-threads=1
```

### Enable for Specific Test

```rust
#[cfg_attr(feature = "page_fault_tracking")]
#[tokio::test]
async fn my_test_requiring_signals() {
    // This test only runs when page_fault_tracking is enabled
}
```

## Why Enable?

1. **Performance**: Zero-overhead tracking for hot access patterns
2. **Production**: Optimal for high-throughput workloads
3. **Hot Segments**: Benefits workloads with many repeated accesses to same segments

## Why It's Disabled by Default

1. **Test Isolation**: Signal handlers are global and can conflict between tests
2. **Debugging**: Easier to debug without SIGSEGV handlers interfering
3. **CI/CD**: Some CI environments may have restrictions on signal handling
4. **Safety**: Direct marking is simpler and more predictable

## Performance Impact

- **With feature**: ~1-2μs overhead on first access after protection, then zero overhead
- **Without feature**: ~1 atomic operation per read (negligible, ~5ns)

Both are highly efficient. The difference is mainly in edge cases with very hot access patterns.

## Implementation Details

- **Signal Handler**: Installs SIGSEGV/SIGBUS handlers to catch protection faults
- **mprotect**: Sets segments to PROT_NONE when clearing reference bits
- **Direct Marking**: Falls back to atomic operations when feature is disabled

All tiered memory functionality works identically in both modes.

