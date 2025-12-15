# How to Enable Schema-Only Logs with env_logger

## Quick Start

### Option 1: Schema Module Only (Recommended)
```bash
# Show only schema-related logs at INFO level
RUST_LOG=neb::ram::schema=info ./your_binary

# Show all schema logs (including debug and trace)
RUST_LOG=neb::ram::schema=debug ./your_binary

# Show only schema errors
RUST_LOG=neb::ram::schema=warn ./your_binary
```

### Option 2: Schema Module + Initialization
```bash
# Include schema initialization logs from server module
RUST_LOG=neb::ram::schema=info,neb::server=debug ./your_binary
```

### Option 3: Multiple Schema-Related Modules
```bash
# Schema + Raft state machine that manages schemas
RUST_LOG=neb::ram::schema=info,neb::ram::schema::sm=info ./your_binary
```

## Detailed Examples

### Show Only Schema Creation/Deletion Events
```bash
# INFO level shows schema creation/deletion, collisions, etc.
RUST_LOG=neb::ram::schema=info ./your_binary
```

**Expected output:**
```
INFO neb::ram::schema::sm: Schema created in SchemasSM: 12345 (my_schema)
INFO neb::ram::schema: Received schema_added event: schema 12345 (my_schema)
INFO neb::ram::schema: Added schema to local cache: 12345 (my_schema)
WARN neb::ram::schema: Received schema_deleted event for schema: my_schema
WARN neb::ram::schema: Deleted local schema 'my_schema' with id 12345
```

### Show Schema Creation + Debug Details
```bash
# DEBUG level includes updates, subscription events, initialization
RUST_LOG=neb::ram::schema=debug ./your_binary
```

**Expected output (includes DEBUG logs):**
```
DEBUG neb::ram::schema: Initializing local schema cache
DEBUG neb::ram::schema: Importing 42 schemas from cluster
DEBUG neb::ram::schema: Subscribing schema events...
INFO  neb::ram::schema: Received schema_added event: schema 12345 (my_schema)
DEBUG neb::ram::schema: Updating existing schema 12345 (my_schema)
INFO  neb::ram::schema: Added schema to local cache: 12345 (my_schema)
```

### Show Schema Creation + Raft State Machine
```bash
# Include Raft state machine logs that manage schemas
RUST_LOG=neb::ram::schema=info,neb::ram::schema::sm=info ./your_binary
```

### Show Everything Including Trace
```bash
# TRACE level shows individual schema imports during bulk load
RUST_LOG=neb::ram::schema=trace ./your_binary
```

**Expected output:**
```
TRACE neb::ram::schema: Importing schema test_schema_1
TRACE neb::ram::schema: Importing schema test_schema_2
...
```

## Filtering by Log Level

### Only Errors and Warnings
```bash
RUST_LOG=neb::ram::schema=warn ./your_binary
```

Shows:
- Schema deletion events
- Schema name collisions (ERROR)
- Failed deletion attempts

### Only Errors
```bash
RUST_LOG=neb::ram::schema=error ./your_binary
```

Shows:
- Schema name collisions
- Failed schema deletion attempts

## Combining Multiple Filters

### Schema + Cell Operations (for debugging schema reads)
```bash
RUST_LOG=neb::ram::schema=info,neb::ram::cell=warn ./your_binary
```

Shows:
- Schema creation/deletion
- Schema-not-found errors from cell reads

### Schema + Server Initialization
```bash
RUST_LOG=neb::ram::schema=info,neb::server=debug ./your_binary
```

Shows:
- Schema operations
- Server startup including schema cache initialization

## Suppressing Other Logs

### Show ONLY Schema Logs (Silence Everything Else)
```bash
# Set everything to error level, then enable schema at info
RUST_LOG=error,neb::ram::schema=info ./your_binary
```

### Show Schema + Errors from Other Modules
```bash
# Schema at info, everything else at error
RUST_LOG=error,neb::ram::schema=info ./your_binary
```

## Module Paths for Schema Logs

Based on the codebase structure:

- `neb::ram::schema` - Main schema cache and LocalSchemasMap
  - `neb::ram::schema::sm` - Raft state machine (SchemasSM)
  - `neb::ram::schema::LocalSchemasCache` - Local cache implementation
  - `neb::ram::schema::LocalSchemasMap` - Internal map

## Common Use Cases

### Debugging "Schema Not Found" Errors
```bash
# Show schema operations + cell reads that fail
RUST_LOG=neb::ram::schema=info,neb::ram::cell=warn ./your_binary
```

### Monitoring Schema Creation
```bash
# Watch for new schemas being added
RUST_LOG=neb::ram::schema=info ./your_binary | grep -i "schema.*added\|schema.*created"
```

### Tracking Schema Deletions
```bash
# Monitor schema deletions
RUST_LOG=neb::ram::schema=warn ./your_binary | grep -i "deleted\|deletion"
```

### Debugging Schema Initialization Issues
```bash
# Full debug output for schema initialization
RUST_LOG=neb::ram::schema=debug,neb::server=debug ./your_binary
```

## Programmatic Setup (in code)

If you want to configure this in your code instead of environment variable:

```rust
use env_logger;

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default()
        .default_filter_or("neb::ram::schema=info"))
        .init();
    
    // Your code here
}
```

Or more flexible:

```rust
use std::env;
use env_logger;

fn main() {
    // Allow RUST_LOG override, but default to schema logs
    let log_level = env::var("RUST_LOG")
        .unwrap_or_else(|_| "error,neb::ram::schema=info".to_string());
    
    env_logger::Builder::from_env(env_logger::Env::default()
        .default_filter_or(&log_level))
        .init();
    
    // Your code here
}
```

## Testing

To test your logging configuration:

```bash
# Test with a simple schema creation
RUST_LOG=neb::ram::schema=info cargo test -- --nocapture your_schema_test
```

## Tips

1. **Start with INFO level** - This shows all important schema events
2. **Use DEBUG for troubleshooting** - Shows subscription events and initialization details
3. **Use TRACE sparingly** - Very verbose, shows every schema during bulk import
4. **Combine with grep** - Filter output: `RUST_LOG=neb::ram::schema=info ./binary | grep "schema"`
5. **Redirect to file** - Save logs: `RUST_LOG=neb::ram::schema=info ./binary > schema.log 2>&1`

## Example Output

With `RUST_LOG=neb::ram::schema=info`:

```
INFO neb::ram::schema: Initializing local schema cache
INFO neb::ram::schema: Importing 42 schemas from cluster
INFO neb::ram::schema: Local schema initialization completed
INFO neb::ram::schema::sm: Schema created in SchemasSM: 12345 (user_profile)
INFO neb::ram::schema: Received schema_added event: schema 12345 (user_profile)
INFO neb::ram::schema: Added schema to local cache: 12345 (user_profile)
WARN neb::ram::schema: Received schema_deleted event for schema: old_schema
WARN neb::ram::schema: Deleted local schema 'old_schema' with id 67890
```

