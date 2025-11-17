# Distributed Counters: Serializability vs Linearizability

## TL;DR

**Distributed counters with serializability (relaxed protocol)**:
- ✅ **Commutative increments/decrements** → SAFE (use CRDT/escrow)
- ❌ **Read-then-increment** → UNSAFE (can appear non-monotonic)
- ❌ **External visibility** → UNSAFE (observers see backwards movement)
- ✅ **Increment-only, no external reads** → SAFE

**Solution**: Use **commutative operations** (CRDTs) or **escrow techniques** to make counters safe under serializability.

---

## Problem: Counters Under Relaxed Timestamp Ordering

### Scenario 1: Simple Increment (UNSAFE with Read-Modify-Write)

```
Distributed counter starting at 100:

Node A:
  T1 (tid=100, 9:00 AM):
    read counter = 100
    write counter = 101
    commits
    
Node B:
  T2 (tid=50, 9:01 AM):
    read counter = 100  (sees old value due to caching/delay)
    write counter = 101
    commits
    
Timeline:
──────────────────────────────────────────────────────────
9:00 AM: T1 commits → counter = 101
9:01 AM: T2 commits with older tid=50

With Relaxed Protocol:
  - T2 prepares: no write-write timestamp check ✓
  - T2 commits: Thomas Write Rule: 50 < 101 → SKIP T2's write
  - Final value: 101 ✓
  
Result: T2's increment is LOST!
```

**Problem**: Read-modify-write pattern doesn't commute. T2's increment silently disappears.

---

### Scenario 2: Counter with External Visibility (UNSAFE)

```
Public view counter on a website:

9:00:00: T1 (tid=100) increments → counter = 1000
         T1 commits
         Dashboard displays: "1000 views"
         User takes screenshot

9:00:01: T2 (tid=50) increments → counter = 999
         T2 has older timestamp

With Relaxed Protocol:
  - T1 commits first: counter = 1000
  - T2 prepares: no write-write check, gets lock
  - T2 commits: writes counter = 999 (older value)
  
External observers see:
  - 9:00:00: counter = 1000
  - 9:00:01: counter = 999 ← appears to go backwards!
  
Analytics query: "How many views at 9:00:01?"
  Answer: 999 (but screenshot shows 1000)
```

**Problem**: External observers expect **monotonic increases**. Relaxed protocol can make counter appear to decrease!

---

### Scenario 3: Quota/Limit Enforcement (VERY UNSAFE)

```
API rate limiting: Max 1000 requests/minute

9:00:00: T1 (tid=100): increment counter → 999
         Check: 999 < 1000 → allow request ✓
         T1 commits
         
9:00:01: T2 (tid=50): increment counter → 998
         Check: 998 < 1000 → allow request ✓
         T2 commits (older timestamp)
         
With Relaxed Protocol:
  - T1 writes 999
  - T2 writes 998 (older tid, but later by wall-clock)
  - Thomas Write Rule might skip T2's write
  
Result: 
  - Database shows: 999 requests
  - Actually allowed: 1000 requests (both T1 and T2)
  - Quota exceeded by 1!
  
If many transactions like T2:
  - Database shows: 999
  - Actually allowed: 999 + N (massive over-subscription!)
```

**Problem**: Quota enforcement assumes monotonicity. Relaxed protocol breaks this assumption.

---

## When Distributed Counters ARE Safe

### ✅ Option 1: Commutative Increment Operations (CRDT-style)

Instead of read-modify-write, use **increment-only operations**:

```rust
// BAD: Read-modify-write
let current = read(counter_id);
write(counter_id, current + 1);

// GOOD: Commutative operation
increment(counter_id, +1);  // Doesn't read, just adds
```

**Implementation**:
```rust
pub enum CounterOp {
    Increment(i64),
    Decrement(i64),
}

// Store operations, not values
pub struct Counter {
    operations: Vec<(TxnId, CounterOp)>,
}

// Compute value on read
fn get_value(counter: &Counter) -> i64 {
    counter.operations.iter()
        .map(|(_, op)| match op {
            CounterOp::Increment(n) => *n,
            CounterOp::Decrement(n) => -*n,
        })
        .sum()
}
```

**Why It's Safe**:
- Increments commute: `+1 then +2` = `+2 then +1` = `+3`
- No read-modify-write race
- Order doesn't matter!

**Example**:
```
T1 (tid=100): increment(counter, +1)
T2 (tid=50):  increment(counter, +1)

Regardless of commit order:
  - Both operations recorded
  - Final value = initial + 1 + 1 ✓
  - No lost increments!
```

---

### ✅ Option 2: Escrow Technique

Allocate "budget" to transactions, merge on commit:

```rust
pub struct EscrowCounter {
    committed_value: i64,
    escrow: HashMap<TxnId, i64>,  // Pre-allocated increments
}

// Transaction requests increment
fn request_increment(counter: &mut EscrowCounter, tid: TxnId, amount: i64) {
    counter.escrow.insert(tid, amount);
}

// On commit: merge escrow into committed value
fn commit_increment(counter: &mut EscrowCounter, tid: TxnId) {
    if let Some(amount) = counter.escrow.remove(&tid) {
        counter.committed_value += amount;
    }
}

// Quota check uses: committed + sum(all_escrow)
fn check_quota(counter: &EscrowCounter, limit: i64) -> bool {
    let total = counter.committed_value + 
                counter.escrow.values().sum::<i64>();
    total < limit
}
```

**Why It's Safe**:
- Pre-allocate increments before commit
- Quota checks include in-flight increments
- No double-counting or lost increments

**Example**:
```
Counter = 998, Limit = 1000

T1 (tid=100): request_increment(+1)
  - escrow[100] = 1
  - check: 998 + 1 = 999 < 1000 ✓ allow

T2 (tid=50): request_increment(+1)
  - escrow[50] = 1
  - check: 998 + 1 (T1's escrow) + 1 = 1000 ✗ deny

T1 commits:
  - committed = 999
  - escrow[100] removed

T2 retries:
  - check: 999 + 1 = 1000 ✗ still deny (limit reached)
```

---

### ✅ Option 3: Last-Write-Wins (If Monotonicity Not Required)

For counters where **exact value doesn't matter**, only approximate:

```rust
// Analytics counter: "approximately 1M views"
// Small errors acceptable

T1 (tid=100): write counter = 1000
T2 (tid=50):  write counter = 999

With Relaxed Protocol:
  - Final value: 1000 or 999 (either is fine)
  - Approximate count: ~1000 ✓
```

**Use Cases**:
- Page view counters (approximate)
- Cache statistics
- Performance metrics
- Non-critical analytics

**Why It's Safe**:
- Small errors acceptable
- No external consistency required
- Users understand it's approximate

---

## When Distributed Counters Are UNSAFE

### ❌ Case 1: Quota Enforcement

```rust
// API rate limiting
if counter < LIMIT {
    allow_request();
    counter += 1;
}
```

**Problem**: Relaxed protocol can allow over-subscription.

**Solution**: 
- Use **escrow technique** 
- Or enable **strict linearizability** for quota counters

---

### ❌ Case 2: Sequential ID Generation

```rust
// Generate unique IDs
let next_id = counter;
counter += 1;
return next_id;
```

**Problem**: Two transactions might return same ID if timestamps crossed.

**Solution**:
- Use **atomic increment** (single-node)
- Or use **UUID/snowflake** (no counter needed)
- Or enable **strict linearizability**

---

### ❌ Case 3: Financial Transactions

```rust
// Account balance
balance = balance + transaction_amount;
```

**Problem**: Lost updates = lost money!

**Solution**:
- Use **escrow technique** (pre-allocate before commit)
- Or enable **strict linearizability** for financial counters
- Or use **CRDT** with operation log

---

### ❌ Case 4: External Dashboards/Monitoring

```rust
// Real-time dashboard: "Current active users"
active_users_counter += 1;  // on login
active_users_counter -= 1;  // on logout
```

**Problem**: Dashboard might show counter going backwards (external visibility).

**Solution**:
- Use **eventually consistent counters** with explicit refresh
- Or buffer updates and batch-apply in timestamp order
- Or enable **strict linearizability** for monitored counters

---

## Implementation Recommendations for Nebuchadnezzar

### Strategy 1: Add Counter-Specific Operations

```rust
// In data_site.rs or new counter module
pub enum CellOp {
    Read,
    Write(OwnedCell),
    Update(OwnedCell),
    Remove,
    Increment(Id, i64),  // NEW: Commutative increment
    Decrement(Id, i64),  // NEW: Commutative decrement
}

// In commit phase
CommitOp::Increment(cell_id, delta) => {
    // Atomic increment - commutes regardless of timestamp
    self.server.chunks.atomic_increment(&cell_id, delta)?;
}
```

**Benefits**:
- Commutative operations safe under relaxed protocol
- No lost increments
- No read-modify-write races

---

### Strategy 2: Add Escrow Support

```rust
pub struct CellMeta {
    read: TxnId,
    write: TxnId,
    owner: Option<TxnId>,
    escrow: HashMap<TxnId, i64>,  // NEW: Escrowed increments
}

// During prepare for counter operations
fn prepare_counter_increment(&self, cell_id: &Id, tid: &TxnId, delta: i64) {
    let meta = self.cell_meta_mutex(cell_id);
    let mut meta_guard = meta.lock();
    
    // Check quota with escrow
    let current = self.server.chunks.read_counter(cell_id)?;
    let escrowed = meta_guard.escrow.values().sum::<i64>();
    if current + escrowed + delta > LIMIT {
        return DMPrepareResult::NotRealizable;  // Quota exceeded
    }
    
    // Reserve increment
    meta_guard.escrow.insert(tid.clone(), delta);
    DMPrepareResult::Success
}
```

**Benefits**:
- Quota enforcement works correctly
- No over-subscription
- Safe under relaxed protocol

---

### Strategy 3: Counter Type Flag

```rust
pub struct Schema {
    // ... existing fields
    counter_fields: HashSet<u64>,  // Field IDs that are counters
}

// In prepare phase
if schema.counter_fields.contains(&field_id) {
    // Use commutative increment instead of write
    prepare_counter_increment(cell_id, tid, delta)?;
} else {
    // Normal write path
    prepare_write(cell_id, tid)?;
}
```

**Benefits**:
- Application declares counter fields
- System handles them correctly automatically
- No application changes needed once declared

---

### Strategy 4: Configuration Per Counter

```rust
pub enum CounterMode {
    Approximate,     // Fast, LWW, eventual consistency
    Commutative,     // Safe, CRDT-style operations
    Strict,          // Slow, enable linearizability for this counter
}

pub struct CounterConfig {
    mode: CounterMode,
    quota: Option<i64>,  // Enable quota checking
}

// Application configures counters
schema.configure_counter("view_count", CounterConfig {
    mode: CounterMode::Approximate,  // Page views - approximate OK
    quota: None,
});

schema.configure_counter("api_quota", CounterConfig {
    mode: CounterMode::Commutative,  // Rate limiting - needs exactness
    quota: Some(1000),
});
```

---

## Summary Table: Counter Safety

| Counter Type | Relaxed Protocol | Strict Protocol | CRDT/Escrow |
|-------------|------------------|-----------------|-------------|
| **Page views (approx)** | ✅ Safe | ✅ Safe | ✅ Safe |
| **Analytics (approx)** | ✅ Safe | ✅ Safe | ✅ Safe |
| **API quotas** | ❌ Unsafe | ✅ Safe | ✅ Safe |
| **Financial balances** | ❌ Unsafe | ✅ Safe | ✅ Safe |
| **Sequential IDs** | ❌ Unsafe | ✅ Safe | ❌ Use UUID |
| **External dashboards** | ❌ Unsafe | ✅ Safe | ⚠️ Eventual |
| **Like counts** | ⚠️ Approx | ✅ Safe | ✅ Safe |
| **Follower counts** | ⚠️ Approx | ✅ Safe | ✅ Safe |

**Legend**:
- ✅ Safe: Works correctly
- ❌ Unsafe: Can lose increments or violate constraints
- ⚠️ Approx: Approximately correct (small errors possible)

---

## Recommendations for Nebuchadnezzar

### For Most Graph Workloads (No Counters)
✅ **Use relaxed protocol** (current implementation)
- 50-80% fewer aborts
- Best performance

### For Graphs With Counters

**Option A**: Implement **commutative operations**
```rust
txn.increment(vertex_id, "like_count", +1);  // Commutative
```
- ✅ Safe under relaxed protocol
- ✅ High performance
- ✅ No lost increments

**Option B**: Use **escrow for quotas**
```rust
txn.increment_with_quota(user_id, "api_calls", +1, max=1000);
```
- ✅ Safe quota enforcement
- ✅ Works with relaxed protocol

**Option C**: Enable **strict mode for critical counters**
```rust
// Per-transaction or per-field configuration
txn.set_mode(StrictLinearizability);
txn.increment(account_id, "balance", amount);
```
- ✅ Guaranteed correctness
- ❌ Lower performance (more aborts)

---

## Conclusion

**Distributed counters under serializability are**:
- ❌ **UNSAFE** with naive read-modify-write
- ❌ **UNSAFE** for quota enforcement
- ❌ **UNSAFE** with external visibility
- ✅ **SAFE** with commutative operations (CRDT)
- ✅ **SAFE** with escrow techniques
- ⚠️ **APPROXIMATELY SAFE** if exactness not required

**Best approach**: Implement **commutative increment/decrement operations** in Nebuchadnezzar. This makes counters safe under the relaxed protocol while maintaining high performance.

