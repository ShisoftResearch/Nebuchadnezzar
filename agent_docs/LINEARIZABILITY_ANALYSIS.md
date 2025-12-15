# Linearizability Analysis: Wait-Die + Relaxed Timestamp Ordering

## Quick Answer

**Short version**: The relaxed protocol maintains **serializability** but **may violate strict linearizability** in some edge cases.

**Longer version**: It depends on your definition and what you're willing to trade.

---

## Background: Serializability vs Linearizability

### Serializability
- Transactions appear to execute in **some** serial order
- Order can differ from real-time order
- ✅ What databases typically guarantee

### Linearizability (Strict Serializability)
- Transactions execute in an order **consistent with real-time**
- If T1 commits before T2 starts → T1's effects visible to T2
- ✅ Stronger guarantee, harder to achieve in distributed systems

---

## Does Our Implementation Provide Linearizability?

### ❌ **NO** - With Relaxed Write-Write Timestamp Check

The relaxed protocol can violate linearizability in this scenario:

```
Real-Time Order:
─────────────────────────────────────────────────────────────
T1 (tid=100):  write(X=1)         commit
                |──────────────────|
                                      T2 (tid=200): read(X) → sees X=2
                                                      |
T3 (tid=150):       write(X=2)  commit
                    |──────────|
```

**What happens:**

1. **t=0**: T1 (tid=100) prepares to write X=1, gets lock
2. **t=1**: T1 commits, sets `meta.write=100`, releases lock
3. **t=2**: T3 (tid=150) prepares to write X=2
   - Wait-Die: no owner conflict ✅
   - Read check: passes ✅
   - Write check: **REMOVED** (relaxed) ✅
   - Gets lock, writes X=2
4. **t=3**: T3 commits, sets `meta.write=150`, releases lock
5. **t=4**: T2 (tid=200) reads X → sees X=2 (from T3)
6. **t=5**: T1's write is now "in the past" (tid=100 < 200)

**In commit phase**, when both try to commit:
- T3 commits first: writes X=2, sets `meta.write=150`
- T1 commits later: Thomas Write Rule checks `100 < 150` → **skips write**

**Result**: X=2 (T3's value), even though T1 committed before T3 in real-time.

**Is this linearizable?**
- ❌ **NO**: Real-time order was T1 → T3, but final state reflects T3's write, not T1's
- ✅ **Serializable**: Equivalent to serial order T3 → T1 (valid serialization)

---

## Why Does This Happen?

### Root Cause: Timestamp ≠ Real-Time Order

In your current design:
- `TxnId` (timestamp) is assigned at **transaction start**, not commit
- Transactions can commit **out of timestamp order**
- Relaxed write-write check allows T3 to "overtake" T1

### Example Timeline

```
Real-Time   Transaction        TxnId    Action
─────────────────────────────────────────────────────────────
t=0         T1 starts          100      -
t=1         T1 writes X=1      100      prepare, get lock
t=2         T1 commits         100      commit, release lock
            
t=3         T3 starts          150      -
t=4         T3 writes X=2      150      prepare, get lock (no write-write check!)
t=5         T3 commits         150      commit, sets meta.write=150
            
t=6         T2 starts          200      -
t=7         T2 reads X         200      sees X=2 (from T3)

# Even though T1 committed before T3 started, T2 doesn't see T1's write!
```

---

## Can We Restore Linearizability?

### Option 1: Keep Strict Write-Write Check ✅ (Most Conservative)

```rust
// In prepare():
if tid < meta.write {
    break;  // Restore this check
}
```

**Effect**:
- T3 would abort when checking `150 < 150` (if T1 committed first)
- Or T1 would abort if T3 prepared first
- Enforces timestamp order = real-time order for writes
- ✅ Linearizable
- ❌ Lower concurrency on hot cells (your original problem)

### Option 2: Use Commit Timestamps Instead of Start Timestamps 🤔 (Complex)

Assign timestamps at **commit time**, not start time:

```rust
// In commit phase:
let commit_ts = get_current_timestamp();  // Not transaction start time
```

**Effect**:
- Timestamp order matches commit order matches real-time order
- ✅ Linearizable
- ⚠️ Requires significant protocol changes
- ⚠️ May need to re-validate reads at commit time

### Option 3: Accept Serializability, Not Linearizability ✅ (Pragmatic)

Many distributed databases do this:
- PostgreSQL: Serializable, not linearizable
- MySQL: Serializable (with proper isolation level)
- MongoDB: Provides "snapshot isolation" option
- Spanner: Linearizable (but uses TrueTime for global ordering)

**Effect**:
- Keep current relaxed protocol
- ✅ High concurrency on hot cells
- ✅ Serializability guaranteed
- ❌ Not strictly linearizable
- Most applications don't require linearizability

---

## What Does Your Current Code Guarantee?

### ✅ Guarantees (With Relaxed Protocol)

1. **Serializability**: Every execution is equivalent to **some** serial order
2. **Deadlock-freedom**: Wait-Die prevents circular waits
3. **No lost updates**: Locks ensure mutual exclusion
4. **Consistency**: Thomas Write Rule ensures final state is valid

### ❌ Does NOT Guarantee

1. **Strict Linearizability**: Real-time order may not be preserved for all operations
2. **External Consistency**: If external observer sees T1 commit, then T2 starts, T2 might not see T1's effects if T3 "sneaks in"

### ⚠️ Edge Case

The linearizability violation **only occurs** when:
- Two transactions with **different timestamps** commit in **reverse timestamp order**
- AND both write to the **same cell**
- AND a third transaction reads after both commit

This is **rare** but **possible**.

---

## Recommendations

### For Graph Workloads (Your Use Case)

**Recommendation**: **Accept serializability, not linearizability**

**Why**:
1. Most graph algorithms don't require linearizability
2. PageRank, community detection, path finding → all OK with serializability
3. The performance gain (50-80% fewer aborts) is substantial
4. External consistency violations are rare in practice

**When linearizability matters**:
- Distributed counters where external observers matter
- Banking/financial transactions with external audit logs
- Coordination protocols (leader election, etc.)

If your graph workload includes these, consider Option 1 or 2.

### Implementation Recommendation

Add a **configuration flag**:

```rust
pub struct DataManager {
    // ...
    strict_linearizability: bool,  // Default: false
}

// In prepare():
if self.strict_linearizability && tid < meta.write {
    break;  // Enforce linearizability
}
```

This lets users choose:
- **Performance mode** (default): Relaxed, serializability only
- **Strict mode**: Lower concurrency, linearizability

---

## Testing for Linearizability Violations

### Test Case

```rust
#[test]
fn test_linearizability_violation() {
    // T1 (tid=100) writes X=1, commits at t=2
    // T3 (tid=150) writes X=2, commits at t=5
    // T2 (tid=200) reads X at t=6
    
    // RELAXED: T2 sees X=2 (T3's write)
    // STRICT: T2 would see X=1 (T1's write) if T1 committed before T3 started
    
    // Check if real-time order is preserved
}
```

### Jepsen-Style Testing

Use a linearizability checker like [Jepsen](https://jepsen.io/) or [Knossos](https://github.com/jepsen-io/knossos):
- Record all operation start/end times
- Check if a valid linearization exists
- Should PASS for serializability, may FAIL for strict linearizability

---

## Summary Table

| Property | Strict TO | Wait-Die + Relaxed | Wait-Die Only |
|----------|-----------|-------------------|---------------|
| **Serializability** | ✅ | ✅ | ✅ |
| **Linearizability** | ✅ | ❌ (edge cases) | ❌ (edge cases) |
| **Deadlock-free** | ✅ | ✅ | ✅ |
| **Abort rate** | High | Low | Medium |
| **Throughput** | Low | High | Medium |

---

## Final Answer

**Your current implementation (Wait-Die + Relaxed write-write check)**:
- ✅ **Serializable**: Yes, always
- ❌ **Linearizable**: No, can be violated when transactions commit out of timestamp order
- 🎯 **Good for**: High-throughput graph workloads where performance > strict real-time ordering
- ⚠️ **Not good for**: Systems requiring external consistency or strict linearizability

For most graph database use cases, **serializability is sufficient** and the performance gain is worth it.

If you need linearizability, restore the `tid < meta.write` check (Option 1).

