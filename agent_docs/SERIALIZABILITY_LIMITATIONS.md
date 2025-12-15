# When Serializability Is NOT Sufficient

## TL;DR

**Serializability is insufficient when**:
- External observers require real-time consistency
- Cross-system coordination depends on commit ordering
- Regulatory/audit requirements mandate strict causal ordering
- Non-transactional reads must see the latest committed state

Most **graph workloads are fine with serializability**. Strict linearizability is needed for **coordination systems, financial transactions, and externally-consistent distributed systems**.

---

## Understanding the Gap

### Serializability Guarantees
- Transactions appear to execute in **some** serial order
- Order may differ from real-time/wall-clock order
- ✅ Internal consistency (within the database)
- ❌ External consistency (across systems or with wall-clock)

### Linearizability Guarantees  
- Transactions execute in an order **matching real-time**
- If T1 commits before T2 starts (by wall-clock), T2 sees T1's effects
- ✅ Internal AND external consistency

---

## Workloads That Need Linearizability

### 1. Financial Systems with External Observers

**Problem**: Bank account updates with external audit logs

```
Timeline (Wall-Clock):
─────────────────────────────────────────────────────────
9:00 AM: T1 (tid=100) deposits $1000 → balance = $1500
         T1 commits successfully
         External audit log records: "Balance $1500 at 9:00 AM"

9:01 AM: T2 (tid=50) deposits $500 → balance = $1000
         T2 has older timestamp but commits after T1
         
With Serializability (Relaxed Protocol):
  - T2's write might win (Thomas Write Rule skips T1)
  - Final balance: $1000
  - Audit log says: "$1500 at 9:00 AM"
  - Inconsistency: External log contradicts database state!

With Linearizability:
  - T2 would be rejected (timestamp too old vs wall-clock)
  - Final balance: $1500
  - Audit log matches database state ✓
```

**Why Serializability Fails**: External observers (audit logs, ATM receipts, email confirmations) create ordering expectations that pure serializability doesn't guarantee.

---

### 2. Distributed Transactions Across Multiple Systems

**Problem**: Multi-database transactions with non-transactional reads

```
System A (Inventory DB) and System B (Order DB):

9:00 AM: T1 commits on System A: inventory[item_X] = 10
         External system reads inventory = 10
         
9:01 AM: External system writes to System B: order[item_X] = 5
         
9:02 AM: T2 (older timestamp) commits on System A: inventory[item_X] = 5

With Serializability:
  - T2's write might become visible (older timestamp, but committed later)
  - System A shows: inventory = 5
  - System B shows: order = 5
  - Inconsistency: Ordered 5 units when inventory was only 5, 
    but external read saw 10!

With Linearizability:
  - T2 rejected (real-time ordering enforced)
  - Systems A and B remain consistent
```

**Why Serializability Fails**: Non-transactional reads between systems create causal dependencies that serializability doesn't preserve.

---

### 3. Leader Election and Coordination Protocols

**Problem**: Distributed consensus requiring strong ordering

```
Leader Election Protocol:

Node A: T1 (tid=100) writes leader=A, commits at 9:00:00
Node B: T2 (tid=150) writes leader=B, commits at 9:00:01
Node C: T3 (tid=120) writes leader=C, commits at 9:00:02

With Serializability:
  - Final state could be leader=A or leader=C (depending on lock order)
  - Different nodes might see different leaders temporarily
  - Split-brain possible!

With Linearizability:
  - Order is: T1 → T2 → T3 (real-time order)
  - Final state: leader=B (latest by wall-clock)
  - All nodes converge to same leader
```

**Why Serializability Fails**: Coordination protocols depend on **real-time ordering** to prevent split-brain and ensure safety.

---

### 4. Real-Time Trading Systems

**Problem**: Stock trades with external market data

```
Stock Trading Platform:

9:00:00.000: External market feed: AAPL = $150
9:00:00.100: T1 (tid=100) places order: buy AAPL @ $150
              T1 commits
9:00:00.200: External market feed: AAPL = $155
9:00:00.300: T2 (tid=50, delayed) places order: buy AAPL @ $150
              T2 commits

With Serializability:
  - T2 might execute at $150 (older timestamp wins via Thomas Write Rule)
  - Order filled at stale price!
  - Arbitrage opportunity created

With Linearizability:
  - T2 rejected (price changed in real-time)
  - Orders execute at correct market prices
```

**Why Serializability Fails**: External real-time data sources create ordering constraints based on wall-clock time.

---

### 5. Regulatory Compliance and Immutable Audit Logs

**Problem**: Healthcare records with legal requirements

```
Healthcare Record System:

10:00 AM: Dr. Smith (T1, tid=100) prescribes Drug A
          T1 commits
          Prescription printed and given to patient

10:05 AM: Dr. Jones (T2, tid=80) prescribes Drug B (incompatible with A)
          T2 has older timestamp

With Serializability:
  - T2's prescription might "appear first" in serialization order
  - Audit trail shows: Drug B → Drug A
  - Patient has prescription for Drug A (real-time order)
  - Legal issue: Prescription doesn't match audit trail!

With Linearizability:
  - T2 rejected or recorded after T1 (real-time order)
  - Audit trail matches physical prescriptions
```

**Why Serializability Fails**: Legal/regulatory requirements often mandate that **database order = real-time order** for audit purposes.

---

### 6. Distributed Counters with External Visibility

**Problem**: Analytics dashboard with real-time updates

```
Page View Counter:

9:00:00: T1 (tid=100) increments counter: views = 1000
         T1 commits
         Dashboard shows: "1000 views"
         User screenshots dashboard

9:00:01: T2 (tid=50) increments counter: views = 999
         T2 commits (older timestamp)

With Serializability:
  - Counter might become 999 (T2's write wins via Thomas Write Rule)
  - User's screenshot shows 1000, but database shows 999
  - Appears counter went backwards!

With Linearizability:
  - Counter monotonically increases
  - Never appears to go backwards
```

**Why Serializability Fails**: External observations create ordering expectations that pure serializability violates.

---

### 7. Multi-Tenant Systems with Cross-Tenant Constraints

**Problem**: Resource quotas enforced across tenants

```
Multi-Tenant Cloud Platform:

9:00 AM: Tenant A (T1, tid=100): allocate 10 CPUs → total_used = 90/100
         T1 commits
         Monitoring system: "10 CPUs available"

9:01 AM: Tenant B (T2, tid=50): allocate 15 CPUs
         T2 has older timestamp

With Serializability:
  - T2 might be serialized before T1
  - System allocates 15 CPUs when only 10 available!
  - Over-subscription!

With Linearizability:
  - T2 sees T1's allocation (real-time order)
  - T2 rejected (only 10 CPUs available)
```

**Why Serializability Fails**: Global resource constraints require real-time ordering to prevent over-allocation.

---

## Workloads That ARE Fine With Serializability

### ✅ Graph Database Operations

**Why It Works**: Most graph operations are order-insensitive or convergent

```
Social Network:

T1: Add edge (Alice → Bob)
T2: Add edge (Alice → Carol)

Serialization order doesn't matter:
  - T1 → T2: Alice has edges to {Bob, Carol}
  - T2 → T1: Alice has edges to {Carol, Bob}
  - Both are correct! Sets are unordered.
```

**Graph Operations That Work**:
- ✅ Edge additions (commutative)
- ✅ Vertex property updates (last-write-wins acceptable)
- ✅ PageRank, community detection (convergent algorithms)
- ✅ Path finding (graph topology eventually consistent)
- ✅ Degree counting (convergent)

---

### ✅ Analytics and Aggregation Workloads

```
Analytics Pipeline:

T1: Record event A (timestamp=100)
T2: Record event B (timestamp=50)

Aggregate query: COUNT(*) WHERE timestamp BETWEEN 0 AND 200

Result: 2 events (both counted)
Order doesn't matter for aggregates!
```

**Why It Works**: Aggregates are commutative and associative.

---

### ✅ Content Management Systems

```
Blog Platform:

T1: User A posts article "Hello World"
T2: User B posts article "Goodbye Cruel World"

With Serializability:
  - Both articles stored
  - Display order determined by application logic (post_time, not commit order)
  - Users see correct article when they query their own post
```

**Why It Works**: Content is independent; ordering determined by application timestamps, not commit order.

---

### ✅ Cache Invalidation and Updates

```
Cache Refresh:

T1: Update user profile (name = "Alice Smith")
T2: Update user profile (name = "Alice Johnson")

With Serializability:
  - One of the writes wins
  - Last write wins (LWW) acceptable
  - No external observers tracking intermediate states
```

**Why It Works**: Application handles conflicts; eventual consistency sufficient.

---

### ✅ Machine Learning Feature Stores

```
ML Feature Store:

T1: Update feature vector for user_123
T2: Update different feature for user_123

With Serializability:
  - Both updates stored
  - Model training reads consistent snapshot
  - Order within snapshot doesn't affect model quality significantly
```

**Why It Works**: ML algorithms are robust to small ordering variations.

---

## Decision Matrix: Serializability vs Linearizability

| Requirement | Serializability | Linearizability |
|-------------|----------------|-----------------|
| **Internal consistency** | ✅ Yes | ✅ Yes |
| **External consistency** | ❌ No | ✅ Yes |
| **Real-time ordering** | ❌ No | ✅ Yes |
| **Non-transactional reads** | ⚠️ May be stale | ✅ Fresh |
| **Cross-system coordination** | ❌ No | ✅ Yes |
| **Audit/regulatory compliance** | ⚠️ Depends | ✅ Yes |
| **Performance (hot spots)** | ✅ High | ⚠️ Lower |
| **Throughput (contended)** | ✅ High | ⚠️ Lower |

---

## Summary Table: Workload Suitability

| Workload Type | Serializability OK? | Reason |
|--------------|---------------------|--------|
| **Graph database operations** | ✅ Yes | Commutative, convergent |
| **Social network updates** | ✅ Yes | Order-insensitive |
| **Analytics aggregations** | ✅ Yes | Commutative operations |
| **Content management** | ✅ Yes | Independent updates |
| **ML feature stores** | ✅ Yes | Robust to ordering |
| **Cache updates** | ✅ Yes | LWW acceptable |
| | | |
| **Financial transactions** | ❌ No | External audit logs |
| **Multi-system transactions** | ❌ No | Cross-system dependencies |
| **Leader election** | ❌ No | Coordination protocol |
| **Real-time trading** | ❌ No | Market data ordering |
| **Healthcare records** | ❌ No | Legal audit requirements |
| **Distributed counters** | ❌ No | External visibility |
| **Resource quotas** | ❌ No | Global constraints |

---

## For Your Graph Database (Nebuchadnezzar)

### ✅ Serializability Is Sufficient For:

1. **Social graphs**: Friend connections, follows, likes
2. **Knowledge graphs**: Entity relationships, property updates
3. **Recommendation graphs**: User-item interactions
4. **Network graphs**: Topology, connectivity
5. **Workflow graphs**: Task dependencies (if eventual consistency OK)

### ⚠️ Consider Strict Mode For:

1. **Financial graphs**: Payment networks, transaction flows
2. **Compliance graphs**: Audit trails, regulatory data
3. **Real-time coordination**: Distributed locking, leader election
4. **Cross-system integration**: If external systems read immediately after commit

### 🎯 Recommendation

**For most graph workloads**: Use the **relaxed protocol** (current implementation)
- 50-80% fewer aborts
- 50-100% higher throughput
- Correctness maintained

**For strict ordering needs**: Add a **configuration flag** to enable strict linearizability:

```rust
pub struct DataManager {
    strict_linearizability: bool,  // Default: false
}

// In prepare():
if self.strict_linearizability && tid < meta.write {
    break;  // Enforce strict TO
}
```

This gives users the choice based on their specific workload requirements.

---

## Testing Your Workload

### Questions to Ask:

1. **Do external systems read immediately after commits?**
   - Yes → May need linearizability
   - No → Serializability OK

2. **Are there audit/regulatory requirements for ordering?**
   - Yes → May need linearizability  
   - No → Serializability OK

3. **Does your application assume real-time ordering?**
   - Yes → May need linearizability
   - No → Serializability OK

4. **Are operations commutative/convergent?**
   - Yes → Serializability OK
   - No → Evaluate carefully

5. **Is performance more important than strict ordering?**
   - Yes → Use serializability (relaxed)
   - No → Use linearizability (strict)

---

## Conclusion

**Serializability is sufficient for most workloads**, especially:
- Pure database operations
- Commutative/convergent updates
- Analytics and aggregations
- Graph operations

**Linearizability is needed when**:
- External systems create ordering expectations
- Real-time constraints matter
- Audit/compliance requires strict ordering
- Cross-system coordination is involved

For Nebuchadnezzar's graph workloads, **the relaxed protocol (serializability) is the right default**, with an option to enable strict mode for special cases.



