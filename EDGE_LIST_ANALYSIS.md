# Edge List Operations: Safety Analysis

## TL;DR

**Edge additions CAN lose edges** under relaxed protocol if implemented as **read-modify-write**:
- ❌ Read edge list → append edge → write back = UNSAFE
- ✅ Append-only log of edges = SAFE
- ✅ Separate edge cells (one per edge) = SAFE
- ✅ Commutative append operations = SAFE

**Recommendation**: Use **append-only operations** or **separate edge storage** to avoid lost edges.

---

## The Problem: Read-Modify-Write on Edge Lists

### Scenario: Adjacency List Stored as Array

```rust
// Vertex cell contains:
struct Vertex {
    id: u64,
    properties: Map,
    edges: Vec<EdgeId>,  // ← Edge list stored in vertex
}

// Two transactions add edges:
T1 (tid=100):
    let vertex = read(vertex_id);           // edges = [e1, e2]
    vertex.edges.push(e3);                  // edges = [e1, e2, e3]
    write(vertex_id, vertex);
    commit();

T2 (tid=50, older timestamp):
    let vertex = read(vertex_id);           // edges = [e1, e2]
    vertex.edges.push(e4);                  // edges = [e1, e2, e4]
    write(vertex_id, vertex);
    commit();
```

### What Happens with Relaxed Protocol

```
Timeline:
─────────────────────────────────────────────────────────────────
Initial: vertex.edges = [e1, e2]

9:00:00: T1 reads vertex → edges = [e1, e2]
9:00:01: T1 writes vertex → edges = [e1, e2, e3]
9:00:02: T1 commits ✓

9:00:03: T2 reads vertex → edges = [e1, e2] (stale read or snapshot)
9:00:04: T2 writes vertex → edges = [e1, e2, e4]
9:00:05: T2 prepares → no write-write check ✓
9:00:06: T2 commits → Thomas Write Rule: tid(50) < write_ts(100)
         → SKIP T2's write

Final: vertex.edges = [e1, e2, e3]
Lost: e4 ❌
```

**Result**: **Edge e4 is LOST!**

---

## Why This Happens (Same as Counter Problem)

The issue is **read-modify-write on the entire edge list**:

1. **Both transactions read the old list** `[e1, e2]`
2. **Each creates a new version** with their edge added
3. **T1's write wins** (higher timestamp)
4. **T2's write is skipped** (Thomas Write Rule)
5. **Edge e4 disappears**

This is **exactly the same problem** as distributed counters!

---

## Solution 1: Append-Only Edge Log ⭐ RECOMMENDED

Instead of storing edges as an array in the vertex, store them as **append-only log**:

```rust
// Store edges as separate log entries
struct EdgeLogEntry {
    vertex_id: u64,
    edge_id: EdgeId,
    timestamp: TxnId,
    operation: EdgeOp,  // Add or Remove
}

// Transactions append to log
T1 (tid=100):
    append_edge_log(vertex_id, e3, Add);
    commit();

T2 (tid=50):
    append_edge_log(vertex_id, e4, Add);
    commit();

// Read edges by scanning log
fn get_edges(vertex_id: u64) -> Vec<EdgeId> {
    let log = read_edge_log(vertex_id);
    let mut edges = HashSet::new();
    for entry in log {
        match entry.operation {
            Add => edges.insert(entry.edge_id),
            Remove => edges.remove(&entry.edge_id),
        };
    }
    edges.into_iter().collect()
}
```

**Why it's safe**:
- Each transaction **appends** to the log, doesn't modify existing entries
- No read-modify-write of the edge list
- Both T1 and T2's entries are preserved
- Final log: `[..., Add(e3), Add(e4)]` ✓

**Trade-off**:
- ✅ No lost edges
- ✅ Works with relaxed protocol
- ⚠️ Reading edges requires scanning log (can be optimized with compaction)

---

## Solution 2: Separate Edge Cells (One Cell Per Edge)

Store each edge as a **separate cell**, not in vertex:

```rust
// Each edge is its own cell
struct Edge {
    id: EdgeId,
    from: VertexId,
    to: VertexId,
    properties: Map,
}

// Transactions create edge cells
T1 (tid=100):
    let edge3 = Edge { id: e3, from: v1, to: v2, ... };
    write(e3, edge3);
    commit();

T2 (tid=50):
    let edge4 = Edge { id: e4, from: v1, to: v3, ... };
    write(e4, edge4);
    commit();

// Query edges by scanning edge cells
fn get_edges(vertex_id: u64) -> Vec<Edge> {
    scan_edges_where(from == vertex_id OR to == vertex_id)
}
```

**Why it's safe**:
- Each transaction writes to **different cells** (e3 vs e4)
- No conflict at all!
- Both edges persist ✓

**Trade-off**:
- ✅ No lost edges
- ✅ Works perfectly with relaxed protocol
- ⚠️ Reading edges requires scanning/indexing (typical for graph DBs)

---

## Solution 3: Commutative Append Operation

Add a **native append operation** that doesn't require reading:

```rust
pub enum CommitOp {
    Read(Id, u64),
    Write(OwnedCell),
    Update(OwnedCell),
    Remove(Id),
    AppendEdge(VertexId, EdgeId),  // NEW: Commutative append
}

// Usage
T1 (tid=100):
    txn.append_edge(vertex_id, e3);
    txn.commit();

T2 (tid=50):
    txn.append_edge(vertex_id, e4);
    txn.commit();

// In commit phase
CommitOp::AppendEdge(vertex_id, edge_id) => {
    // Atomically append edge to vertex's edge list
    self.server.chunks.atomic_append_edge(vertex_id, edge_id)?;
    // No read, just append - commutes regardless of order
}
```

**Implementation**:
```rust
fn atomic_append_edge(&self, vertex_id: Id, edge_id: EdgeId) -> Result<()> {
    self.update_cell_by(&vertex_id, |vertex| {
        vertex.edges.push(edge_id);  // In-place append
        Some(vertex)
    })
}
```

**Why it's safe**:
- No read phase
- Append operations **commute**: `push(e3); push(e4)` = `push(e4); push(e3)` = `[..., e3, e4]`
- Both edges added ✓

**Trade-off**:
- ✅ No lost edges
- ✅ High performance
- ⚠️ Need to implement atomic append operation

---

## Solution 4: Copy-on-Write with Version Check

Use **optimistic concurrency control** with versioning:

```rust
T1 (tid=100):
    let (vertex, version) = read_with_version(vertex_id);
    vertex.edges.push(e3);
    write_if_version(vertex_id, vertex, expected_version=version);
    commit();

T2 (tid=50):
    let (vertex, version) = read_with_version(vertex_id);
    vertex.edges.push(e4);
    write_if_version(vertex_id, vertex, expected_version=version);
    commit();  // This will FAIL because version changed by T1

// T2 retries
T2_retry (tid=200, new timestamp):
    let (vertex, version) = read_with_version(vertex_id);  // Now sees e3
    vertex.edges.push(e4);
    write_if_version(vertex_id, vertex, expected_version=version);
    commit();  // Success!

Final: vertex.edges = [e1, e2, e3, e4] ✓
```

**Why it's safe**:
- Version check detects concurrent modifications
- Transaction retries with fresh data
- No lost edges ✓

**Trade-off**:
- ✅ No lost edges
- ⚠️ Retry overhead on conflicts
- ⚠️ High contention = many retries (back to the original problem!)

---

## Real-World Graph Database Approaches

### Neo4j / ArangoDB / JanusGraph

**Separate edge storage** (Solution 2):
```
Vertex Cell: { id, properties }
Edge Cell 1: { id, from: v1, to: v2, label: "friend" }
Edge Cell 2: { id, from: v1, to: v3, label: "follows" }
```

**Advantages**:
- No conflicts when adding edges to same vertex
- Natural fit for graph queries
- Edges are first-class citizens

---

### DGraph

**Edge list with CRDT semantics**:
```
Use set-based CRDTs for edge lists
Add operations commute
Remove operations commute
Final state = union of all adds - union of all removes
```

---

### TigerGraph

**Compressed edge lists with delta encoding**:
```
Base edge list + append log
Periodically compact log into base
Append operations never conflict
```

---

## Analysis: Your Current Architecture

Based on your codebase, you're using **cells** to store data. Let me check how edges might be stored:

### If Edges Are Stored In Vertex Cells

```rust
// Vertex cell contains edge list
Cell {
    id: vertex_id,
    data: {
        properties: { ... },
        edges: [e1, e2, e3, ...]  // ← Array in cell
    }
}
```

**Problem**: Read-modify-write on entire cell
- ❌ Can lose edges with relaxed protocol
- ❌ Same issue as counters

**Solution**: 
- Use Solution 1 (append-only log)
- Or Solution 2 (separate edge cells)
- Or Solution 3 (commutative append)

---

### If Each Edge Is a Separate Cell

```rust
// Edge cells
Cell { id: e3, data: Edge { from: v1, to: v2, ... } }
Cell { id: e4, data: Edge { from: v1, to: v3, ... } }

// Vertex cell just has properties
Cell { id: v1, data: { properties: { ... } } }
```

**Status**: ✅ **SAFE!**
- No conflicts (different cells)
- Both edges persist
- This is the typical graph DB approach

---

## Recommendation for Nebuchadnezzar

### Architecture Decision

**Option A**: Separate Edge Cells (Most Compatible)
```rust
// Current pattern (likely what you have)
pub struct Edge {
    id: Id,
    from: Id,
    to: Id,
    properties: OwnedValue,
}

// Each edge is a separate cell
let edge_cell = OwnedCell::new(edge_schema, &edge_id, edge_data);
txn.write(edge_id, edge_cell);
```

If you're already doing this → **You're safe!** ✅

---

**Option B**: Add Append-Only Edge Log

```rust
// Add to your schema
pub struct EdgeLog {
    vertex_id: Id,
    entries: Vec<EdgeLogEntry>,  // Append-only
}

pub struct EdgeLogEntry {
    edge_id: Id,
    operation: EdgeOp,
    timestamp: TxnId,
}

// Transaction appends entry
txn.append_edge_log(vertex_id, EdgeLogEntry {
    edge_id: new_edge,
    operation: EdgeOp::Add,
    timestamp: txn_id,
});
```

---

**Option C**: Implement Atomic Edge Append

```rust
// Add new operation type
pub enum CellOp {
    // ... existing ops
    AppendEdge(VertexId, EdgeId),
    RemoveEdge(VertexId, EdgeId),
}

// Commutative operations - safe under relaxed protocol
```

---

## Testing Your Current Implementation

Run this test to check if you lose edges:

```rust
#[test]
async fn test_concurrent_edge_additions() {
    let vertex_id = Id::rand();
    
    // Create vertex
    let vertex = create_vertex(vertex_id);
    write_cell(vertex).await;
    
    // Concurrent transactions add edges
    let t1 = tokio::spawn(async move {
        let txn = begin().await;
        add_edge(txn, vertex_id, edge_1).await;
        commit(txn).await
    });
    
    let t2 = tokio::spawn(async move {
        let txn = begin().await;
        add_edge(txn, vertex_id, edge_2).await;
        commit(txn).await
    });
    
    t1.await;
    t2.await;
    
    // Check: both edges should exist
    let edges = get_edges(vertex_id).await;
    assert!(edges.contains(&edge_1), "Edge 1 lost!");
    assert!(edges.contains(&edge_2), "Edge 2 lost!");
    assert_eq!(edges.len(), 2, "Edge count mismatch!");
}
```

If this test **fails** → you have the read-modify-write problem  
If this test **passes** → your architecture is already safe ✓

---

## Summary

**Edge additions WILL lose edges if**:
- ❌ Edges stored as array in vertex cell
- ❌ Using read-modify-write pattern
- ❌ Under relaxed timestamp protocol

**Edge additions are SAFE if**:
- ✅ Each edge is a separate cell (most graph DBs)
- ✅ Using append-only log
- ✅ Using commutative append operations
- ✅ Version-checked updates with retry

**Most likely**: If you're following typical graph DB patterns (separate edge cells), you're **already safe**! But it's worth testing to be sure.

Would you like me to check your actual edge storage implementation to confirm?


