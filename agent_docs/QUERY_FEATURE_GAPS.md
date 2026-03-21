# Neb Basic Query Feature Gaps

This note summarizes SQL-style query features that are still unavailable, partially available, or constrained in Neb's current structured query path. The scope here is basic single-table querying without joins, using the behavior behind `IndexedDataClient`.

## Supported Core Surface

- Boolean predicates with nested `and` and `or`
- Scalar comparisons: `=`, `>`, `>=`, `<`, `<=`
- First-class `IN (...)`
- First-class `BETWEEN`
- `IS NULL` / `IS NOT NULL`
- Equality over hashed indexes
- Range predicates over ranged indexes
- Explicit ordering on indexed and unindexed scalar fields
- Field-list `DISTINCT` with executor-side post-processing
- Query ordering without explicit `ORDER BY`
- `LIMIT`
- `OFFSET`
- Schema-scan fallback when the schema is marked `scannable`

## Missing or Constrained Features

- `SELECT *` / full scan is not universally available
  - Neb requires the schema to be created with `scannable = true` before it can fall back to a schema scan.
  - On non-scannable schemas, queries without a usable indexed path are not generally available.

- Arbitrary `ORDER BY` is still constrained
  - Explicit `ORDER BY` works for schema fields, including unindexed fields, but unindexed or hashed-only sort keys are handled as post-processing sorts over the candidate set.
  - Neb still does not expose SQL-style planner choices such as external sort, partial sort, or expression-based sort keys.

- `ORDER BY` is field-only, not expression-based
  - Neb does not currently support `ORDER BY expr`, `ORDER BY a + b`, or function-based sort keys.

- `GROUP BY` is not available
  - There is no grouped aggregation path in the current structured query interface.

- Aggregate functions are not available in the structured query path
  - `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` are not exposed as SQL-style query operators here.

- `HAVING` is not available
  - Because grouped aggregation is unavailable, post-aggregation filtering is also unavailable.

- `DISTINCT` is only partially available
  - Neb now supports executor-side distinct over an explicit field list, retaining the first row in final query order for each distinct key.
  - This is closer to `DISTINCT ON (field_list)` semantics than full SQL projection-level `SELECT DISTINCT column_list`.
  - There is still no planner-level distinct pushdown or projection-driven distinct stage.

- Projection is limited compared with SQL `SELECT column_list`
  - The structured query APIs are primarily centered on cell IDs or whole-cell reads.
  - There is no full SQL projection layer with aliases, computed columns, or projection-only query planning.
  - Because of that, Neb does not yet expose full SQL-style `SELECT DISTINCT projection_list` semantics independently from row identity.

- `NOT` is available, but negation remains planner-limited
  - Neb now supports `not` as a structured predicate operator.
  - Ranged scalar negation is planner-aware: negated comparisons are normalized into indexed ranged clauses or ranged disjunctions where that rewrite is sound.
  - Hashed and other non-ordered indexes still do not have full negation-aware index plans, so those cases fall back to residual filtering or schema scan when needed.

- `IN`, `BETWEEN`, and null predicates are now available, but planner coverage is still operator-specific
  - `IN` is normalized into disjunctions of equality clauses, so hashed and ranged equality planning can be reused.
  - `BETWEEN` is normalized into paired range comparisons, so ranged planning can be reused.
  - `IS NULL` is index-backed when the field carries the dedicated `Null` index.
  - `IS NOT NULL` is planner-aware for ranged indexed nullable fields by scanning the non-null index domain.
  - Without a `Null` index, `IS NULL` still requires residual filtering or schema scan unless it can be proven impossible on a non-nullable field.

- Pattern matching operators are not available in the structured scalar path
  - There is no SQL-style `LIKE`, `ILIKE`, or wildcard matcher in this basic query surface.
  - Full-text and semantic search exist separately, but they are not direct substitutes for scalar pattern predicates.

- Set operators are not available
  - `UNION`, `INTERSECT`, and `EXCEPT` are not exposed as SQL-style query operators across multiple selects.

- Subqueries are not available
  - There is no SQL-style nested `SELECT` in predicates, projections, or derived tables.

- Window functions are not available
  - No `OVER (...)`, ranking, running aggregates, or partitioned analytic functions.

- Query semantics are still index-aware rather than fully relational
  - Whether a query can run, and how it runs, still depends materially on index availability and schema scan capability.
  - This is different from SQLite, where the same logical query remains available regardless of index presence.

## Practical Implications

- If a query needs flexible filtering on unindexed fields, the schema must be created as scannable.
- If a query needs explicit ordering on large candidate sets, a ranged index is still the efficient path; unindexed ordering is available but falls back to in-memory post-sort.
- If a query needs `DISTINCT`, Neb currently applies it after final ordering and before `OFFSET`/`LIMIT`, so the retained representative row depends on query order.
- full scans still require scannable schemas when no usable indexed path exists

## Alignment Work Relevance

The SQLite alignment harness should treat Neb's current supported subset as:

- boolean predicate trees
- scalar comparison predicates
- explicit `ORDER BY` on schema fields, with post-sort fallback for non-ranged fields
- field-list `DISTINCT` with post-processing retention of the first row in final order
- implicit query ordering
- `LIMIT` and `OFFSET`
- schema-scan fallback only on scannable schemas

Anything outside that subset should either be excluded from the harness or marked as an intentional unsupported feature rather than a correctness failure.
