# Schema Transform Engine Design

**Builds on:** `docs/superpowers/specs/2026-08-25-schema-uid-vid-design.md`
**Replaces:** Task 9 of `docs/superpowers/plans/2026-08-25-schema-uid-vid.md`,
which deliberately deferred this until a real need named the transforms. All
four refused classes are now wanted, so this is that replan.

## Problem

Evolution today admits only *identity* changes -- ones where decode-under-the-
old-generation then encode-under-the-new one already produces the right bytes.
Everything else is refused at admission with a reason. Five refusals exist and
four of them are now needed:

| refused | why the encoder alone cannot do it |
|---|---|
| add a non-nullable field | no value exists in any old cell |
| a field stops being nullable | cells holding null have nothing to encode |
| type change, incl. widening | the value must be rewritten, not re-laid-out |
| scalar <-> array, vector width | the value's shape changes |
| drop a field from a **dynamic** schema | it is not dropped; it falls into the dynamic region and is re-encoded |

(Changing `key_field` stays `Illegal`. Cell ids derive from it, so no transform
can avoid orphaning every cell already written.)

## The shape the information takes

The plan assumed one mechanism: an ordered list of ops stored per record. That
is right for exactly one of the four. Sorting the transforms by *where their
information lives* gives a much smaller design:

| transform | information comes from | composition across a multi-hop migration |
|---|---|---|
| **default** | the target `Field` itself | none -- "absent" is absent however many generations back |
| **coercion** | the source and target `Field` types | none -- both records are in hand |
| **dynamic drop** | an explicit purge list on the target | set union; order does not matter |
| **rename** | an explicit old->new mapping, folded forward | none -- see the correction below |

**Correction, made during implementation.** This section originally said
renames need an ordered replay of generations `source+1 ..= target`, on the
grounds that `a` became `b` became `c` is not recoverable from comparing the
endpoints. That is true of the endpoints, but it is not true of the record: a
rename list that is FOLDED FORWARD at evolution time carries the answer.

If a family already knows `a -> b` and the next hop renames `b -> c`, the
inherited entry is rewritten to `a -> c` and `b -> c` is added. Both the
original and the intermediate name then resolve to the current one from the
target record alone, and no generation in between is ever consulted.

So **nothing replays.** Every transform here resolves from the target record:
defaults ride on the field, drops and renames accumulate. That is what keeps
migration a single hop whatever the generation depth -- the hundredth evolution
costs a cell no more than the first, which an ordered replay would not have.

What survives of the original worry is narrower, and is handled by refusal
rather than machinery: two rename shapes genuinely cannot be resolved without
knowing which generation a cell came from.

- a **swap** (`a -> b` and `b -> a` in one hop) is order-dependent
- renaming **onto a live field name** leaves two fields reading one source

Both are `Illegal`.

## Metadata

`Field` gains one field:

```rust
pub struct Field {
    // ...
    /// The value a cell gets for this field when it has none: a cell written
    /// before the field existed, or one holding null for a field that has
    /// stopped being nullable. `None` means the field is genuinely required
    /// and an evolution that adds it must be refused.
    #[serde(default)]
    pub default: Option<OwnedValue>,
}
```

`Schema` gains one field:

```rust
/// How this generation was produced from the one it superseded. Empty for a
/// generation-0 record, which superseded nothing.
#[serde(default)]
pub transform: SchemaTransform,

#[derive(Default)]
pub struct SchemaTransform {
    /// Field path hash, old -> new. This hop only; migration composes.
    pub renames: Vec<(u64, u64)>,
    /// Names to purge from a dynamic schema's dynamic region.
    pub dynamic_drops: Vec<u64>,
}
```

Both are `#[serde(default)]` -- not for backward compatibility, which this
campaign does not keep, but because the overwhelmingly common record has
neither and should not pay for them in every snapshot.

## Where it runs

One function, between decode and encode:

```
cell bytes --decode(source gen)--> value map --apply_transform--> value map --encode(target gen)--> bytes
```

`OwnedCell::plan_write` already knows both ends: `self.header.schema` is the
source generation and the resolved schema is the target. So the transform is
applied there, which puts it on the write path and the cleaner path at once --
they both reach the encoder through `plan_write`, and the campaign has already
paid for making that the single chokepoint.

The contract is: **the generation you name describes the shape of the data you
supply.** A caller naming a stale vid is saying "this map is in that
generation's shape", and migration moves it forward.

Every op is written to be idempotent, so a caller who names a stale vid but
supplies data already in the new shape is not corrupted -- a rename finds no
source key and does nothing, a default fills only what is absent, a coercion
that sees the target type already is a no-op.

## Ordering within one field

The transforms did not become a pipeline of passes over a value map. Each one
lands where the encoder already makes the decision it affects, which gets the
ordering for free rather than by arranging it:

1. **rename** resolves at the field LOOKUP -- before anything has a value, so a
   renamed field is never mistaken for a missing one
2. **coercion** and **default** are one substitution at the point the encoder
   would otherwise write the value or refuse
3. **dynamic drops** filter the dynamic region, which is walked after the
   declared fields

The hazard the original ordering worried about -- a default overwriting a
renamed value -- cannot arise, because the default is only reached when the
lookup, rename fallback included, found nothing.

## Admission

`classify_evolution` stops refusing what the engine can now express:

- an added non-nullable field with a `default` -> admitted
- a field that stops being nullable, with a `default` -> admitted
- a type change with a defined coercion -> admitted
- a dynamic-schema drop listed in `dynamic_drops` -> admitted
- a rename declared in `renames` -> admitted, and the dropped/added pair it
  would otherwise look like is suppressed

Anything still without the information it needs is refused exactly as now, with
a message naming what is missing. **Refusal at admission stays the design's
backbone**: an evolution that cannot be expressed must be rejected when it is
proposed, not discovered mid-migration on a cleaner thread.

## Coercions

Only widenings that cannot fail, so migration never has to decide what to do
with a value that does not fit:

- `U8 -> U16 -> U32 -> U64`, `I8 -> I16 -> I32 -> I64`
- `U8/U16/U32 -> I64`, and any integer -> `F64` where exactly representable
- scalar `T` -> array of `T` (wraps in a one-element array)

Narrowing, array -> scalar, and vector width changes stay refused. They are
value-dependent, and an evolution that admits at proposal time but strands an
arbitrary subset of cells at migration time is worse than one that is refused.

## Sequencing

1. **Defaults.** Covers two of the five refusals, needs no composition, and is
   the most commonly wanted. Field-local.
2. **Coercions.** Endpoint-derived, no composition.
3. **Dynamic drops.** Set union across hops.
4. **Renames.** The ordered replay, and the only piece that needs it.

Each increment is independently landable, and each ends with the same gate the
uid/vid campaign used: focused tests, then the full lib suite at
`--test-threads=8`.

## The delta API

`evolve_schema` takes a whole target shape, which turned out to be a poor thing
to ask a caller for: every unchanged field has to be re-declared, and one left
off the list is silently dropped rather than refused. Losing a column by
omission is not an acceptable failure mode for a schema tool.

`SchemaEdit` describes the CHANGE instead:

```rust
client.evolve("person", SchemaEdit::new()
    .rename("old_name", "new_name")
    .retype("age", Type::U64)
    .add(Field::new_unindexed("rank", Type::U64).with_default(OwnedValue::U64(0)))
    .drop("legacy")
).await??;
```

Three properties earn it:

- **Unchanged fields carry.** Anything the edit does not mention survives.
- **Transforms declare themselves.** `.rename()` records the rename rather than
  just moving the field, and `.drop()` on a dynamic schema records the purge.
  Neither can be forgotten, which is the other way a hand-built target goes
  wrong.
- **A stale base is refused.** `evolve` reads the current generation, applies
  the edit, and sends that vid as a precondition. An evolution landing in
  between yields `StaleBase { expected, actual }` with nothing changed, rather
  than an edit built on an old shape quietly undoing it.

It is ergonomics over `evolve_schema`, not a way around admission: an edit that
produces an inexpressible schema is refused exactly as a hand-built one is. And
it rebuilds through `Schema::new` rather than mutating a clone, so offsets, the
field indexes and the compression plan are recomputed -- hand-patching those is
how a schema ends up describing a layout it does not produce.

## Non-goals

- User-supplied transform code. The vocabulary stays fixed and mechanical.
- Anything that can fail per-cell at migration time. If it can strand a cell,
  it is refused at admission instead.
- Rewriting `key_field`. Still `Illegal`.
