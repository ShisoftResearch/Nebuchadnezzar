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
| **rename** | an explicit old->new mapping per hop | **ordered** -- must replay each generation in turn |

Only renames need the ordered per-record op list. That matters because
resolution is deliberately ONE hop: a generation-0 cell migrates straight to
generation 3 without materializing 1 or 2. Defaults and coercions stay correct
under that shortcut because they are computed from the endpoints. A rename
cannot be -- `a` became `b` became `c` is not recoverable from comparing
generation 0 with generation 3 -- so each record carries the renames that
produced it, and migration replays generations `source+1 ..= target` in order.

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

## Ordering within one hop

Renames first, then coercions, then defaults, then dynamic drops:

1. **renames** move values to their new keys, so everything after sees the
   target generation's names
2. **coercions** convert values that are present
3. **defaults** fill what is still absent -- after renames, so a renamed field
   is not mistaken for a missing one and overwritten with its default
4. **dynamic drops** purge undeclared names last, so a rename out of the
   dynamic region has already happened

Getting 1 before 3 wrong is the subtle one: a rename applied after defaults
would leave the default sitting in the new key and the real value orphaned.

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

## Non-goals

- User-supplied transform code. The vocabulary stays fixed and mechanical.
- Anything that can fail per-cell at migration time. If it can strand a cell,
  it is refused at admission instead.
- Rewriting `key_field`. Still `Illegal`.
