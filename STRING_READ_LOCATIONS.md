# String Reading Locations in Codebase

## Overview

Strings are read at multiple points in the codebase. Here are the key locations:

## 1. Main Field Reading Path (src/ram/io/reader.rs)

### Entry Point: `read_by_schema()`
```rust:333:340:src/ram/io/reader.rs
pub fn read_by_schema<'v>(ptr: usize, schema: &Schema) -> SharedValue<'v> {
    let mut tail_offset = schema.static_bound;
    let mut schema_value = read_field(ptr, &schema.fields, false, &mut tail_offset, false);
    if schema.is_dynamic {
        read_attach_dynamic_part(ptr + tail_offset, &mut schema_value)
    }
    schema_value
}
```

### Field Reading: `read_field()`
**Location:** `src/ram/io/reader.rs:118-134`

For **variable-length string fields** (like `statement_id`, `property_id`, `literal_id`):

```rust:117:134:src/ram/io/reader.rs
// Case 1: Simple typed fields (primitives) - direct read
(_, false, _, None) => {
    let val = types::get_shared_val(field.data_type, base_ptr + target_offset);
    //                                               ^^^^^^^^^^^^^^^^^^^^
    //                                               THIS IS WHERE STRINGS ARE READ!
    let size = if field_var_base_ty {
        types::get_rsize(field.data_type, &val) // Calculate size for variable-sized types
    } else {
        types::size_of_type(field.data_type) // Use fixed size for fixed types
    };
    trace!(
        "Reading schema field {} shared value {:?}, type {:?}, size {}, offset {}, base {}",
        field.name,
        val,
        field.data_type,
        size,
        target_offset,
        base_ptr
    );
    (val, size)
}
```

**Critical Point:** 
- `target_offset` is calculated earlier in `read_field()` at lines 42-108
- For variable-length string fields in schema region, `target_offset` comes from reading a **pointer value** stored at `schema_field_offset`:

```rust:52:74:src/ram/io/reader.rs
// Case 2: Variable-sized or nullable field in schema region - read indirect offset
(Some(schema_field_offset), true, _, false)
| (Some(schema_field_offset), _, true, false) => {
    // Read the offset value stored at the schema offset
    let rel_offset = *u32_io::read(base_ptr + schema_field_offset) as usize;
    //                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    //                  READS THE POINTER VALUE FROM STATIC REGION!
    if rel_offset == 0 {
        // Zero offset indicates null for nullable fields
        return SharedValue::Null;
    } else {
        *tail_offset = rel_offset; // Update tail_offset to the indirect location
        (*tail_offset, true)  // tail_offset now points to variable region
    }
}
```

Then later, the string is read from:
```rust
base_ptr + target_offset  // where target_offset = tail_offset = rel_offset
```

## 2. Dynamic Map Field Names (src/ram/io/reader.rs)

**Location:** `src/ram/io/reader.rs:299-308`

When reading dynamic maps (maps without schemas):

```rust:299:308:src/ram/io/reader.rs
let field_names = (0..*len)
    .map(|_| {
        *ptr = align_address_with_ty(Type::String, *ptr);
        let name = types::get_shared_val(Type::String, *ptr)
        //                           ^^^^^^^^^^^^^^^^^^^^^^
        //                           READS STRING FROM VARIABLE REGION
            .string()
            .unwrap()
            .to_owned();
        *ptr += types::string_io::size_at(*ptr);
        name
    })
    .collect_vec();
```

## 3. Dynamic Values (src/ram/io/reader.rs)

**Location:** `src/ram/io/reader.rs:324-330`

```rust:324:330:src/ram/io/reader.rs
} else {
    let ty = Type::from_id(type_id);
    *ptr = align_address_with_ty(ty, *ptr);
    let value = types::get_shared_val(ty, *ptr);
    //                         ^^^^^^^^^^^^^^^^^^
    //                         READS ANY TYPE INCLUDING STRINGS
    *ptr += types::get_size(ty, *ptr);
    return value;
}
```

## 4. Low-Level String Reading (dovahkiin types)

The actual string reading happens in `types::get_shared_val()` which calls the **dovahkiin** library:

### String Reading Sequence:

1. **Read length** (u32, 4 bytes) - This is where misalignment crashes occur!
   - Uses: `dovahkiin::types::u32_io::read(ptr)`
   - **REQUIRES 4-byte alignment!**

2. **Read data** (variable length bytes)
   - Uses: `dovahkiin::types::string_io::read(ptr)`
   - Internally calls `string_io::read()` which:
     - Reads u32 length at `ptr`
     - Reads `length` bytes starting at `ptr + 4`
     - Validates UTF-8

### The Critical Call Chain:

```
read_by_schema()
  ↓
read_field()  [reader.rs:24]
  ↓
Case 2: Read pointer from static region [reader.rs:56]
  ↓
u32_io::read(base_ptr + schema_field_offset)  [reads pointer value]
  ↓
Set target_offset = rel_offset
  ↓
types::get_shared_val(Type::String, base_ptr + target_offset)  [reader.rs:119]
  ↓
dovahkiin::types::get_shared_val(Type::String, ptr)
  ↓
dovahkiin::types::string_io::read(ptr)  ← ERROR HAPPENS HERE!
  ↓
dovahkiin::types::u32_io::read(ptr)  ← READS LENGTH (requires 4-byte alignment)
  ↓
PANIC if ptr % 4 != 0
```

## 5. Where Pointer Values Are Stored (Writer Side)

**Location:** `src/ram/io/writer.rs:120-125`

When writing variable-length string fields:

```rust:111:125:src/ram/io/writer.rs
} else if is_field_var {
    // Write position tag for variable sized field
    if !is_var {
        // No need to jump to var region when it is var
        trace!(
            "Push var field jump tailing inst with {} at {:?}",
            tail_offset,
            schema_offset
        );
        *tail_offset = align_address_with_ty(field.data_type, *tail_offset);
        //                                                ^^^^^^^^^^^^^^^^^^
        //                                                ALIGNS TAIL_OFFSET FOR STRING
        ins.push(Instruction {
            data_type: Type::U32,
            val: InstData::Val(OwnedValue::U32(*tail_offset as u32)),
            //                              ^^^^^^^^^^^^^^^^^^^^^^^^^^^
            //                              THIS VALUE IS STORED AT schema_offset!
            offset: schema_offset.unwrap(),
        });
    }
```

## Critical Analysis: The Bug

### What Should Happen:

1. **Writer** (with NEW schema, static_bound=48):
   - Aligns tail_offset to 4 bytes for string: `tail_offset = align_address_with_ty(Type::String, 48)` → `48` (already aligned)
   - Stores pointer value `48` at offset `32` (statement_id field)

2. **Reader** (with SAME schema, static_bound=48):
   - Reads pointer from offset `32`: gets `48`
   - Calculates target: `base_ptr + 48`
   - Reads string from `base_ptr + 48`: ✓ Correct!

### What Might Be Going Wrong:

1. **Schema Mismatch**:
   - Writer uses schema with static_bound=48
   - Reader uses **DIFFERENT schema** with static_bound=44
   - Writer stores pointer `48` 
   - Reader expects pointer `44`
   - Reads from wrong offset → garbage length!

2. **Alignment Issue**:
   - Writer stores aligned tail_offset (e.g., 48)
   - But base_ptr itself is misaligned (e.g., ends in 0xE6)
   - `base_ptr + 48` is still misaligned!
   - Reading u32 length from misaligned address → PANIC!

3. **Pointer Corruption**:
   - Pointer value stored correctly (48)
   - But corrupted before reading (cell location corruption?)
   - Wrong address read → garbage length!

## Recommendations:

1. **Add logging** to see what pointer values are written vs read:
   ```rust
   // In writer.rs:120
   println!("WRITE: field={}, storing pointer={} at offset={}", 
            field.name, *tail_offset, schema_offset.unwrap());
   
   // In reader.rs:56
   println!("READ: field={}, read pointer={} from offset={}", 
            field.name, rel_offset, schema_field_offset);
   ```

2. **Verify base_ptr alignment** before reading:
   ```rust
   // In reader.rs:119, before get_shared_val
   let read_addr = base_ptr + target_offset;
   assert_eq!(read_addr % 4, 0, "String read address must be 4-byte aligned!");
   ```

3. **Check schema matching**:
   - Ensure writer and reader use the SAME schema object
   - Verify schema.static_bound matches between write and read

