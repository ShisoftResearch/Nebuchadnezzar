/// Test to demonstrate the root cause of misaligned pointer dereference
///
/// The problem: Even though field offsets are properly aligned RELATIVE to the base,
/// if the base address (cell location) itself is misaligned, all absolute addresses
/// will be misaligned.
use crate::ram::io::{align_address, align_address_with_ty};
use dovahkiin::types::Type;

#[test]
fn test_alignment_propagation_from_base() {
    println!("Demonstrating how misaligned base addresses cause downstream misalignment");
    println!();

    // Scenario 1: CORRECT - Properly aligned base address
    let correct_base = 0x0000_7f00_0000_0000_usize; // 8-byte aligned
    let field_offset_0 = 0; // u32 at offset 0
    let field_offset_8 = 8; // String length (u32) at offset 8

    println!("=== Scenario 1: CORRECT (8-byte aligned base) ===");
    println!(
        "Base address:     0x{:016x} (aligned: {})",
        correct_base,
        correct_base % 8 == 0
    );

    let addr_0 = correct_base + field_offset_0;
    let addr_8 = correct_base + field_offset_8;

    println!(
        "Field at offset 0:  0x{:016x} (4-byte aligned: {})",
        addr_0,
        addr_0 % 4 == 0
    );
    println!(
        "Field at offset 8:  0x{:016x} (4-byte aligned: {})",
        addr_8,
        addr_8 % 4 == 0
    );
    println!("✅ All addresses are properly aligned");
    println!();

    // Scenario 2: INCORRECT - Misaligned base address (like in crash)
    let corrupted_base = 0x6c395a4000e6_usize; // Misaligned by 6 bytes (like actual crash)

    println!("=== Scenario 2: CORRUPTED (misaligned base like 0x...E6) ===");
    println!(
        "Base address:     0x{:016x} (aligned: {}, offset: {})",
        corrupted_base,
        corrupted_base % 8 == 0,
        corrupted_base % 8
    );

    // Even with "aligned" offsets, the absolute addresses are wrong
    let addr_0_bad = corrupted_base + field_offset_0;
    let addr_8_bad = corrupted_base + field_offset_8;

    println!(
        "Field at offset 0:  0x{:016x} (4-byte aligned: {}, actual offset: {})",
        addr_0_bad,
        addr_0_bad % 4 == 0,
        addr_0_bad % 4
    );
    println!(
        "Field at offset 8:  0x{:016x} (4-byte aligned: {}, actual offset: {})",
        addr_8_bad,
        addr_8_bad % 4 == 0,
        addr_8_bad % 4
    );
    println!("❌ Even with 'aligned' offsets, absolute addresses are MISALIGNED");
    println!();

    // Demonstrate that alignment functions can't fix a bad base address
    println!("=== Why alignment functions don't help ===");

    let misaligned_base = 0x1000_00e6_usize;
    println!(
        "Misaligned base:  0x{:016x} (offset: {})",
        misaligned_base,
        misaligned_base % 8
    );

    // Try to align an offset from this base
    let field_offset = 0;
    let aligned_offset = align_address_with_ty(Type::U32, field_offset);
    println!(
        "Aligned offset:   {} (this is correct - offset 0 is already aligned)",
        aligned_offset
    );

    let final_address = misaligned_base + aligned_offset;
    println!(
        "Final address:    0x{:016x} (4-byte aligned: {})",
        final_address,
        final_address % 4 == 0
    );
    println!("❌ Result is STILL misaligned because base is wrong!");
    println!();

    // Show what SHOULD happen
    let correct_base = align_address(8, misaligned_base);
    println!("What base SHOULD be: 0x{:016x}", correct_base);
    let correct_final = correct_base + aligned_offset;
    println!(
        "Correct final addr:  0x{:016x} (4-byte aligned: {})",
        correct_final,
        correct_final % 4 == 0
    );
    println!("✅ Now it's properly aligned!");
    println!();

    // Key insight
    println!("=== KEY INSIGHT ===");
    println!("The alignment functions work on OFFSETS, not BASE addresses.");
    println!("If the cell location (base address) stored in cell_index is corrupted,");
    println!("NO amount of offset alignment will fix the resulting absolute addresses.");
    println!();
    println!("This is why we see:");
    println!("  - Address 0x6c395a4000e6 trying to read u32");
    println!("  - Last byte 0xE6 means offset of 6 from 8-byte boundary");
    println!("  - u32 needs 4-byte alignment, but 6 % 4 = 2 ❌");
    println!();
    println!("ROOT CAUSE: The cell location stored in the cell index is CORRUPTED.");
    println!("It should be 8-byte aligned but it's not.");

    // Verify the actual crash addresses
    assert_eq!(
        corrupted_base % 8,
        6,
        "Crash address has offset 6 from 8-byte boundary"
    );
    assert_eq!(
        corrupted_base % 4,
        2,
        "Crash address has offset 2 from 4-byte boundary"
    );
    assert_ne!(corrupted_base % 4, 0, "Crash address is NOT 4-byte aligned");
}

#[test]
fn test_cell_location_must_be_8_byte_aligned() {
    println!("Testing that cell locations MUST be 8-byte aligned for safe access");
    println!();

    // The cell header contains u64 values (version, timestamp, partition, hash)
    // Reading these requires proper alignment
    let valid_bases = vec![
        0x0000_0000_0000_0000_usize,
        0x0000_0000_0000_0008_usize,
        0x0000_0000_0000_0010_usize,
        0x7f00_0000_0000_0000_usize,
    ];

    println!("=== Valid cell locations (8-byte aligned) ===");
    for base in &valid_bases {
        println!("  0x{:016x} - alignment check: {}", base, base % 8 == 0);
        assert_eq!(base % 8, 0, "Cell location must be 8-byte aligned");
    }
    println!();

    // Invalid bases that would cause misaligned reads
    let invalid_bases = vec![
        0x0000_0000_0000_00e6_usize, // Like crash addresses
        0x6c395a4000e6_usize,        // Actual crash address
        0x6c51974000e6_usize,        // Another crash address
        0x0000_0000_0000_0001_usize, // Off by 1
        0x0000_0000_0000_0004_usize, // 4-byte aligned but not 8-byte
    ];

    println!("=== Invalid cell locations (NOT 8-byte aligned) ===");
    for base in &invalid_bases {
        let offset = base % 8;
        println!(
            "  0x{:016x} - offset: {}, 4-byte aligned: {}",
            base,
            offset,
            base % 4 == 0
        );
        assert_ne!(base % 8, 0, "These should NOT be 8-byte aligned");

        // Show what happens when you try to read a u32 field at offset 0
        let u32_addr = base + 0; // Trying to read u32 at the start
        if u32_addr % 4 != 0 {
            println!(
                "    ❌ Reading u32 at 0x{:016x} would PANIC (not 4-byte aligned)",
                u32_addr
            );
        }
    }
    println!();

    println!("=== CONCLUSION ===");
    println!("Cell locations stored in cell_index MUST be 8-byte aligned.");
    println!("Otherwise, ANY field read (even at offset 0) may be misaligned.");
    println!("The validation I added will catch when non-aligned addresses");
    println!("are stored in the cell index.");
}

#[test]
fn test_alignment_math() {
    println!("Understanding the alignment mathematics");
    println!();

    // The crash address pattern
    let addr = 0x6c395a4000e6_usize;

    println!("Crash address: 0x{:016x}", addr);
    println!("Binary (last byte): {:08b}", addr & 0xFF);
    println!("Last byte (hex): 0x{:02x}", addr & 0xFF);
    println!();

    // Check various alignments
    for alignment in [2, 4, 8, 16] {
        let offset = addr % alignment;
        let is_aligned = offset == 0;
        println!(
            "{}-byte alignment: offset={}, aligned={}",
            alignment, offset, is_aligned
        );

        if !is_aligned {
            let next_aligned = addr + (alignment - offset);
            println!(
                "  Next {}-byte aligned address: 0x{:016x}",
                alignment, next_aligned
            );
        }
    }
    println!();

    println!("=== Why 0xE6 is problematic ===");
    println!("0xE6 = 230 in decimal");
    println!("230 % 2 = {} (2-byte alignment OK)", 230 % 2);
    println!("230 % 4 = {} (4-byte alignment FAIL)", 230 % 4);
    println!("230 % 8 = {} (8-byte alignment FAIL)", 230 % 8);
    println!();
    println!("For an address ending in 0xE6:");
    println!("  - You CAN'T read u32 (needs 4-byte alignment)");
    println!("  - You CAN'T read u64 (needs 8-byte alignment)");
    println!("  - You CAN read u8 or u16 (2-byte alignment OK)");

    // Verify
    assert_eq!(addr % 4, 2, "Address ending in 0xE6 has 4-byte offset of 2");
    assert_eq!(addr % 8, 6, "Address ending in 0xE6 has 8-byte offset of 6");
}
