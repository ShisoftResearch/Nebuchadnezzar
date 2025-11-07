/// Check what alignment value is used for strings
use crate::ram::io::align_address_with_ty;
use crate::ram::types;
use dovahkiin::types::Type;

#[test]
fn check_string_alignment_value() {
    println!("\n=== Checking String Alignment ===\n");

    // Check what alignment value is returned for Type::String
    let string_align = types::align_of_type(Type::String);
    println!("align_of_type(Type::String) = {}", string_align);

    // Test alignment at various offsets
    let test_offsets = vec![44, 45, 46, 47, 48, 49, 50];

    println!("\nAlignment behavior for Type::String:");
    for offset in test_offsets {
        let aligned = align_address_with_ty(Type::String, offset);
        let adjustment = aligned - offset;
        println!(
            "  offset {} → aligned {} (adjustment: {})",
            offset, aligned, adjustment
        );
    }

    // Test for other types for comparison
    println!("\nAlignment for U32 (string length type):");
    let u32_align = types::align_of_type(Type::U32);
    println!("align_of_type(Type::U32) = {}", u32_align);

    for offset in vec![44, 45, 46, 47, 48] {
        let aligned = align_address_with_ty(Type::U32, offset);
        println!("  offset {} → aligned {}", offset, aligned);
    }

    // CRITICAL: Check if string alignment is 4 bytes
    println!("\n=== CRITICAL CHECK ===");
    if string_align == 4 {
        println!("✓ String alignment is 4 bytes (correct for u32 length field)");
    } else if string_align == 1 {
        println!("⚠️  String alignment is 1 byte (no alignment!)");
    } else {
        println!(
            "❌ String alignment is {} bytes (unexpected!)",
            string_align
        );
    }
}

#[test]
fn test_alignment_math_for_static_bound() {
    println!("\n=== Testing Alignment Math for static_bound ===\n");

    // Simulate what happens with static_bound values
    let old_static_bound = 44;
    let new_static_bound = 48;

    println!("OLD schema: static_bound = {}", old_static_bound);
    let old_aligned = align_address_with_ty(Type::String, old_static_bound);
    println!("  First string would be at: {}", old_aligned);
    println!("  Adjustment: {}", old_aligned - old_static_bound);
    println!();

    println!("NEW schema: static_bound = {}", new_static_bound);
    let new_aligned = align_address_with_ty(Type::String, new_static_bound);
    println!("  First string would be at: {}", new_aligned);
    println!("  Adjustment: {}", new_aligned - new_static_bound);
    println!();

    if old_aligned == new_aligned {
        println!(
            "✓ Both schemas align first string to same offset: {}",
            old_aligned
        );
        println!("  This means old and new cells would be compatible!");
    } else {
        println!("❌ MISMATCH!");
        println!("  Old cells: string at offset {}", old_aligned);
        println!("  New code expects: string at offset {}", new_aligned);
        println!("  Difference: {} bytes", new_aligned - old_aligned);
    }
}
