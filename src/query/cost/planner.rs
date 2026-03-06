pub fn indexed_clause_priority(
    supports_hashed: bool,
    supports_ranged: bool,
    is_equality: bool,
    start_open: bool,
    end_open: bool,
) -> u8 {
    if supports_hashed && is_equality {
        return 100;
    }
    if supports_ranged {
        if is_equality {
            return 90;
        }
        if !start_open && !end_open {
            return 70;
        }
        if !start_open || !end_open {
            return 50;
        }
        return 30;
    }
    10
}
