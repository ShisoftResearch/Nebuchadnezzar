// Temporary offline segment-file scanner for corpse forensics. NOT part of
// the harness; safe to delete. Walks every segment file under a corpse dir
// and prints every CELL entry (id, version, schema) matching target ids,
// plus TOMBSTONE entries for them.
use neb::ram::cell::cell_header_from_entry_content_addr;
use neb::ram::entry::{Entry, EntryType, ENTRY_HEAD_SIZE};
use neb::ram::recovery::load_file_with_used_len;
use neb::ram::tombstone::Tombstone;
use neb::ram::types::Id;
use std::path::PathBuf;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = args.get(1).expect("dir");
    let targets: Vec<u64> = args[2..]
        .iter()
        .map(|s| s.parse::<u64>().expect("id number"))
        .collect();

    let mut files: Vec<PathBuf> = Vec::new();
    for sub in ["backup", "wal"] {
        let root = PathBuf::from(dir).join(sub);
        if let Ok(walk) = std::fs::read_dir(&root) {
            for entry in walk.flatten() {
                if entry.path().is_dir() {
                    if let Ok(inner) = std::fs::read_dir(entry.path()) {
                        for f in inner.flatten() {
                            files.push(f.path());
                        }
                    }
                } else {
                    files.push(entry.path());
                }
            }
        }
    }
    files.sort();

    for path in files {
        let Ok((data, used_len)) = load_file_with_used_len(&path) else {
            println!("UNREADABLE {}", path.display());
            continue;
        };
        let live = used_len.map(|l| l.min(data.len())).unwrap_or(data.len());
        let base = data.as_ptr() as usize;
        let bound = base + live;
        let mut cursor = base;
        let mut entries = 0usize;
        while cursor + ENTRY_HEAD_SIZE <= bound {
            let type_bits =
                u32::from_le_bytes(unsafe { *((cursor) as *const [u8; 4]) });
            let (neb::ram::entry::TypeWord::Unchecked(_)
            | neb::ram::entry::TypeWord::Checked(_, _)) =
                neb::ram::entry::unpack_type_word(type_bits)
            else {
                println!(
                    "  {}: invalid entry type bits {} at offset {} ({} entries before)",
                    path.display(),
                    type_bits,
                    cursor - base,
                    entries
                );
                break;
            };
            let (header, _) = Entry::decode_from(cursor, |_, h| h);
            if header.entry_type == EntryType::UNDECIDED || header.content_length == 0 {
                break;
            }
            let size = ENTRY_HEAD_SIZE + header.content_length as usize;
            if cursor + size > bound {
                println!(
                    "  {}: torn tail at offset {} (entry size {})",
                    path.display(),
                    cursor - base,
                    size
                );
                break;
            }
            let content = Entry::content_pos(cursor);
            if header.entry_type == EntryType::CELL {
                let ch = cell_header_from_entry_content_addr(content);
                if targets.is_empty() || targets.contains(&ch.id.bits()) {
                    let body = unsafe {
                        std::slice::from_raw_parts(content as *const u8, size - ENTRY_HEAD_SIZE)
                    };
                    println!(
                        "CELL file={} offset={} id={} version={} schema={} size={} body={}",
                        path.display(),
                        cursor - base,
                        ch.id.bits(),
                        ch.version,
                        ch.schema,
                        size,
                        body.iter().map(|b| format!("{:02x}", b)).collect::<String>()
                    );
                }
            } else if header.entry_type == EntryType::TOMBSTONE {
                let ts = Tombstone::read_from_entry_content_addr(content);
                if targets.is_empty() || targets.contains(&ts.id.bits()) {
                    println!(
                        "TOMBSTONE file={} offset={} id={} version={}",
                        path.display(),
                        cursor - base,
                        ts.id.bits(),
                        ts.version
                    );
                }
            }
            entries += 1;
            cursor += size;
        }
    }
    let _ = Id::unit_id();
}
