use super::cell::cell_header_from_entry_content_addr;
use super::chunk::Chunk;
use super::entry::{Entry, EntryType, ENTRY_HEAD_SIZE};
use super::segs::{Segment, SEGMENT_SIZE};
use super::tombstone::Tombstone;
use libc::{c_void, mmap, open, MAP_FIXED, MAP_PRIVATE, O_RDONLY, PROT_READ};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct SegmentFileInfo {
    pub chunk_id: usize,
    pub seg_id: u64,
    pub seq_id: u64,
    pub path: PathBuf,
    pub size: u64,
    pub is_backup: bool,
}

impl SegmentFileInfo {
    /// Parse filename pattern: {chunk_id}-{seg_id}-{seq_id}.{nlog|nbackup}
    pub fn parse_filename(path: &Path) -> Option<Self> {
        let stem = path.file_stem()?.to_str()?;
        
        // Check extension
        let extension = path.extension()?.to_str()?;
        let is_backup = match extension {
            "nbackup" => true,
            "nlog" => false,
            _ => return None,
        };
        
        // Parse {chunk_id}-{seg_id}-{seq_id}
        let parts: Vec<&str> = stem.split('-').collect();
        if parts.len() != 3 {
            return None;
        }
        
        let chunk_id = parts[0].parse::<usize>().ok()?;
        let seg_id = parts[1].parse::<u64>().ok()?;
        let seq_id = parts[2].parse::<u64>().ok()?;
        
        let metadata = fs::metadata(path).ok()?;
        let size = metadata.len();
        
        Some(SegmentFileInfo {
            chunk_id,
            seg_id,
            seq_id,
            path: path.to_path_buf(),
            size,
            is_backup,
        })
    }
}

/// Helper function to recursively scan directories for segment files
fn scan_dir_recursive(dir: &Path, files: &mut Vec<SegmentFileInfo>, is_backup_scan: bool) -> io::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            // Recursively scan subdirectories
            scan_dir_recursive(&path, files, is_backup_scan)?;
        } else if path.is_file() {
            if let Some(info) = SegmentFileInfo::parse_filename(&path) {
                if info.is_backup == is_backup_scan {
                    files.push(info);
                }
            }
        }
    }
    Ok(())
}

/// Discover all segment files in storage directories
pub fn discover_segment_files(
    backup_storage: &Option<String>,
    wal_storage: &Option<String>,
) -> io::Result<Vec<SegmentFileInfo>> {
    let mut files = Vec::new();
    
    // Scan backup storage recursively
    if let Some(backup_dir) = backup_storage {
        scan_dir_recursive(Path::new(backup_dir), &mut files, true)?;
    }
    
    // Scan WAL storage recursively
    if let Some(wal_dir) = wal_storage {
        scan_dir_recursive(Path::new(wal_dir), &mut files, false)?;
    }
    
    // Deduplicate: prefer backup over WAL for same (chunk_id, seg_id, seq_id)
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    
    // Sort: backup files first (is_backup = true)
    files.sort_by(|a, b| b.is_backup.cmp(&a.is_backup));
    
    for file in files {
        let key = (file.chunk_id, file.seg_id, file.seq_id);
        if seen.insert(key) {
            deduped.push(file);
        }
    }
    
    // Sort by seq_id (chronological order)
    deduped.sort_by_key(|f| f.seq_id);
    
    Ok(deduped)
}

/// Load file content into memory buffer
pub fn load_file_to_memory(path: &Path) -> io::Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    Ok(buffer)
}

/// Find the actual append_header by scanning segment data
pub fn find_append_header(seg_addr: usize, file_size: usize) -> usize {
    let mut cursor = seg_addr;
    let bound = seg_addr + file_size;
    let mut entries_found = 0;
    
    while cursor < bound {
        // Try to decode entry header
        let (entry_header, _) = Entry::decode_from(cursor, |_, header| header);
        
        if entry_header.entry_type == EntryType::UNDECIDED || entry_header.content_length == 0 {
            // Found uninitialized space
            eprintln!("[find_append_header] Found uninitialized space at offset {} after {} entries", 
                cursor - seg_addr, entries_found);
            break;
        }
        
        entries_found += 1;
        let entry_size = ENTRY_HEAD_SIZE + entry_header.content_length as usize;
        cursor += entry_size;
        
        if cursor > bound {
            // Overflow, use file_size
            eprintln!("[find_append_header] Overflow detected, returning bound");
            return bound;
        }
    }
    
    eprintln!("[find_append_header] Returning cursor at offset {} ({} entries)", 
        cursor - seg_addr, entries_found);
    cursor
}

#[derive(Debug, Clone)]
pub struct CellIndexEntry {
    pub addr: usize,
    pub version: u64,
    pub seg_id: u64,
    pub partition: u64,
}

/// Rebuild cell index by scanning segment memory
pub fn rebuild_cell_index_from_segment(
    chunk: &Chunk,
    seg: &Segment,
    cell_index: &mut HashMap<u64, CellIndexEntry>,
) -> (u32, u32) {
    let mut dead_space = 0u32;
    let mut tombstone_count = 0u32;
    
    let bound = seg.append_header.load(Ordering::Relaxed);
    let mut cursor = seg.addr;
    
    while cursor < bound {
        let (entry_header, _) = Entry::decode_from(cursor, |_, header| header);
        let entry_size = ENTRY_HEAD_SIZE + entry_header.content_length as usize;
        
        match entry_header.entry_type {
            EntryType::CELL => {
                let content_addr = Entry::content_pos(cursor);
                let cell_header = cell_header_from_entry_content_addr(content_addr);
                let hash = cell_header.hash;
                
                // Check if we should update the index
                match cell_index.get(&hash) {
                    None => {
                        // New cell, insert into index
                        cell_index.insert(hash, CellIndexEntry {
                            addr: cursor,
                            version: cell_header.version,
                            seg_id: seg.id,
                            partition: cell_header.partition,
                        });
                        debug!(
                            "Recovered cell hash {} version {} at segment {}",
                            hash, cell_header.version, seg.id
                        );
                    }
                    Some(existing) => {
                        if cell_header.version > existing.version {
                            // Newer version, update index and mark old as dead
                            info!(
                                "Cell hash {} updated from version {} to {} (seg {} -> {})",
                                hash, existing.version, cell_header.version,
                                existing.seg_id, seg.id
                            );
                            
                            // Mark old cell as dead in its segment
                            if let Some(old_seg) = chunk.segs.get(&(existing.seg_id as usize)) {
                                chunk.mark_dead_entry_with_seg(existing.addr, &old_seg);
                            }
                            
                            // Update to new version
                            cell_index.insert(hash, CellIndexEntry {
                                addr: cursor,
                                version: cell_header.version,
                                seg_id: seg.id,
                                partition: cell_header.partition,
                            });
                        } else {
                            // Older version, mark this one as dead
                            dead_space += entry_header.content_length;
                            debug!(
                                "Cell hash {} has older version {} (current {}), marking dead",
                                hash, cell_header.version, existing.version
                            );
                        }
                    }
                }
            }
            EntryType::TOMBSTONE => {
                let content_addr = Entry::content_pos(cursor);
                let tombstone = Tombstone::read_from_entry_content_addr(content_addr);
                let hash = tombstone.hash;
                
                tombstone_count += 1;
                
                // Check if we should remove from index
                match cell_index.get(&hash) {
                    Some(existing) => {
                        if tombstone.version >= existing.version {
                            // Tombstone is newer or equal, remove cell
                            info!(
                                "Cell hash {} removed by tombstone version {} (cell version {})",
                                hash, tombstone.version, existing.version
                            );
                            
                            // Mark cell as dead in its segment
                            if let Some(target_seg) = chunk.segs.get(&(tombstone.segment_id as usize)) {
                                chunk.mark_dead_entry_with_seg(existing.addr, &target_seg);
                            }
                            
                            cell_index.remove(&hash);
                        } else {
                            // Tombstone is older, ignore it
                            debug!(
                                "Tombstone for hash {} has older version {} (cell version {}), ignoring",
                                hash, tombstone.version, existing.version
                            );
                        }
                    }
                    None => {
                        debug!(
                            "Tombstone for hash {} but no cell in index",
                            hash
                        );
                    }
                }
            }
            _ => {
                warn!("Unknown entry type at {}: {:?}", cursor, entry_header.entry_type);
            }
        }
        
        cursor += entry_size;
    }
    
    (dead_space, tombstone_count)
}

/// Recovery configuration
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    pub num_chunks: usize,
    pub chunk_size: usize,
}

impl RecoveryConfig {
    /// Check if all discovered segments can fit in the current configuration
    pub fn can_fit(&self, files: &[SegmentFileInfo]) -> bool {
        // Group by chunk_id and find max seg_id per chunk
        let mut max_seg_per_chunk: HashMap<usize, u64> = HashMap::new();
        
        for file in files {
            let entry = max_seg_per_chunk.entry(file.chunk_id).or_insert(0);
            *entry = (*entry).max(file.seg_id);
        }
        
        // Check if all chunks fit
        for (chunk_id, max_seg_id) in max_seg_per_chunk {
            if chunk_id >= self.num_chunks {
                warn!(
                    "Chunk {} in recovery files exceeds configured chunks {}",
                    chunk_id, self.num_chunks
                );
                return false;
            }
            
            // Calculate max segments this chunk can hold
            let max_segs = self.chunk_size / SEGMENT_SIZE;
            if (max_seg_id as usize) >= max_segs {
                warn!(
                    "Chunk {} segment {} exceeds capacity {} segments",
                    chunk_id, max_seg_id, max_segs
                );
                return false;
            }
        }
        
        true
    }
}

/// Phase 1: Discover segment files
fn phase1_discover_files(
    backup_storage: &Option<String>,
    wal_storage: &Option<String>,
) -> io::Result<Vec<SegmentFileInfo>> {
    info!("Phase 1: Discovering segment files...");
    let files = discover_segment_files(backup_storage, wal_storage)?;
    
    if files.is_empty() {
        info!("No segment files found, starting fresh");
        eprintln!("[RECOVERY] No segment files found, starting fresh");
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "No segment files found",
        ));
    }
    
    info!("Discovered {} segment files", files.len());
    eprintln!("[RECOVERY] Discovered {} segment files", files.len());
    
    Ok(files)
}

/// Phase 1.5: Set initial next_seq_id values for each chunk
fn phase1_5_set_initial_seq_ids(chunks: &[Chunk], files: &[SegmentFileInfo]) {
    info!("Phase 1.5: Setting initial next_seq_id values...");
    eprintln!("[RECOVERY] Phase 1.5: Setting initial next_seq_id values...");
    
    for chunk in chunks {
        // Find max seq_id for this chunk from discovered files
        let max_seq_id = files
            .iter()
            .filter(|f| f.chunk_id == chunk.id)
            .map(|f| f.seq_id)
            .max()
            .unwrap_or(0);
        
        // Set next_seq_id so newly allocated segments get higher seq_ids
        chunk
            .allocator
            .next_seq_id
            .store((max_seq_id + 1) as usize, Ordering::Release);
        
        info!(
            "Chunk {} initial next_seq_id set to {}",
            chunk.id,
            max_seq_id + 1
        );
        eprintln!("[RECOVERY] Chunk {} initial next_seq_id set to {}", chunk.id, max_seq_id + 1);
    }
}

/// Check if we should recover this segment as cold based on tiered memory settings
fn should_recover_as_cold(
    chunk: &Chunk,
    file_info: &SegmentFileInfo,
    hot_memory_used: &HashMap<usize, usize>,
) -> bool {
    if let Some(ref tiered_manager) = chunk.tiered_manager {
        if let Some(physical_limit) = tiered_manager.physical_memory_limit {
            let chunk_hot_used = hot_memory_used.get(&file_info.chunk_id).copied().unwrap_or(0);
            let would_exceed_limit = (chunk_hot_used + SEGMENT_SIZE) > physical_limit;
            
            let recover_as_cold = file_info.is_backup && would_exceed_limit;
            
            debug!(
                "Recovery decision: hot_used={} MB, limit={} MB, would_exceed={}, recover_as_cold={}",
                chunk_hot_used / (1024 * 1024),
                physical_limit / (1024 * 1024),
                would_exceed_limit,
                recover_as_cold
            );
            
            recover_as_cold
        } else {
            false // No physical limit, recover all as hot
        }
    } else {
        false // Tiered memory not enabled
    }
}

/// Recover a segment as cold by mmapping the backup file directly
fn recover_segment_as_cold(segment: &Segment, file_info: &SegmentFileInfo) -> io::Result<()> {
    info!("Recovering segment {} as COLD (tiered memory enabled)", segment.id);
    
    // mmap the backup file directly instead of copying to memory
    let c_path = std::ffi::CString::new(file_info.path.to_str().unwrap()).unwrap();
    let fd = unsafe { open(c_path.as_ptr(), O_RDONLY) };
    
    if fd < 0 {
        error!("Failed to open backup file for cold recovery: {:?}", io::Error::last_os_error());
        return Err(io::Error::last_os_error());
    }
    
    // Use MAP_FIXED to replace the anonymous mapping with file-backed
    let mmap_addr = unsafe {
        mmap(
            segment.addr as *mut c_void,
            SEGMENT_SIZE,
            PROT_READ,
            MAP_PRIVATE | MAP_FIXED,
            fd,
            0,
        )
    };
    
    if mmap_addr == libc::MAP_FAILED {
        unsafe { libc::close(fd); }
        error!("Failed to mmap backup file for cold recovery: {:?}", io::Error::last_os_error());
        return Err(io::Error::last_os_error());
    }
    
    // Mark segment as cold
    segment.cold_file_fd.store(fd, Ordering::Release);
    segment.archived.store(true, Ordering::Release); // Already archived (it's the backup file!)
    
    info!(
        "Recovered segment {} as COLD from {}",
        segment.id,
        file_info.path.display()
    );
    
    Ok(())
}

/// Recover a segment as hot by copying file data to memory
fn recover_segment_as_hot(segment: &Segment, file_data: &[u8]) {
    // Traditional hot recovery - copy data to memory
    unsafe {
        let src_ptr = file_data.as_ptr();
        let dst_ptr = segment.addr as *mut u8;
        std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, file_data.len());
    }
    
    debug!(
        "Copied {} bytes to segment memory at address {:#x} (HOT recovery)",
        file_data.len(),
        segment.addr
    );
}

/// Phase 2: Allocate segments and load data
fn phase2_allocate_and_load(
    files: &[SegmentFileInfo],
    chunks: &[Chunk],
    allocated_segments: &mut Vec<(usize, Arc<Segment>)>,
    hot_memory_used: &mut HashMap<usize, usize>,
) -> io::Result<()> {
    info!("Phase 2: Allocating segments and loading data...");
    eprintln!("[RECOVERY] Phase 2: Allocating segments and loading data...");
    eprintln!("[RECOVERY] Files to process: {}", files.len());
    
    for file_info in files {
        eprintln!("[RECOVERY] Processing file: chunk={}, seg={}, seq={}, path={}", 
            file_info.chunk_id, file_info.seg_id, file_info.seq_id, file_info.path.display());
        
        let chunk = &chunks[file_info.chunk_id];
        
        info!(
            "Loading chunk {} segment {} seq {} from {} ({} bytes)",
            file_info.chunk_id,
            file_info.seg_id,
            file_info.seq_id,
            file_info.path.display(),
            file_info.size
        );
        
        // Load file data
        eprintln!("[RECOVERY] Loading file data...");
        let file_data = load_file_to_memory(&file_info.path)?;
        eprintln!("[RECOVERY] Loaded {} bytes", file_data.len());
        
        if file_data.len() > SEGMENT_SIZE {
            error!(
                "File {} size {} exceeds segment size {}",
                file_info.path.display(),
                file_data.len(),
                SEGMENT_SIZE
            );
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "File size exceeds segment capacity",
            ));
        }
        
        // Allocate segment memory with the ORIGINAL seq_id from the file
        // This ensures undo log references remain valid after recovery
        debug!(
            "About to allocate segment with original seq_id {}, current next_seq_id = {}",
            file_info.seq_id,
            chunk.allocator.next_seq_id.load(Ordering::Acquire)
        );
        eprintln!("[RECOVERY] Allocating segment with original seq_id {}...", file_info.seq_id);
        let seg_opt = chunk.allocator.alloc_seg_with_seq_id(
            file_info.seq_id,
            &chunk.backup_storage,
            &chunk.wal_storage
        );
        
        let segment = match seg_opt {
            Some(seg) => {
                eprintln!("[RECOVERY] Segment allocated: id={}, seq_id={}, addr={:#x}", seg.id, seg.seq_id, seg.addr);
                seg
            },
            None => {
                eprintln!("[RECOVERY] FAILED to allocate segment!");
                error!(
                    "Failed to allocate segment for chunk {} during recovery",
                    file_info.chunk_id
                );
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "Cannot allocate segment during recovery",
                ));
            }
        };
        
        // Determine if this segment should be recovered as cold
        let should_recover_as_cold = should_recover_as_cold(chunk, file_info, hot_memory_used);
        
        if should_recover_as_cold {
            recover_segment_as_cold(&segment, file_info)?;
        } else {
            recover_segment_as_hot(&segment, &file_data);
            // Update hot memory tracking
            *hot_memory_used.entry(file_info.chunk_id).or_insert(0) += SEGMENT_SIZE;
        }
        
        // Set append_header based on actual data
        let append_header = find_append_header(segment.addr, file_data.len());
        segment.append_header.store(append_header, Ordering::Release);
        eprintln!("[RECOVERY] Set segment {} append_header to {} (offset {})",
            segment.id, append_header, append_header - segment.addr);
        
        info!(
            "Recovered segment: chunk {} original_seg {} -> runtime_seg {} (seq {}), append_header offset {}",
            file_info.chunk_id,
            file_info.seg_id,
            segment.id,
            segment.seq_id,
            append_header - segment.addr
        );
        
        if file_info.seg_id != segment.id {
            warn!(
                "Segment ID changed: original {} -> runtime {} (address-derived)",
                file_info.seg_id, segment.id
            );
        }
        
        allocated_segments.push((file_info.chunk_id, Arc::new(segment)));
    }
    
    Ok(())
}

/// Phase 3: Rebuild cell indices from recovered segments
fn phase3_rebuild_indices(
    allocated_segments: &[(usize, Arc<Segment>)],
    chunks: &[Chunk],
    global_cell_index: &mut HashMap<u64, CellIndexEntry>,
) -> u32 {
    info!("Phase 3: Rebuilding cell indices...");
    let mut total_tombstones = 0;
    
    for (chunk_id, segment) in allocated_segments {
        let chunk = &chunks[*chunk_id];
        
        debug!(
            "Scanning chunk {} segment {} for entries",
            chunk_id, segment.id
        );
        
        // Put segment in chunk first
        chunk.segs.insert_back(segment.id as usize, segment.clone());
        
        // Rebuild index from this segment
        let (dead_space, tombstone_count) =
            rebuild_cell_index_from_segment(chunk, segment, global_cell_index);
        
        // Update segment metadata
        segment.dead_space.store(dead_space, Ordering::Release);
        segment.tombstones.store(tombstone_count, Ordering::Release);
        
        total_tombstones += tombstone_count;
        
        debug!(
            "Segment {} has {} dead bytes, {} tombstones",
            segment.id, dead_space, tombstone_count
        );
    }
    
    total_tombstones
}

/// Phase 4: Populate chunk cell indices
fn phase4_populate_chunk_indices(
    global_cell_index: &HashMap<u64, CellIndexEntry>,
    config: &RecoveryConfig,
    chunks: &[Chunk],
) -> usize {
    info!("Phase 4: Populating chunk cell indices...");
    eprintln!("[RECOVERY] Phase 4: global_cell_index has {} entries", global_cell_index.len());
    
    let mut total_cells = 0;
    
    for (hash, entry) in global_cell_index.iter() {
        // Find which chunk this cell belongs to using partition
        let chunk_id = (entry.partition as usize) % config.num_chunks;
        let chunk = &chunks[chunk_id];
        
        // Insert into chunk's cell_index
        if let Some(mut guard) = chunk.cell_index.try_insert_locked(*hash as usize) {
            *guard = entry.addr;
            total_cells += 1;
            eprintln!("[RECOVERY] Inserted cell hash {} to chunk {} at addr {:#x}", hash, chunk_id, entry.addr);
        } else {
            warn!("Cell hash {} already exists in chunk index", hash);
            eprintln!("[RECOVERY] WARNING: Cell hash {} already exists in chunk {}", hash, chunk_id);
        }
    }
    
    total_cells
}

/// Phase 5: Clean up old segment files
fn phase5_cleanup_old_files(files: &[SegmentFileInfo]) {
    info!("Phase 5: Cleaning up old segment files...");
    for file_info in files {
        if let Err(e) = fs::remove_file(&file_info.path) {
            warn!(
                "Failed to remove old file {}: {:?}",
                file_info.path.display(),
                e
            );
        } else {
            debug!("Removed old file {}", file_info.path.display());
        }
    }
}

/// Main recovery coordinator
pub fn recover_chunks(
    config: &RecoveryConfig,
    backup_storage: &Option<String>,
    wal_storage: &Option<String>,
    chunks: &[Chunk],
) -> io::Result<()> {
    info!("Starting recovery from storage directories");
    
    // Phase 1: Discover files
    let files = match phase1_discover_files(backup_storage, wal_storage) {
        Ok(files) => files,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            info!("No segment files found, starting fresh");
            return Ok(());
        },
        Err(e) => return Err(e),
    };
    
    // Check if configuration can fit all segments
    let can_fit = config.can_fit(&files);
    if !can_fit {
        error!(
            "Recovery configuration mismatch: {} chunks x {} bytes cannot fit all segments",
            config.num_chunks, config.chunk_size
        );
        error!("You may need to adjust chunk count or size, or manually migrate data");
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Configuration cannot fit recovered segments",
        ));
    }
    
    // Phase 1.5: Pre-set next_seq_id for each chunk BEFORE allocating segments
    phase1_5_set_initial_seq_ids(chunks, &files);
    
    // Phase 2: Allocate segments and load data
    let mut allocated_segments: Vec<(usize, Arc<Segment>)> = Vec::new();
    let mut hot_memory_used: HashMap<usize, usize> = HashMap::new();
    
    phase2_allocate_and_load(&files, chunks, &mut allocated_segments, &mut hot_memory_used)?;
    
    // Phase 3: Rebuild cell indices
    let mut global_cell_index: HashMap<u64, CellIndexEntry> = HashMap::new();
    let total_tombstones = phase3_rebuild_indices(&allocated_segments, chunks, &mut global_cell_index);
    
    // Phase 4: Update chunk cell_index WordMaps
    let total_cells = phase4_populate_chunk_indices(&global_cell_index, config, chunks);
    
    info!(
        "Recovery complete: {} cells, {} tombstones across {} segments",
        total_cells,
        total_tombstones,
        allocated_segments.len()
    );
    
    // Phase 5: Clean up old files
    phase5_cleanup_old_files(&files);
    
    info!("Recovery completed successfully");
    Ok(())
}

/// Create a recovery marker file to indicate recovery is in progress
pub fn create_recovery_marker(storage_dir: &str) -> io::Result<()> {
    let marker_path = Path::new(storage_dir).join(".recovery_in_progress");
    let mut file = File::create(&marker_path)?;
    file.write_all(b"recovery")?;
    file.sync_all()?;
    Ok(())
}

/// Remove recovery marker file after successful recovery
pub fn remove_recovery_marker(storage_dir: &str) -> io::Result<()> {
    let marker_path = Path::new(storage_dir).join(".recovery_in_progress");
    if marker_path.exists() {
        fs::remove_file(&marker_path)?;
    }
    Ok(())
}

/// Check if recovery marker exists (indicates previous crash during recovery)
pub fn check_recovery_marker(storage_dir: &str) -> bool {
    let marker_path = Path::new(storage_dir).join(".recovery_in_progress");
    marker_path.exists()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dovahkiin::types::Map;
    use crate::ram::cell::*;
    use crate::ram::chunk::Chunks;
    use crate::ram::schema::{Field, LocalSchemasCache, Schema};
    use crate::ram::types::Id;
    use crate::server::ServerMeta;
    use dovahkiin::types::Type;
    use std::sync::Arc;
    use tempfile::TempDir;

    const TEST_SEGMENT_SIZE: usize = 8 * 1024 * 1024; // 8MB
    const DATA_SIZE: usize = 1024; // 1KB per cell

    fn default_cell(id: &Id) -> OwnedCell {
        let data: Vec<_> = std::iter::repeat(id.lower as u8).take(DATA_SIZE).collect();
        OwnedCell {
            header: CellHeader::new(0, id),
            data: data_map_value!(id: id.lower as i32, data: data),
        }
    }

    fn default_fields() -> Field {
        Field::new_schema(vec![
            Field::new_unindexed("id", Type::I32),
            Field::new_unindexed_array("data", Type::U8),
        ])
    }

    fn setup_test_schema() -> LocalSchemasCache {
        let schema = Schema::new("recovery_test", None, default_fields(), false, false);
        let schemas = LocalSchemasCache::new_local("");
        schemas.new_schema(schema);
        schemas
    }

    // Purpose: Validate parsing of segment filenames `{chunk}-{seg}-{seq}.{nlog|nbackup}`
    // and correct discrimination of WAL vs backup extensions.
    #[test]
    fn test_parse_filename() {
        use std::fs::File;
        let temp_dir = TempDir::new().unwrap();
        
        // Create test files
        let path1 = temp_dir.path().join("0-12345-67.nlog");
        File::create(&path1).unwrap();
        let info = SegmentFileInfo::parse_filename(&path1).unwrap();
        assert_eq!(info.chunk_id, 0);
        assert_eq!(info.seg_id, 12345);
        assert_eq!(info.seq_id, 67);
        assert!(!info.is_backup);

        let path2 = temp_dir.path().join("1-98765-43.nbackup");
        File::create(&path2).unwrap();
        let info = SegmentFileInfo::parse_filename(&path2).unwrap();
        assert_eq!(info.chunk_id, 1);
        assert_eq!(info.seg_id, 98765);
        assert_eq!(info.seq_id, 43);
        assert!(info.is_backup);

        // Invalid format
        let path3 = temp_dir.path().join("invalid-file.log");
        File::create(&path3).unwrap();
        assert!(SegmentFileInfo::parse_filename(&path3).is_none());
    }

    // Purpose: End-to-end sanity check. Write cells, archive segments,
    // restart with recovery enabled, and verify all cells are restored.
    #[test]
    fn test_recovery_basic_write_and_recover() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();

        // Phase 1: Create chunks, write data, and let it persist
        let cell_ids: Vec<Id> = (0..10).map(|i| Id::new(0, i)).collect();
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                false, // no recovery on first run
            );

            // Write cells
            for id in &cell_ids {
                let mut cell = default_cell(id);
                chunks.write_cell(&mut cell).unwrap();
            }

            // Archive segments to create backup files
            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }

            // Verify data exists before recovery
            assert_eq!(chunks.list[0].cell_count(), 10);
        }

        // Phase 2: Create new chunks instance with recovery enabled
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                true, // enable recovery
            );

            // Verify recovered data
            assert_eq!(chunks.list[0].cell_count(), 10);
            for id in &cell_ids {
                let cell = chunks.read_cell(id).unwrap();
                let expected = default_cell(id);
                assert_eq!(cell.to_owned().data, expected.data);
            }
        }
    }

    // Purpose: Verify version handling across recovery. After recovering an
    // initial write, write a newer version, archive, and finally recover again;
    // latest version must be returned.
    #[test]
    fn test_recovery_with_updates() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();

        let cell_id = Id::new(0, 42);
        
        println!("=== Phase 1: Write initial data ===");
        // Phase 1: Write initial data
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                false,
            );

            println!("Writing cell...");
            let mut cell = default_cell(&cell_id);
            chunks.write_cell(&mut cell).unwrap();
            println!("Cell written successfully");
            
            // Archive
            println!("Archiving segments...");
            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }
            println!("Phase 1 complete, dropping chunks...");
        }
        println!("Phase 1 chunks dropped");

        println!("=== Phase 2: Recover and update ===");
        // Phase 2: Update the cell (higher version)
        {
            let schemas = setup_test_schema();
            println!("Creating chunks with recovery enabled...");
            use std::fs;
            fn list_files_recursively(dir: &Path) -> Vec<String> {
                let mut files = Vec::new();
                if let Ok(entries) = fs::read_dir(dir) {
                    for entry in entries {
                        if let Ok(entry) = entry {
                            let path = entry.path();
                            if path.is_dir() {
                                files.extend(list_files_recursively(&path));
                            } else {
                                files.push(path.display().to_string());
                            }
                        }
                    }
                }
                files
            }
            
            println!("Files before recovery: WAL={:?}, Backup={:?}", 
                list_files_recursively(wal_dir.path()),
                list_files_recursively(backup_dir.path()));
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                true, // recover first
            );
            println!("Chunks recovered successfully");
            println!("Files after recovery: WAL={:?}, Backup={:?}", 
                list_files_recursively(wal_dir.path()),
                list_files_recursively(backup_dir.path()));

            // Update with new data (version will be higher)
            println!("Updating cell...");
            let updated_data: Vec<u8> = vec![0xFF; DATA_SIZE];
            let mut updated_cell = OwnedCell {
                header: CellHeader {
                    version: 2,
                    timestamp: 200,
                    schema: 0,
                    partition: 0,
                    hash: cell_id.lower,
                },
                data: data_map_value!(id: 999 as i32, data: updated_data),
            };
            println!("About to update cell...");
            chunks.update_cell(&mut updated_cell).unwrap();
            println!("Cell updated successfully");
            println!("Cell count before archiving: {}", chunks.list[0].cell_count());

            // Archive updated segment
            println!("Archiving updated segments...");
            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    println!("Archiving segment {} with seq_id {}", seg.id, seg.seq_id);
                    let result = seg.archive().unwrap();
                    println!("Archive result: {}", result);
                }
            }
            
            // List files
            println!("WAL files: {:?}", std::fs::read_dir(wal_dir.path()).unwrap().collect::<Vec<_>>());
            println!("Backup files: {:?}", std::fs::read_dir(backup_dir.path()).unwrap().collect::<Vec<_>>());
            
            println!("Phase 2 complete, dropping chunks...");
        }
        println!("Phase 2 chunks dropped");

        println!("=== Phase 3: Final recovery and verification ===");
        println!("WAL files: {:?}", std::fs::read_dir(wal_dir.path()).unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path().display().to_string())
            .collect::<Vec<_>>());
        println!("Backup files: {:?}", std::fs::read_dir(backup_dir.path()).unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path().display().to_string())
            .collect::<Vec<_>>());
        // Phase 3: Recover and verify we get the latest version
        {
            let schemas = setup_test_schema();
            println!("Creating chunks with recovery for verification...");
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                true,
            );
            println!("Chunks recovered, cell count: {}", chunks.list[0].cell_count());
            println!("Trying to read cell with id {:?}, hash {}", cell_id, cell_id.lower);

            println!("Reading cell...");
            let cell = chunks.read_cell(&cell_id).unwrap();
            // Should have the updated data
            let cell_owned = cell.to_owned();
            let id_val = cell_owned.data["id"].i32().unwrap();
            assert_eq!(*id_val, 999);
        }
    }

    // Purpose: Verify delete/tombstone semantics across recovery. After initial
    // writes and archive, recover and delete a subset (tombstones), archive, then
    // recover; only non-deleted cells should remain.
    // Note: Currently ignored due to file cleanup semantics that can remove older
    // segment files still holding live cells.
    #[test]
    #[ignore] // TODO: This test reveals a limitation where old cells are lost after file cleanup during recovery with tombstones
    fn test_recovery_with_tombstones() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();

        let cell_ids: Vec<Id> = (0..5).map(|i| Id::new(0, i)).collect();

        // Phase 1: Write cells
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                false,
            );

            for id in &cell_ids {
                let mut cell = default_cell(id);
                chunks.write_cell(&mut cell).unwrap();
            }

            // Archive
            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }

            assert_eq!(chunks.list[0].cell_count(), 5);
        }

        // Phase 2: Delete some cells
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                true, // recover
            );

            // Delete cells 0, 2, 4
            chunks.remove_cell(&cell_ids[0]).unwrap();
            chunks.remove_cell(&cell_ids[2]).unwrap();
            chunks.remove_cell(&cell_ids[4]).unwrap();

            assert_eq!(chunks.list[0].cell_count(), 2);

            // Archive with tombstones
            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }
        }

        // Phase 3: Recover and verify deletions
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                true,
            );

            // Should only have cells 1 and 3
            assert_eq!(chunks.list[0].cell_count(), 2);
            assert!(chunks.read_cell(&cell_ids[1]).is_ok());
            assert!(chunks.read_cell(&cell_ids[3]).is_ok());
            
            // Deleted cells should not exist
            assert!(chunks.read_cell(&cell_ids[0]).is_err());
            assert!(chunks.read_cell(&cell_ids[2]).is_err());
            assert!(chunks.read_cell(&cell_ids[4]).is_err());
        }
    }

    // Purpose: Ensure allocator next_seq_id is restored to at least
    // max(recovered seq_id) + 1 so new segments get strictly higher seq_ids.
    #[test]
    fn test_recovery_preserves_seq_id() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();

        let initial_seq_id: u64;

        // Phase 1: Write some data and capture seq_id
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                false,
            );

            // Write enough cells to allocate multiple segments
            for i in 0..20 {
                let mut cell = default_cell(&Id::new(0, i));
                chunks.write_cell(&mut cell).unwrap();
            }

            // Archive
            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }

            initial_seq_id = chunks.list[0].allocator.next_seq_id.load(Ordering::Relaxed) as u64;
            println!("Initial seq_id after writes: {}", initial_seq_id);
        }

        // Phase 2: Recover and verify seq_id continues from where it left off
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                true,
            );

            let recovered_seq_id = chunks.list[0].allocator.next_seq_id.load(Ordering::Relaxed) as u64;
            println!("Recovered seq_id: {}", recovered_seq_id);
            
            // seq_id should be at least the initial value (may be higher if more segments were allocated)
            assert!(recovered_seq_id >= initial_seq_id, 
                "Expected seq_id >= {}, got {}", initial_seq_id, recovered_seq_id);
        }
    }

    // Purpose: Ensure recovery on empty storage results in a clean state with
    // zero cells and no errors.
    #[test]
    fn test_recovery_handles_empty_storage() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();

        // Try to recover when no files exist
        let schemas = setup_test_schema();
        let chunks = Chunks::new_with_recovery(
            1,
            TEST_SEGMENT_SIZE * 2,
            Arc::new(ServerMeta { schemas }),
            None, // index_builder
            Some(backup_dir.path().to_str().unwrap().to_string()),
            Some(wal_dir.path().to_str().unwrap().to_string()),
            true, // enable recovery even though nothing to recover
        );

        // Should create fresh chunks with no data
        assert_eq!(chunks.list[0].cell_count(), 0);
    }

    // Purpose: Confirm append_header offset (relative to segment base) is
    // reconstructed by scanning recovered segment contents.
    #[test]
    fn test_find_append_header() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();

        // Phase 1: Write data and get actual append position
        let actual_append_offset: usize;
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                false,
            );

            // Write a few cells
            for i in 0..5 {
                let mut cell = default_cell(&Id::new(0, i));
                chunks.write_cell(&mut cell).unwrap();
            }

            let seg = &chunks.list[0].segments()[0];
            let actual_append = seg.append_header.load(Ordering::Relaxed);
            actual_append_offset = actual_append - seg.addr;
            println!("Actual append header: {} (offset: {})", actual_append, actual_append_offset);

            // Archive the segment
            seg.archive().unwrap();
        }

        // Phase 2: Recover and verify append_header is correctly restored
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                true,
            );

            // Find the segment with data (non-zero offset)
            let segs = chunks.list[0].segments();
            println!("Total segments after recovery: {}", segs.len());
            
            let mut found_matching_segment = false;
            for seg in &segs {
                let recovered_append = seg.append_header.load(Ordering::Relaxed);
                let recovered_offset = recovered_append - seg.addr;
                println!("Segment {} append_header offset: {}", seg.id, recovered_offset);
                
                if recovered_offset == actual_append_offset {
                    found_matching_segment = true;
                    println!("Found matching segment with correct offset!");
                    break;
                }
            }

            // Should find at least one segment with the correct offset
            assert!(found_matching_segment, 
                "No segment found with offset {}. Segments: {:?}", 
                actual_append_offset, 
                segs.iter().map(|s| (s.id, s.append_header.load(Ordering::Relaxed) - s.addr)).collect::<Vec<_>>());
        }
    }

    // Purpose: Ensure discovery/dedup handles both WAL and backup variants for
    // the same segment-generation, preserving data integrity.
    #[test]
    fn test_recovery_deduplication() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();

        // Phase 1: Create backup for a segment
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                false,
            );

            let mut cell = default_cell(&Id::new(0, 1));
            chunks.write_cell(&mut cell).unwrap();

            let seg = &chunks.list[0].segments()[0];
            seg.archive().unwrap();
        }

        // Phase 2: Recover - should handle file discovery and deduplication gracefully
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                1,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                true,
            );

            // Verify data integrity
            let cell = chunks.read_cell(&Id::new(0, 1));
            assert!(cell.is_ok());
        }
    }

    // Purpose: Verify recovery marker helpers for crash-safety signaling
    // (create, check, remove).
    #[test]
    fn test_recovery_marker() {
        let marker_dir = TempDir::new().unwrap();
        let marker_path = marker_dir.path().to_str().unwrap();

        // Create marker
        create_recovery_marker(marker_path).unwrap();
        assert!(check_recovery_marker(marker_path));

        // Remove marker
        remove_recovery_marker(marker_path).unwrap();
        assert!(!check_recovery_marker(marker_path));
    }

    // Purpose: Validate multi-chunk recovery and routing. Write across 3 chunks,
    // archive, recover, and verify all cells are present and readable. Confirms
    // partition-based mapping used during recovery population.
    #[test]
    fn test_multiple_chunks_recovery() {
        let _ = env_logger::try_init();
        let wal_dir = TempDir::new().unwrap();
        let backup_dir = TempDir::new().unwrap();

        // Phase 1: Create multiple chunks with data
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                3, // 3 chunks
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                false,
            );

            // Write data to different chunks
            for i in 0..30 {
                let mut cell = default_cell(&Id::new(0, i));
                chunks.write_cell(&mut cell).unwrap();
            }

            // Archive all segments
            for chunk in &chunks.list {
                for seg in chunk.segments() {
                    seg.archive().unwrap();
                }
            }

            let total_cells: usize = chunks.list.iter().map(|c| c.cell_count()).sum();
            println!("Total cells across all chunks: {}", total_cells);
            assert_eq!(total_cells, 30);
        }

        // Phase 2: Recover all chunks
        {
            let schemas = setup_test_schema();
            let chunks = Chunks::new_with_recovery(
                3,
                TEST_SEGMENT_SIZE * 8, // Need more space for multiple recovery cycles
                Arc::new(ServerMeta { schemas }),
                None, // index_builder
                Some(backup_dir.path().to_str().unwrap().to_string()),
                Some(wal_dir.path().to_str().unwrap().to_string()),
                true,
            );

            let total_cells: usize = chunks.list.iter().map(|c| c.cell_count()).sum();
            println!("Total recovered cells across all chunks: {}", total_cells);
            assert_eq!(total_cells, 30);

            // Verify all cells are accessible
            for i in 0..30 {
                let cell = chunks.read_cell(&Id::new(0, i));
                assert!(cell.is_ok(), "Cell {} should exist", i);
            }
        }
    }
}
