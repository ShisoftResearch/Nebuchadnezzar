use crate::ram::chunk::Chunk;
use crate::ram::segs::SEGMENT_SIZE;
use crate::ram::tiered::clock::ClockEvictionPolicy;
use crate::ram::tiered::eviction::evict_segment;
use crate::ram::tiered::promotion::promote_segment;
use std::io;
use std::sync::atomic::AtomicBool;

/// Manages tiered memory for a chunk
/// 
/// Coordinates eviction of hot segments to cold storage and promotion of cold segments
/// back to hot storage based on access patterns and memory pressure.
pub struct TieredMemoryManager {
    /// Physical memory limit in bytes (hot segments cannot exceed this)
    /// When hot segment memory usage exceeds this limit, eviction is triggered
    pub physical_memory_limit: Option<usize>,
    
    /// Eviction threshold as percentage of physical memory limit (0.0 to 1.0)
    /// Default: 0.8 (80%)
    eviction_threshold_percent: f32,
    
    /// CLOCK eviction policy for victim selection
    clock_policy: ClockEvictionPolicy,
    
    /// Whether tiered memory is enabled
    enabled: bool,
    
    /// Whether to disable promotion (for benchmarking cold reads)
    /// When true, cold segments remain cold even when accessed
    pub disable_promotion: AtomicBool,
}

impl TieredMemoryManager {
    /// Create a new tiered memory manager
    /// 
    /// # Arguments
    /// * `physical_memory_limit` - Physical memory limit in bytes (None = use threshold % of capacity)
    /// * `eviction_threshold_percent` - Percentage (0.0-1.0) of limit before eviction
    pub fn new(physical_memory_limit: Option<usize>, eviction_threshold_percent: f32) -> Self {
        TieredMemoryManager {
            physical_memory_limit,
            eviction_threshold_percent: eviction_threshold_percent.clamp(0.0, 1.0),
            clock_policy: ClockEvictionPolicy::new(),
            enabled: true,
            disable_promotion: AtomicBool::new(false),
        }
    }
    
    /// Create with default threshold (80%) and no explicit memory limit
    pub fn with_defaults() -> Self {
        Self::new(None, 0.8)
    }
    
    /// Create with explicit physical memory limit
    pub fn with_memory_limit(physical_memory_limit_bytes: usize, threshold: f32) -> Self {
        Self::new(Some(physical_memory_limit_bytes), threshold)
    }
    
    /// Check if eviction is needed and evict segments if necessary
    /// 
    /// Uses physical_memory_limit if set, otherwise uses chunk capacity
    /// 
    /// Returns the number of segments evicted
    pub fn check_and_evict(&self, chunk: &Chunk) -> Result<usize, io::Error> {
        if !self.enabled {
            return Ok(0);
        }
        
        let hot_segments_count = self.count_hot_segments(chunk);
        let hot_memory_bytes = hot_segments_count * SEGMENT_SIZE;
        
        // Calculate memory limit: use physical_memory_limit if set, else use chunk capacity
        let memory_limit = self.physical_memory_limit.unwrap_or(chunk.capacity);
        let threshold_bytes = (memory_limit as f32 * self.eviction_threshold_percent) as usize;
        
        if hot_memory_bytes > threshold_bytes {
            // Need to evict - target 70% of threshold to avoid thrashing
            let target_bytes = (threshold_bytes as f32 * 0.7) as usize;
            let target_segments = target_bytes / SEGMENT_SIZE;
            let num_to_evict = hot_segments_count.saturating_sub(target_segments);
            
            info!(
                "Memory pressure detected: {} hot segments ({} MB), limit: {} MB, threshold: {} MB, evicting {} segments",
                hot_segments_count, 
                hot_memory_bytes / (1024 * 1024),
                memory_limit / (1024 * 1024),
                threshold_bytes / (1024 * 1024),
                num_to_evict
            );
            
            self.evict_until_target(chunk, num_to_evict)
        } else {
            Ok(0)
        }
    }
    
    /// Evict segments until we've evicted the target number
    /// 
    /// Returns the number of segments successfully evicted
    fn evict_until_target(&self, chunk: &Chunk, target: usize) -> Result<usize, io::Error> {
        let mut evicted_count = 0;
        
        for _ in 0..target {
            match self.clock_policy.select_victim(chunk) {
                Some(victim) => {
                    match evict_segment(&victim, chunk) {
                        Ok(()) => {
                            evicted_count += 1;
                            debug!("Evicted segment {} ({}/{})", victim.id, evicted_count, target);
                        }
                        Err(e) => {
                            warn!("Failed to evict segment {}: {}", victim.id, e);
                            // Continue trying other segments
                        }
                    }
                }
                None => {
                    // No more victims available
                    debug!(
                        "CLOCK could not find more victims, evicted {}/{} segments",
                        evicted_count, target
                    );
                    break;
                }
            }
        }
        
        if evicted_count > 0 {
            info!("Evicted {} segments to cold storage", evicted_count);
        }
        
        Ok(evicted_count)
    }
    
    /// Explicitly evict a specific number of segments
    /// 
    /// This is useful for manual memory management or testing
    /// Returns the number of segments successfully evicted
    pub fn explicit_evict(&self, chunk: &Chunk, num_segments: usize) -> Result<usize, io::Error> {
        if !self.enabled {
            return Ok(0);
        }
        
        debug!("Explicit eviction requested for {} segments", num_segments);
        self.evict_until_target(chunk, num_segments)
    }
    
    /// Promote a cold segment to hot
    /// 
    /// This is called when a cold segment is accessed and needs to be brought
    /// back into hot storage
    pub fn promote(&self, segment: &crate::ram::segs::Segment, chunk: &Chunk) -> Result<(), io::Error> {
        if !self.enabled {
            return Ok(());
        }
        
        promote_segment(segment, chunk)
    }
    
    /// Count hot segments in the chunk
    fn count_hot_segments(&self, chunk: &Chunk) -> usize {
        chunk.segments().iter().filter(|s| s.is_hot()).count()
    }
    
    /// Count cold segments in the chunk
    pub fn count_cold_segments(&self, chunk: &Chunk) -> usize {
        chunk.segments().iter().filter(|s| s.is_cold()).count()
    }
    
    /// Get memory statistics
    pub fn stats(&self, chunk: &Chunk) -> TieredMemoryStats {
        let segments = chunk.segments();
        let total = segments.len();
        let hot = segments.iter().filter(|s| s.is_hot()).count();
        let cold = segments.iter().filter(|s| s.is_cold()).count();
        
        TieredMemoryStats {
            total_segments: total,
            hot_segments: hot,
            cold_segments: cold,
            threshold: (chunk.capacity / SEGMENT_SIZE) as f32 * self.eviction_threshold_percent,
        }
    }
    
    /// Enable or disable tiered memory
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }
    
    /// Check if tiered memory is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// Statistics about tiered memory usage
#[derive(Debug, Clone)]
pub struct TieredMemoryStats {
    pub total_segments: usize,
    pub hot_segments: usize,
    pub cold_segments: usize,
    pub threshold: f32,
}

impl Default for TieredMemoryManager {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_manager_creation() {
        let manager = TieredMemoryManager::new(None, 0.8);
        assert!(manager.is_enabled());
        assert_eq!(manager.eviction_threshold_percent, 0.8);
    }
    
    #[test]
    fn test_threshold_clamping() {
        let manager = TieredMemoryManager::new(None, 1.5);
        assert_eq!(manager.eviction_threshold_percent, 1.0);
        
        let manager = TieredMemoryManager::new(None, -0.5);
        assert_eq!(manager.eviction_threshold_percent, 0.0);
    }
    
    #[test]
    fn test_manager_with_memory_limit() {
        let limit = 64 * 1024 * 1024; // 64MB
        let manager = TieredMemoryManager::with_memory_limit(limit, 0.9);
        assert!(manager.is_enabled());
        assert_eq!(manager.physical_memory_limit, Some(limit));
        assert_eq!(manager.eviction_threshold_percent, 0.9);
    }
}

