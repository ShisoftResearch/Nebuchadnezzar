pub mod clock;
pub mod eviction;
pub mod promotion;
pub mod manager;
pub mod page_fault_tracker;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod bench;

/// Configuration for tiered memory management
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TieredConfig {
    /// Memory usage threshold (0.0-1.0) to trigger eviction
    pub threshold: f32,
    /// Physical memory limit in bytes for hot segments
    pub physical_memory_limit: usize,
}

impl TieredConfig {
    /// Get total system memory in bytes
    fn get_system_memory() -> usize {
        #[cfg(target_os = "linux")]
        {
            // Read from /proc/meminfo
            if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
                for line in meminfo.lines() {
                    if line.starts_with("MemTotal:") {
                        if let Some(kb_str) = line.split_whitespace().nth(1) {
                            if let Ok(kb) = kb_str.parse::<usize>() {
                                return kb * 1024; // Convert KB to bytes
                            }
                        }
                    }
                }
            }
        }
        
        #[cfg(target_os = "macos")]
        {
            use std::process::Command;
            if let Ok(output) = Command::new("sysctl")
                .arg("-n")
                .arg("hw.memsize")
                .output()
            {
                if let Ok(mem_str) = String::from_utf8(output.stdout) {
                    if let Ok(mem) = mem_str.trim().parse::<usize>() {
                        return mem;
                    }
                }
            }
        }
        
        // Default fallback: 16GB if we can't detect
        log::warn!("Could not detect system memory, defaulting to 16GB");
        16 * 1024 * 1024 * 1024
    }

    /// Create a new tiered config with default threshold of 0.8 and system memory as limit
    pub fn new() -> Self {
        Self {
            threshold: 0.8,
            physical_memory_limit: Self::get_system_memory(),
        }
    }

    /// Create a new tiered config with explicit memory limit
    pub fn with_memory_limit(physical_memory_limit: usize) -> Self {
        Self {
            threshold: 0.8,
            physical_memory_limit,
        }
    }

    /// Create a new tiered config with custom threshold and explicit memory limit
    pub fn with_threshold(threshold: f32, physical_memory_limit: usize) -> Self {
        Self {
            threshold,
            physical_memory_limit,
        }
    }

    /// Read tiered config from environment variables
    pub fn from_env() -> Option<Self> {
        let enabled = std::env::var("NEB_TIERED_MEMORY_ENABLED")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false);
        
        if !enabled {
            return None;
        }

        let threshold = std::env::var("NEB_TIERED_MEMORY_THRESHOLD")
            .ok()
            .and_then(|v| v.parse::<f32>().ok())
            .unwrap_or(0.8);

        let physical_memory_limit = std::env::var("NEB_TIERED_PHYSICAL_MEMORY_LIMIT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or_else(Self::get_system_memory);

        Some(Self {
            threshold,
            physical_memory_limit,
        })
    }
}

impl Default for TieredConfig {
    fn default() -> Self {
        Self::new()
    }
}

