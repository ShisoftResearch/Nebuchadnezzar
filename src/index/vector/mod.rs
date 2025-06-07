use std::{cell::UnsafeCell, sync::{atomic::{AtomicUsize, Ordering}, Arc, OnceLock}};

/// Encodings to allow metric serialization and conversion.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum MetricEncoding {
    L2,
    Cosine,
    Manhattan,
    Chebyshev,
}

pub static VECTOR_INDEX_CORE: OnceLock<Arc<dyn VectorIndexerCore>> = OnceLock::new();
pub fn set_vector_index_core<C: VectorIndexerCore + 'static>(core: C) {
    VECTOR_INDEX_CORE.get_or_init(move || Arc::new(core));
}

pub fn get_vector_index_core() -> Option<&'static Arc<dyn VectorIndexerCore>> {
    VECTOR_INDEX_CORE.get()
}

pub struct VectorIndexer {
    core: Arc<dyn VectorIndexerCore>,
}

impl VectorIndexer {
    pub fn new() -> Self {
        let core = get_vector_index_core().expect("VectorIndexerCore is not set");
        Self {
            core: core.clone(),
        }
    }
}

pub trait VectorIndexerCore: Send + Sync {
    
}
