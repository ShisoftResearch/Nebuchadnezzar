// Nebuchadnezzar does not provide a vector indexer itself, it requires and external implementation.
// This module provides a trait and a default implementation that can be used to create a vector indexer.

// By default, the implementation is Morpheus, it sets the index core to its implementation,
// Nebuchadnezzar will use it to index vectors.

use std::sync::Arc;
use std::sync::OnceLock;

use dovahkiin::types::Id;
use futures::future::BoxFuture;

use crate::index::builder::IndexError;

const NO_CORE_ERROR: &str =
    "Vector indexer core is not set. Should call `set_vector_index_core` to set it.";

/// Encodings to allow metric serialization and conversion.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
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

pub trait VectorIndexerCore: Send + Sync {
    fn insert(
        &self,
        cell_id: &Id,
        schema_id: u32,
        field_id: u64,
        metric_encoding: MetricEncoding,
    ) -> BoxFuture<Result<(), IndexError>>;
    fn remove(
        &self,
        cell_id: &Id,
        schema_id: u32,
        field_id: u64,
        metric_encoding: MetricEncoding,
    ) -> BoxFuture<Result<(), IndexError>>;

    fn new_index(
        &self,
        schema_id: u32,
        field_id: u64,
    ) -> BoxFuture<Result<(), IndexError>>;
    fn delete_index(
        &self,
        schema_id: u32,
        field_id: u64,
    ) -> BoxFuture<Result<(), IndexError>>;
}

pub struct VectorIndexClient {
    core: Option<Arc<dyn VectorIndexerCore>,>
}

impl VectorIndexClient {
    pub fn new() -> Self {
        let core = get_vector_index_core();
        Self { core: core.cloned() }
    }

    pub fn insert(
        &self,
        cell_id: &Id,
        schema_id: u32,
        field_id: u64,
        metric_encoding: MetricEncoding,
    ) -> BoxFuture<Result<(), IndexError>> {
        self.core
            .as_ref()
            .expect(NO_CORE_ERROR)
            .insert(cell_id, schema_id, field_id, metric_encoding)
    }

    pub fn remove(
        &self,
        cell_id: &Id,
        schema_id: u32,
        field_id: u64,
        metric_encoding: MetricEncoding,
    ) -> BoxFuture<Result<(), IndexError>> {
        self.core
            .as_ref()
            .expect(NO_CORE_ERROR)
            .remove(cell_id, schema_id, field_id, metric_encoding)
    }
}

pub fn new_index<'a>(
    schema_id: u32,
    field_id: u64,
) -> BoxFuture<'a, Result<(), IndexError>> {
    get_vector_index_core().expect(NO_CORE_ERROR).new_index(
        schema_id,
        field_id,
    )
}

pub fn delete_index<'a>(
    schema_id: u32,
    field_id: u64,
) -> BoxFuture<'a, Result<(), IndexError>> {
    get_vector_index_core().expect(NO_CORE_ERROR).delete_index(
        schema_id,
        field_id,
    )
}
