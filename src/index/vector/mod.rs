// Nebuchadnezzar does not provide a vector indexer itself, it requires and external implementation.
// This module provides a trait and a default implementation that can be used to create a vector indexer.

// By default, the implementation is Morpheus, it sets the index core to its implementation,
// Nebuchadnezzar will use it to index vectors.

use std::sync::Arc;
use std::sync::OnceLock;

use dovahkiin::types::Id;
use futures::future::BoxFuture;

use crate::index::builder::IndexError;


pub const NO_VECTOR_CORE_ERROR: &str =
    "Vector indexer core is not set. Should call `set_vector_index_core` to set it.";

/// Encodings to allow metric serialization and conversion.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub enum MetricEncoding {
    L2,
    Cosine,
    Manhattan,
    Chebyshev,
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
    ) -> BoxFuture<Result<(), IndexError>>;

    fn new_index(&self, schema_id: u32, field_id: u64) -> BoxFuture<Result<(), IndexError>>;
    fn delete_index(&self, schema_id: u32, field_id: u64) -> BoxFuture<Result<(), IndexError>>;
}

pub struct VectorIndexClient {
    vector_core: OnceLock<Arc<dyn VectorIndexerCore>>,
}

impl VectorIndexClient {
    pub fn new() -> Self {
        Self {
            vector_core: OnceLock::new(),
        }
    }

    pub fn set_vector_index_core<C: VectorIndexerCore + 'static>(&self, core: C) -> bool {
        let res = self.vector_core.set(Arc::new(core));
        return res.is_ok();
    }

    pub fn get_vector_index_core(&self) -> &Arc<dyn VectorIndexerCore> {
        self.vector_core.get().expect(NO_VECTOR_CORE_ERROR)
    }

    pub fn is_vector_index_core_set(&self) -> bool {
        self.vector_core.get().is_some()
    }

    pub fn insert<'a>(
        &'a self,
        cell_id: &Id,
        schema_id: u32,
        field_id: u64,
        metric_encoding: MetricEncoding,
    ) -> BoxFuture<'a, Result<(), IndexError>> {
        self.get_vector_index_core().insert(
            cell_id,
            schema_id,
            field_id,
            metric_encoding,
        )
    }

    pub fn remove<'a>(
        &'a self,
        cell_id: &Id,
        schema_id: u32,
        field_id: u64,
    ) -> BoxFuture<'a, Result<(), IndexError>> {
        self.get_vector_index_core()
            .remove(cell_id, schema_id, field_id)
    }

    pub fn new_index(&self, schema_id: u32, field_id: u64) -> BoxFuture<Result<(), IndexError>> {
        self.get_vector_index_core()
            .new_index(schema_id, field_id)
    }

    pub fn delete_index(&self, schema_id: u32, field_id: u64) -> BoxFuture<Result<(), IndexError>> {
        self.get_vector_index_core()
            .delete_index(schema_id, field_id)
    }
}
