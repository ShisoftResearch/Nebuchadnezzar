// Nebuchadnezzar does not provide a vector indexer itself, it requires an external implementation.
// This module provides a trait and a default implementation that can be used to create a vector indexer.
//
// By default, the implementation is Morpheus, it sets the index core to its implementation,
// Nebuchadnezzar will use it to index vectors.

use std::sync::Arc;
use std::sync::OnceLock;

use dovahkiin::types::Id;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::index::builder::IndexError;

pub const NO_VECTOR_CORE_ERROR: &str =
    "Vector indexer core is not set. Should call `set_vector_index_core` to set it.";
pub const NO_VECTOR_SEARCH_COORDINATOR_ERROR: &str =
    "Vector search coordinator is not set. Should call `set_vector_search_coordinator` to set it.";

/// Encodings to allow metric serialization and conversion.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MetricEncoding {
    L2,
    Cosine,
    Manhattan,
    Chebyshev,
}

/// HNSW configuration for vector indexing.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct HnswConfig {
    /// Max connections per node (M).
    pub m: u16,
    /// Neighbors considered during index build (ef_construction).
    pub ef_construction: u16,
    /// Default ef_search for queries when not overridden.
    pub ef_search_default: u16,
    /// Diversity factor for neighbor selection (0.0-1.0).
    /// Higher values = more selective = better angular diversity.
    /// Default: 0.7. Critical for high-dimensional spaces.
    pub diversity_factor: f32,
}

// Implement Eq manually (f32 doesn't implement Eq, but we need it for schemas)
impl Eq for HnswConfig {}

// Implement Hash manually since f32 doesn't implement Hash
impl std::hash::Hash for HnswConfig {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.m.hash(state);
        self.ef_construction.hash(state);
        self.ef_search_default.hash(state);
        // Convert f32 to bits for hashing
        self.diversity_factor.to_bits().hash(state);
    }
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            ef_construction: 256,
            ef_search_default: 128,
            diversity_factor: 0.7,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum CagraBuildAlgo {
    Auto,
    BruteForceKnn,
    NnDescent,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CagraConfig {
    pub graph_degree: u16,
    pub intermediate_graph_degree: u16,
    pub delta_graph_degree: u16,
    pub max_delta_rows: u32,
    pub build_algo: CagraBuildAlgo,
}

impl Default for CagraConfig {
    fn default() -> Self {
        Self {
            graph_degree: 64,
            intermediate_graph_degree: 128,
            delta_graph_degree: 32,
            max_delta_rows: 50_000,
            build_algo: CagraBuildAlgo::Auto,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum VectorIndexEngine {
    Hnsw(HnswConfig),
    Cagra(CagraConfig),
}

/// Vector index configuration.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct VectorIndexConfig {
    pub metric: MetricEncoding,
    pub engine: VectorIndexEngine,
}

impl VectorIndexConfig {
    pub fn hnsw(metric: MetricEncoding, hnsw: HnswConfig) -> Self {
        Self {
            metric,
            engine: VectorIndexEngine::Hnsw(hnsw),
        }
    }

    pub fn cagra(metric: MetricEncoding, cagra: CagraConfig) -> Self {
        Self {
            metric,
            engine: VectorIndexEngine::Cagra(cagra),
        }
    }
}

/// Result of a vector similarity search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorHit {
    /// The document/cell ID
    pub id: Id,
    /// Distance or similarity score (interpretation depends on metric)
    /// - For L2/Manhattan/Chebyshev: lower is more similar (distance)
    /// - For Cosine: higher is more similar (similarity, typically 0.0 to 1.0)
    pub score: f32,
}

/// Core trait for vector index implementations.
///
/// Implementations should handle:
/// - Vector storage and indexing (e.g., HNSW, IVF-PQ)
/// - Approximate nearest neighbor search
///
/// The vector data is read from the cell during insert operations.
/// The implementation is responsible for:
/// 1. Reading the vector field from the cell
/// 2. Storing the vector with appropriate indexing structures
/// 3. Performing similarity search when queried
pub trait VectorIndexerCore: Send + Sync {
    /// Insert a vector into the index.
    ///
    /// The implementation should read the vector data from the cell.
    ///
    /// # Arguments
    /// * `cell_id` - The ID of the cell containing the vector
    /// * `schema_id` - Schema ID for namespace isolation
    /// * `field_id` - Field ID containing the vector data
    /// * `metric_encoding` - Distance metric to use for this vector
    fn insert(
        &self,
        cell_id: &Id,
        schema_id: u32,
        field_id: u64,
        metric_encoding: MetricEncoding,
        hnsw_config: HnswConfig,
    ) -> BoxFuture<'_, Result<(), IndexError>>;

    /// Remove a vector from the index.
    ///
    /// # Arguments
    /// * `cell_id` - The ID of the cell/document to remove
    /// * `schema_id` - Schema ID for namespace isolation
    /// * `field_id` - Field ID being indexed
    fn remove(
        &self,
        cell_id: &Id,
        schema_id: u32,
        field_id: u64,
    ) -> BoxFuture<'_, Result<(), IndexError>>;

    /// Search for similar vectors using a query vector.
    ///
    /// Performs approximate nearest neighbor search.
    ///
    /// # Arguments
    /// * `schema_id` - Schema ID for namespace isolation
    /// * `field_id` - Field ID to search within
    /// * `query_vector` - The query vector to find neighbors for
    /// * `limit` - Maximum number of results to return
    fn search(
        &self,
        schema_id: u32,
        field_id: u64,
        query_vector: &[f32],
        limit: usize,
        ef_search: Option<u16>,
    ) -> BoxFuture<'_, Result<Vec<VectorHit>, IndexError>>;

    /// Create a new vector index for a schema/field combination.
    ///
    /// Called when a new schema with vector index is created.
    fn new_index(&self, schema_id: u32, field_id: u64) -> BoxFuture<'_, Result<(), IndexError>>;
    fn new_index_with_config(
        &self,
        schema_id: u32,
        field_id: u64,
        hnsw_config: HnswConfig,
    ) -> BoxFuture<'_, Result<(), IndexError>>;

    /// Delete a vector index for a schema/field combination.
    ///
    /// Called when a schema with vector index is deleted.
    fn delete_index(&self, schema_id: u32, field_id: u64) -> BoxFuture<'_, Result<(), IndexError>>;
}

pub trait VectorSearchCoordinator: Send + Sync {
    fn search_distributed(
        &self,
        schema_id: u32,
        field_id: u64,
        query_vector: &[f32],
        limit: usize,
        ef_search: Option<u16>,
    ) -> BoxFuture<'_, Result<Vec<VectorHit>, IndexError>>;
}

/// Client for vector index operations.
///
/// This client wraps a `VectorIndexerCore` implementation and provides
/// a convenient interface for vector index operations.
///
/// # Example
/// ```ignore
/// // In Morpheus or another implementation:
/// let client = VectorIndexClient::new();
/// client.set_vector_index_core(MyVectorCore::new());
///
/// // Search for similar vectors
/// let hits = client.search(schema_id, field_id, &query_vec, 10).await?;
/// ```
pub struct VectorIndexClient {
    vector_core: OnceLock<Arc<dyn VectorIndexerCore>>,
    vector_search_coordinator: OnceLock<Arc<dyn VectorSearchCoordinator>>,
}

impl VectorIndexClient {
    pub fn new() -> Self {
        Self {
            vector_core: OnceLock::new(),
            vector_search_coordinator: OnceLock::new(),
        }
    }

    /// Set the vector index core implementation.
    ///
    /// This should be called once during initialization, typically by Morpheus.
    /// Returns true if the core was set successfully, false if already set.
    pub fn set_vector_index_core<C: VectorIndexerCore + 'static>(&self, core: C) -> bool {
        let res = self.vector_core.set(Arc::new(core));
        return res.is_ok();
    }

    /// Get the vector index core.
    ///
    /// # Panics
    /// Panics if the core has not been set via `set_vector_index_core`.
    pub fn get_vector_index_core(&self) -> &Arc<dyn VectorIndexerCore> {
        self.vector_core.get().expect(NO_VECTOR_CORE_ERROR)
    }

    /// Check if the vector index core has been set.
    pub fn is_vector_index_core_set(&self) -> bool {
        self.vector_core.get().is_some()
    }

    pub fn set_vector_search_coordinator<C: VectorSearchCoordinator + 'static>(
        &self,
        coordinator: C,
    ) -> bool {
        let res = self.vector_search_coordinator.set(Arc::new(coordinator));
        res.is_ok()
    }

    pub fn is_vector_search_coordinator_set(&self) -> bool {
        self.vector_search_coordinator.get().is_some()
    }

    pub fn get_vector_search_coordinator(&self) -> &Arc<dyn VectorSearchCoordinator> {
        self.vector_search_coordinator
            .get()
            .expect(NO_VECTOR_SEARCH_COORDINATOR_ERROR)
    }

    pub fn search_distributed<'a>(
        &'a self,
        schema_id: u32,
        field_id: u64,
        query_vector: &'a [f32],
        limit: usize,
        ef_search: Option<u16>,
    ) -> BoxFuture<'a, Result<Vec<VectorHit>, IndexError>> {
        self.get_vector_search_coordinator().search_distributed(
            schema_id,
            field_id,
            query_vector,
            limit,
            ef_search,
        )
    }

    /// Insert a vector into the index.
    pub fn insert<'a>(
        &'a self,
        cell_id: &Id,
        schema_id: u32,
        field_id: u64,
        metric_encoding: MetricEncoding,
        hnsw_config: HnswConfig,
    ) -> BoxFuture<'a, Result<(), IndexError>> {
        self.get_vector_index_core().insert(
            cell_id,
            schema_id,
            field_id,
            metric_encoding,
            hnsw_config,
        )
    }

    /// Remove a vector from the index.
    pub fn remove<'a>(
        &'a self,
        cell_id: &Id,
        schema_id: u32,
        field_id: u64,
    ) -> BoxFuture<'a, Result<(), IndexError>> {
        self.get_vector_index_core()
            .remove(cell_id, schema_id, field_id)
    }

    /// Search for similar vectors.
    ///
    /// # Arguments
    /// * `schema_id` - Schema ID for namespace isolation
    /// * `field_id` - Field ID to search within
    /// * `query_vector` - The query vector (f32 slice)
    /// * `limit` - Maximum number of results to return
    pub fn search<'a>(
        &'a self,
        schema_id: u32,
        field_id: u64,
        query_vector: &'a [f32],
        limit: usize,
        ef_search: Option<u16>,
    ) -> BoxFuture<'a, Result<Vec<VectorHit>, IndexError>> {
        self.get_vector_index_core()
            .search(schema_id, field_id, query_vector, limit, ef_search)
    }

    /// Create a new vector index.
    pub fn new_index(
        &self,
        schema_id: u32,
        field_id: u64,
    ) -> BoxFuture<'_, Result<(), IndexError>> {
        self.get_vector_index_core().new_index(schema_id, field_id)
    }

    pub fn new_index_with_config(
        &self,
        schema_id: u32,
        field_id: u64,
        hnsw_config: HnswConfig,
    ) -> BoxFuture<'_, Result<(), IndexError>> {
        self.get_vector_index_core()
            .new_index_with_config(schema_id, field_id, hnsw_config)
    }

    /// Delete a vector index.
    pub fn delete_index(
        &self,
        schema_id: u32,
        field_id: u64,
    ) -> BoxFuture<'_, Result<(), IndexError>> {
        self.get_vector_index_core()
            .delete_index(schema_id, field_id)
    }
}

impl Default for VectorIndexClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hnsw_vector_config_uses_explicit_engine() {
        let hnsw = HnswConfig {
            m: 32,
            ef_construction: 512,
            ef_search_default: 400,
            diversity_factor: 0.7,
        };
        let config = VectorIndexConfig::hnsw(MetricEncoding::L2, hnsw);
        let encoded = serde_json::to_string(&config).expect("config should encode");
        let decoded: VectorIndexConfig =
            serde_json::from_str(&encoded).expect("config should decode");

        assert_eq!(config.metric, MetricEncoding::L2);
        assert_eq!(config.engine, VectorIndexEngine::Hnsw(hnsw));
        assert_eq!(decoded, config);
    }

    #[test]
    fn cagra_vector_config_round_trips_through_serde() {
        let cagra = CagraConfig {
            graph_degree: 64,
            intermediate_graph_degree: 128,
            delta_graph_degree: 32,
            max_delta_rows: 50_000,
            build_algo: CagraBuildAlgo::Auto,
        };
        assert_eq!(CagraConfig::default(), cagra);
        let config = VectorIndexConfig::cagra(MetricEncoding::L2, cagra);

        let encoded = serde_json::to_string(&config).expect("config should encode");
        let decoded: VectorIndexConfig =
            serde_json::from_str(&encoded).expect("config should decode");

        assert_eq!(decoded, config);
    }
}
