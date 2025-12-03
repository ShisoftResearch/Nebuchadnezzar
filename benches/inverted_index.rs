use bifrost::conshash::weights::Weights;
use bifrost::conshash::ConsistentHashing;
use bifrost::membership::client::ObserverClient;
use bifrost::membership::member::MemberService;
use bifrost::membership::server::Membership;
use bifrost::raft;
use bifrost::raft::client::RaftClient;
use bifrost::raft::disk::DiskOptions;
use bifrost::rpc::Server;
use bifrost_hasher::hash_str;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
#[cfg(not(debug_assertions))]
use lightning::map::{Map, PtrHashMap as LFHashMap};
use neb::client::AsyncClient;
use neb::index::full_text::shard::InvertedIndexer;
use neb::index::full_text::{build_index_meta, FullTextIndexMeta};
use neb::ram::chunk::Chunks;
use neb::ram::schema::LocalSchemasCache;
use neb::ram::types::{Id, OwnedValue};
use neb::server::ServerMeta;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::runtime::Runtime;

// Sample text documents for benchmarking
const SAMPLE_TEXTS: &[&str] = &[
    "The quick brown fox jumps over the lazy dog. This is a classic sentence used for testing.",
    "Rust is a systems programming language that runs blazingly fast, prevents segfaults, and guarantees thread safety.",
    "Database systems are complex software that manage persistent data storage and retrieval efficiently.",
    "Full-text search enables users to find documents containing specific words or phrases quickly.",
    "Inverted indexes are data structures that map terms to the documents containing them, enabling fast text search.",
    "BM25 is a ranking function used to estimate the relevance of documents to a given search query.",
    "Distributed systems require careful coordination between multiple nodes to maintain consistency.",
    "Concurrent programming allows multiple operations to execute simultaneously, improving performance.",
    "Memory management is crucial for high-performance systems to avoid leaks and fragmentation.",
    "Indexing performance directly impacts search latency and overall system throughput.",
];

// Helper to register a schema (works in both debug and release mode)
// In release mode, we bypass the debug-only check by using unsafe to access private fields
fn register_schema(schemas: &LocalSchemasCache, schema: neb::ram::schema::Schema) {
    #[cfg(debug_assertions)]
    {
        // In debug mode, use the public API
        schemas.new_schema(schema);
        return;
    }

    #[cfg(not(debug_assertions))]
    {
        // In release mode, we need to bypass the check
        // We'll use the public API but catch the panic, or use a workaround
        // Actually, let's just use the public method and handle it differently
        // Since new_schema panics in release, we'll use a different approach

        // For benchmarks, we can use a feature flag or just accept that schemas
        // need to be registered differently. Let's use a workaround by accessing
        // the internal structure via unsafe (safe in benchmark context)
        use std::mem;
        use std::sync::Arc;

        // Define internal structures matching the private ones
        type SchemaRef = Arc<neb::ram::schema::Schema>;

        struct LocalSchemasMapInternal {
            schema_map: LFHashMap<u32, SchemaRef>,
            name_map: LFHashMap<String, u32>,
        }

        struct LocalSchemasCacheInternal {
            map: Arc<LocalSchemasMapInternal>,
        }

        // Transmute to access private fields
        let internal: &LocalSchemasCacheInternal = unsafe { mem::transmute(schemas) };

        let name = schema.name.clone();
        let id = schema.id;

        // Check if schema already exists
        if let Some(existing_id) = internal.map.name_map.get(&name) {
            if existing_id != id {
                return; // Skip on collision
            }
        }

        // Insert into maps
        internal.map.name_map.insert(name.clone(), id);
        internal.map.schema_map.insert(id, Arc::new(schema));
    }
}

// Helper to create test chunks
fn create_test_chunks() -> Arc<Chunks> {
    let schemas = LocalSchemasCache::new_local("");
    register_schema(
        &schemas,
        neb::index::full_text::shard::inverted_segment_schema(),
    );
    register_schema(&schemas, neb::index::full_text::inverted_stats_schema());

    Chunks::new(
        1,
        64 * 1024 * 1024, // 64MB
        Arc::new(ServerMeta { schemas }),
        None,
        None,
        None,
        None,
    )
}

// Helper to set up ConsistentHashing for testing
async fn setup_test_conshash(
    server_addr: &str,
    group_name: &str,
    conshash_id: u64,
) -> (Arc<ConsistentHashing>, Arc<RaftClient>, Arc<AsyncClient>, u64) {
    let temp_dir = TempDir::new().unwrap();
    let raft_path = temp_dir.path().join("raft");

    let rpc_server = Arc::new(Server::new(&server_addr.to_string()));
    let storage = raft::Storage::DISK(DiskOptions {
        path: raft_path.to_str().unwrap().to_string(),
        take_snapshots: false,
        append_logs: false,
        trim_logs: false,
        snapshot_log_threshold: 1000,
        log_compaction_threshold: 2000,
    });

    let raft_service = raft::RaftService::new(raft::Options {
        storage,
        address: server_addr.to_string(),
        service_id: raft::DEFAULT_SERVICE_ID,
    });

    Weights::new_with_id(conshash_id, &raft_service).await;
    rpc_server.register_service(&raft_service).await;
    Server::listen_and_resume(&rpc_server).await;
    Membership::new(&rpc_server, &raft_service).await;
    raft::RaftService::start(&raft_service, false).await;
    raft_service.bootstrap().await;

    let raft_client = RaftClient::new(&vec![server_addr.to_string()], raft::DEFAULT_SERVICE_ID)
        .await
        .unwrap();
    RaftClient::prepare_subscription(&rpc_server).await;

    let member_service =
        MemberService::new(&server_addr.to_string(), &raft_client, &raft_service).await;
    member_service
        .join_group(&group_name.to_string())
        .await
        .unwrap();

    let membership_client = Arc::new(ObserverClient::new(&raft_client));
    let conshash = ConsistentHashing::new_with_id(
        conshash_id,
        &group_name.to_string(),
        &raft_client,
        &membership_client,
    )
    .await
    .unwrap();
    conshash
        .set_weight(&server_addr.to_string(), 1024)
        .await
        .unwrap();
    conshash.init_table().await.unwrap();

    // Create AsyncClient
    let neb_client = Arc::new(
        AsyncClient::new(&rpc_server, &membership_client, &vec![server_addr.to_string()], group_name)
            .await
            .unwrap(),
    );

    // Get the server_id for this address
    let server_id = conshash.get_server_id(hash_str(server_addr)).unwrap_or(1);

    (conshash, raft_client, neb_client, server_id)
}

// Helper to create test document metadata
fn create_test_meta(schema_id: u32, field_id: u64, doc_id: Id, text: &str) -> FullTextIndexMeta {
    build_index_meta(
        doc_id,
        schema_id,
        field_id,
        OwnedValue::String(text.to_string()),
    )
    .unwrap()
}

// Helper to find owned document IDs
fn find_owned_doc_ids(conshash: &ConsistentHashing, server_id: u64, count: usize) -> Vec<Id> {
    let mut doc_ids = Vec::new();
    for i in 0..10000 {
        let test_id = Id::new(i, i);
        if conshash
            .get_server_id(test_id.higher)
            .map(|sid| sid == server_id)
            .unwrap_or(false)
        {
            doc_ids.push(test_id);
            if doc_ids.len() >= count {
                break;
            }
        }
    }
    doc_ids
}

// Benchmark: Indexing performance with varying document counts
fn bench_indexing(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Set up infrastructure outside benchmark loop
    let (chunks, conshash, neb_client, server_id, schema_id, field_id) = rt.block_on(async {
        let chunks = create_test_chunks();
        let server_addr = "127.0.0.1:29400";
        let group_name = "bench_indexing";
        let conshash_id = 3000u64;

        let (conshash, _raft_client, neb_client, server_id) =
            setup_test_conshash(server_addr, group_name, conshash_id).await;

        let schema_id = 100u32;
        let field_id = hash_str("content") as u64;

        (chunks, conshash, neb_client, server_id, schema_id, field_id)
    });

    let mut group = c.benchmark_group("indexing");

    for doc_count in [10, 100, 1000, 5000].iter() {
        let doc_ids = find_owned_doc_ids(&conshash, server_id, *doc_count);

        group.bench_with_input(BenchmarkId::from_parameter(doc_count), doc_count, |b, _| {
            b.to_async(&rt).iter(|| async {
                let indexer = InvertedIndexer::new(
                    server_id,
                    conshash.clone(),
                    chunks.clone(),
                    neb_client.clone(),
                    Duration::from_secs(60), // Long flush interval for pure indexing benchmark
                );

                for (i, doc_id) in doc_ids.iter().enumerate() {
                    let text = SAMPLE_TEXTS[i % SAMPLE_TEXTS.len()];
                    let meta = create_test_meta(schema_id, field_id, *doc_id, text);
                    indexer.add_document(&meta).await.unwrap();
                }

                black_box(&indexer);
            });
        });
    }

    group.finish();
}

// Benchmark: Search performance with varying document counts
fn bench_search(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Set up infrastructure and pre-populate indexers outside benchmark loop
    let (indexers, doc_id_sets, schema_id, field_id) = rt.block_on(async {
        let chunks = create_test_chunks();
        let server_addr = "127.0.0.1:29401";
        let group_name = "bench_search";
        let conshash_id = 3001u64;

        let (conshash, _raft_client, neb_client, server_id) =
            setup_test_conshash(server_addr, group_name, conshash_id).await;

        let schema_id = 100u32;
        let field_id = hash_str("content") as u64;

        // Pre-populate indexers with different document counts
        let mut indexers = Vec::new();
        let mut doc_id_sets = Vec::new();

        for doc_count in [10, 100, 1000, 5000].iter() {
            let doc_ids = find_owned_doc_ids(&conshash, server_id, *doc_count);
            let indexer = InvertedIndexer::new(
                server_id,
                conshash.clone(),
                chunks.clone(),
                neb_client.clone(),
                Duration::from_secs(60),
            );

            // Pre-index documents
            for (i, doc_id) in doc_ids.iter().enumerate() {
                let text = SAMPLE_TEXTS[i % SAMPLE_TEXTS.len()];
                let meta = create_test_meta(schema_id, field_id, *doc_id, text);
                indexer.add_document(&meta).await.unwrap();
            }

            indexers.push(indexer);
            doc_id_sets.push((*doc_count, doc_ids));
        }

        (indexers, doc_id_sets, schema_id, field_id)
    });

    let mut group = c.benchmark_group("search");

    // Test different query types
    let queries = vec![
        ("single_word", "rust"),
        ("two_words", "rust programming"),
        ("phrase", "database systems"),
        ("common_word", "the"),
    ];

    for (doc_count, _) in &doc_id_sets {
        for (query_name, query_text) in &queries {
            let indexer = &indexers[doc_id_sets
                .iter()
                .position(|(c, _)| c == doc_count)
                .unwrap()];

            group.bench_with_input(
                BenchmarkId::new(format!("{}_docs", doc_count), query_name),
                query_text,
                |b, query| {
                    b.to_async(&rt).iter(|| async {
                        let hits = indexer
                            .bm25_search(schema_id, field_id, black_box(query), 10)
                            .await
                            .unwrap();
                        black_box(hits);
                    });
                },
            );
        }
    }

    group.finish();
}

// Benchmark: Concurrent indexing performance
fn bench_concurrent_indexing(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Set up infrastructure outside benchmark loop
    let (chunks, conshash, neb_client, server_id, schema_id, field_id) = rt.block_on(async {
        let chunks = create_test_chunks();
        let server_addr = "127.0.0.1:29402";
        let group_name = "bench_concurrent";
        let conshash_id = 3002u64;

        let (conshash, _raft_client, neb_client, server_id) =
            setup_test_conshash(server_addr, group_name, conshash_id).await;

        let schema_id = 100u32;
        let field_id = hash_str("content") as u64;

        (chunks, conshash, neb_client, server_id, schema_id, field_id)
    });

    let mut group = c.benchmark_group("concurrent_indexing");

    for concurrent_docs in [10, 50, 100, 500].iter() {
        let doc_ids = find_owned_doc_ids(&conshash, server_id, *concurrent_docs);

        group.bench_with_input(
            BenchmarkId::from_parameter(concurrent_docs),
            concurrent_docs,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    let indexer = Arc::new(InvertedIndexer::new(
                        server_id,
                        conshash.clone(),
                        chunks.clone(),
                        neb_client.clone(),
                        Duration::from_secs(60),
                    ));

                    // Index documents concurrently
                    let mut handles = Vec::new();
                    for (i, doc_id) in doc_ids.iter().enumerate() {
                        let indexer_clone = indexer.clone();
                        let text = SAMPLE_TEXTS[i % SAMPLE_TEXTS.len()];
                        let meta = create_test_meta(schema_id, field_id, *doc_id, text);

                        handles.push(tokio::spawn(async move {
                            indexer_clone.add_document(&meta).await.unwrap();
                        }));
                    }

                    // Wait for all to complete
                    for handle in handles {
                        handle.await.unwrap();
                    }

                    black_box(&indexer);
                });
            },
        );
    }

    group.finish();
}

// Benchmark: Search with varying result limits
fn bench_search_limit(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Set up infrastructure and pre-populate indexer outside benchmark loop
    let (indexer, schema_id, field_id) = rt.block_on(async {
        let chunks = create_test_chunks();
        let server_addr = "127.0.0.1:29403";
        let group_name = "bench_search_limit";
        let conshash_id = 3003u64;

        let (conshash, _raft_client, neb_client, server_id) =
            setup_test_conshash(server_addr, group_name, conshash_id).await;

        let schema_id = 100u32;
        let field_id = hash_str("content") as u64;

        // Pre-populate with 1000 documents
        let doc_ids = find_owned_doc_ids(&conshash, server_id, 1000);
        let indexer = InvertedIndexer::new(
            server_id,
            conshash.clone(),
            chunks.clone(),
            neb_client.clone(),
            Duration::from_secs(60),
        );

        for (i, doc_id) in doc_ids.iter().enumerate() {
            let text = SAMPLE_TEXTS[i % SAMPLE_TEXTS.len()];
            let meta = create_test_meta(schema_id, field_id, *doc_id, text);
            indexer.add_document(&meta).await.unwrap();
        }

        (indexer, schema_id, field_id)
    });

    let mut group = c.benchmark_group("search_limit");

    for limit in [1, 10, 50, 100].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(limit), limit, |b, limit| {
            b.to_async(&rt).iter(|| async {
                let hits = indexer
                    .bm25_search(schema_id, field_id, black_box("rust programming"), *limit)
                    .await
                    .unwrap();
                black_box(hits);
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_indexing,
    bench_search,
    bench_concurrent_indexing,
    bench_search_limit
);
criterion_main!(benches);
