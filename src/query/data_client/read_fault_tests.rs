//! A read that a server fails must never answer with fewer or wrong rows.
//!
//! The faults are injected at the send of `CellReader`, per server: a set
//! number of sends pass, then batch sends and one-by-one sends fail a set
//! number of times, or always. Each test states which read the fault hits.
use super::read::read_faults;
use super::*;
use crate::{
    query::join::{BatchRowSource, IndexNestedLoopJoin, RowSourcePairs},
    ram::{
        cell::OwnedCell,
        schema::{Field, IndexType, Schema, SchemaVid},
        types::{Map, OwnedMap, Type, Value},
    },
    server::{NebServer, ServerOptions, Service},
};
use bifrost_hasher::hash_str;
use std::sync::Arc;
use tempfile::TempDir;

const SCHEMA: u32 = 4951;
/// Ranged index; the value is the sequence.
const PRICE: &str = "price";
/// Hash index; the value is the sequence modulo `SKUS`.
const SKU: &str = "sku";
/// Not indexed; the value falls as the sequence rises.
const NOTE: &str = "note";
/// Not indexed; the value is the sequence modulo `SKUS`.
const GROUP: &str = "group";
/// More rows than one index cursor block (`SCAN_BUFFER_SIZE`, 64).
const ROWS: u64 = 150;
const SKUS: u64 = 4;
const LOCALITY: u64 = 5;
const ALWAYS: u32 = u32::MAX;
/// The indexed query verifies its candidates in reads of `SCAN_BUFFER_SIZE`.
const INDEXED_QUERY_READS: u32 = (ROWS as u32).div_ceil(SCAN_BUFFER_SIZE as u32);

fn schema(name: &str) -> Schema {
    Schema::new_with_id(
        SCHEMA,
        name,
        None,
        Field::new_schema(vec![
            Field::new_indexed(PRICE, Type::U64, vec![IndexType::Ranged]),
            Field::new_indexed(SKU, Type::U64, vec![IndexType::Hashed]),
            Field::new_unindexed(NOTE, Type::U64),
            Field::new_unindexed(GROUP, Type::U64),
        ]),
        false,
        true,
    )
}

fn row(locality: u64, seq: u64) -> OwnedCell {
    let mut data = OwnedValue::Map(OwnedMap::new());
    data[PRICE] = OwnedValue::U64(seq);
    data[SKU] = OwnedValue::U64(seq % SKUS);
    data[NOTE] = OwnedValue::U64(ROWS + 1 - seq);
    data[GROUP] = OwnedValue::U64(seq % SKUS);
    OwnedCell::new_with_id(SchemaVid(SCHEMA), &Id::from_parts(locality, seq), data)
}

fn id(seq: u64) -> Id {
    Id::from_parts(LOCALITY, seq)
}

fn all_ids() -> Vec<Id> {
    (1..=ROWS).map(id).collect()
}

fn sorted(mut ids: Vec<Id>) -> Vec<Id> {
    ids.sort();
    ids
}

fn owner_of(client: &IndexedDataClient, id: &Id) -> u64 {
    client
        .conshash
        .get_server_id_for_slot(crate::slots::slot_of(id) as u64)
        .unwrap()
}

async fn fixture(name: &str) -> (Arc<NebServer>, IndexedDataClient, u64, TempDir) {
    let dir = TempDir::new().unwrap();
    let address = crate::utils::test_port::unique_localhost_addr();
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            raft_storage: Some(dir.path().to_str().unwrap().into()),
            index_enabled: true,
            services: vec![Service::Cell, Service::Query, Service::RangedIndexer],
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &address,
        &name.to_string(),
        async |_| {},
    )
    .await
    .unwrap();
    let client = server.data_client(&vec![address]).await.unwrap();
    client
        .new_schema_with_id(schema(name))
        .await
        .unwrap()
        .unwrap();
    for seq in 1..=ROWS {
        client
            .write_cell(row(LOCALITY, seq))
            .await
            .unwrap()
            .unwrap();
    }
    server.await_index_tasks().await;
    let data_client = server.indexed_data_client();
    let owner = owner_of(&data_client, &id(1));
    (server, data_client, owner, dir)
}

fn compare(op: &str, field: &str, value: u64) -> Expr {
    Expr::List(vec![
        Expr::Symbol(hash_str(op), op.into()),
        Expr::Symbol(hash_str(field), field.into()),
        Expr::Value(OwnedValue::U64(value)),
    ])
}

async fn ids_of(cursor: Result<IdCursor, RPCError>) -> Result<Vec<Id>, RPCError> {
    let mut cursor = cursor?;
    let mut ids = vec![];
    while let Some(id) = cursor.next().await? {
        ids.push(id);
    }
    Ok(ids)
}

/// The index-served query: candidates from the ranged index, read verified.
async fn indexed_query(client: &IndexedDataClient) -> Result<Vec<Id>, RPCError> {
    ids_of(
        client
            .query_ids_with_options(
                SchemaUid(SCHEMA),
                compare(">=", PRICE, 1),
                QueryOrdering::Asc,
                None,
                None,
                None,
                None,
                Verify::Predicate,
            )
            .await,
    )
    .await
}

/// The resumable page of a filtered schema scan the index cannot serve.
async fn resumable_page(client: &IndexedDataClient) -> Result<ScanPage, RPCError> {
    Ok(client
        .query_ids_resumable(
            SchemaUid(SCHEMA),
            compare(">", NOTE, 0),
            1000,
            None,
            Verify::Predicate,
        )
        .await?
        .expect("an unindexed selection is a resumable scan"))
}

/// The error must be a `CellReadFailure` that names `server`.
fn assert_read_failure(what: &str, error: &RPCError, server: u64) {
    assert_read_failure_with(what, error, server, "injected read failure");
}

fn assert_read_failure_with(what: &str, error: &RPCError, server: u64, reason: &str) {
    let failure = CellReadFailure::of(error)
        .unwrap_or_else(|| panic!("{what}: not a cell read failure: {error:?}"));
    assert_eq!(failure.server_id, Some(server), "{what}: {failure}");
    assert!(failure.reason.contains(reason), "{what}: {failure}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_read_that_fails_twice_is_retried_and_returns_every_row() {
    let (server, client, owner, _dir) = fixture("read_fault_transient").await;
    let fault = read_faults::inject(owner, 0, 2, 0);
    let began = std::time::Instant::now();
    let ids = indexed_query(&client).await.unwrap();
    let waited = began.elapsed();
    read_faults::clear(owner);
    assert_eq!(sorted(ids), all_ids(), "a transient failure lost rows");
    // The first read sends 3 times (2 failures, 1 retry that works); the
    // other reads of this query send once.
    assert_eq!(
        fault.batch_sends(),
        3 + (INDEXED_QUERY_READS - 1),
        "{fault:?}"
    );
    assert_eq!(
        fault.single_sends(),
        0,
        "a retried batch needs no single reads"
    );
    assert!(
        waited >= std::time::Duration::from_millis(50),
        "the retries wait 10 ms and 40 ms: {waited:?}"
    );

    read_faults::inject(owner, 0, 2, 0);
    let page = resumable_page(&client).await.unwrap();
    read_faults::clear(owner);
    assert_eq!(page.ids, all_ids(), "a transient failure lost page rows");
    assert_eq!(page.next_cursor, None);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_read_that_keeps_failing_falls_back_to_single_reads() {
    let (server, client, owner, _dir) = fixture("read_fault_single").await;
    let fault = read_faults::inject(owner, 0, ALWAYS, 0);
    let ids = indexed_query(&client).await.unwrap();
    read_faults::clear(owner);
    assert_eq!(sorted(ids), all_ids(), "the single-read fallback lost rows");
    assert_eq!(
        fault.batch_sends(),
        3 * INDEXED_QUERY_READS,
        "each batch is sent exactly 3 times: {fault:?}"
    );
    assert_eq!(fault.single_sends(), ROWS as u32, "{fault:?}");

    read_faults::inject(owner, 0, ALWAYS, 0);
    let page = resumable_page(&client).await.unwrap();
    read_faults::clear(owner);
    assert_eq!(
        page.ids,
        all_ids(),
        "the single-read fallback lost page rows"
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_that_cannot_be_read_is_a_typed_error_not_a_short_answer() {
    let (server, client, owner, _dir) = fixture("read_fault_permanent").await;
    let fault = read_faults::inject(owner, 0, ALWAYS, ALWAYS);
    let query = indexed_query(&client).await;
    let page = resumable_page(&client).await;
    read_faults::clear(owner);
    assert_read_failure(
        "indexed query",
        &query.map(|ids| ids.len()).unwrap_err(),
        owner,
    );
    assert_read_failure(
        "resumable page",
        &page.map(|page| page.ids.len()).unwrap_err(),
        owner,
    );
    // Fail fast: one failed single read ends each read.
    assert!(fault.single_sends() <= 2, "{fault:?}");

    // The same reads answer in full once the server recovers.
    assert_eq!(sorted(indexed_query(&client).await.unwrap()), all_ids());
    assert_eq!(resumable_page(&client).await.unwrap().ids, all_ids());
    server.shutdown().await;
}

/// A cell that fails its check is still dropped: that is not a read failure.
#[tokio::test(flavor = "multi_thread")]
async fn a_cell_that_fails_its_check_is_dropped_without_an_error() {
    let (server, client, _owner, _dir) = fixture("read_fault_check").await;
    let page = client
        .query_ids_resumable(
            SchemaUid(SCHEMA),
            compare(">", NOTE, ROWS / 2),
            1000,
            None,
            Verify::Predicate,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(page.ids, all_ids()[..(ROWS / 2) as usize].to_vec());
    let other_schema = client
        .read_cells_from_ids_checked(
            &all_ids(),
            &vec![],
            &Expr::nothing(),
            &Expr::nothing(),
            &vec![crate::index::verification::IndexCheck::Schema(SchemaUid(
                SCHEMA + 1,
            ))],
        )
        .await
        .unwrap();
    assert!(other_schema.is_empty());
    server.shutdown().await;
}

/// ORDER BY an unindexed field reads the cells to sort them. If that read
/// fails, the LIMIT would take the wrong rows; the answer must be an error.
///
/// The sort read is the first read of this query. The fault fails its 3
/// batch sends and its first single read; every later send works, so only
/// the sort read can see the failure.
#[tokio::test(flavor = "multi_thread")]
async fn order_by_with_limit_never_answers_from_a_failed_sort_read() {
    let (server, client, owner, _dir) = fixture("read_fault_sort").await;
    let top_five = |client: &IndexedDataClient| {
        let client = client.clone();
        async move {
            ids_of(
                client
                    .query_ids_with_options(
                        SchemaUid(SCHEMA),
                        compare(">", NOTE, 0),
                        QueryOrdering::Desc,
                        Some(hash_str(NOTE)),
                        None,
                        Some(5),
                        None,
                        Verify::Predicate,
                    )
                    .await,
            )
            .await
        }
    };
    // NOTE falls as the id rises: the 5 largest NOTEs are the 5 smallest ids.
    let expected: Vec<Id> = (1..=5).map(id).collect();
    assert_eq!(top_five(&client).await.unwrap(), expected);

    read_faults::inject(owner, 0, 2, 0);
    let transient = top_five(&client).await;
    read_faults::clear(owner);
    assert_eq!(
        transient.unwrap(),
        expected,
        "a transient failure changed the rows"
    );

    read_faults::inject(owner, 0, 3, 1);
    let failed = top_five(&client).await;
    read_faults::clear(owner);
    assert_read_failure("order by", &failed.unwrap_err(), owner);
    server.shutdown().await;
}

/// DISTINCT reads the cells to compare their fields. A failed read must not
/// return the ids unfiltered, and must not drop the ids it could not read.
///
/// The selection read passes; the fault hits the DISTINCT read.
#[tokio::test(flavor = "multi_thread")]
async fn distinct_never_answers_from_a_failed_read() {
    let (server, client, owner, _dir) = fixture("read_fault_distinct").await;
    let distinct = |client: &IndexedDataClient| {
        let client = client.clone();
        async move {
            ids_of(
                client
                    .query_ids_with_options(
                        SchemaUid(SCHEMA),
                        compare(">", NOTE, 0),
                        QueryOrdering::Asc,
                        None,
                        Some(vec![hash_str(GROUP)]),
                        None,
                        None,
                        Verify::Predicate,
                    )
                    .await,
            )
            .await
        }
    };
    let expected: Vec<Id> = (1..=SKUS).map(id).collect();
    assert_eq!(distinct(&client).await.unwrap(), expected);

    read_faults::inject(owner, 1, 2, 0);
    let transient = distinct(&client).await;
    read_faults::clear(owner);
    assert_eq!(transient.unwrap(), expected);

    read_faults::inject(owner, 1, 3, 1);
    let failed = distinct(&client).await;
    read_faults::clear(owner);
    assert_read_failure("distinct", &failed.unwrap_err(), owner);
    server.shutdown().await;
}

/// The projected read of a query result (`read_all_cells_selected`).
///
/// The selection read passes; the fault hits the projection read.
#[tokio::test(flavor = "multi_thread")]
async fn a_projected_read_never_answers_with_fewer_rows() {
    let (server, client, owner, _dir) = fixture("read_fault_projection").await;
    let project = |client: &IndexedDataClient| {
        let client = client.clone();
        async move {
            client
                .query_with_options(
                    SchemaUid(SCHEMA),
                    compare(">", NOTE, 0),
                    QueryOrdering::Asc,
                    None,
                    None,
                    None,
                    None,
                    vec![ProjectionField {
                        field_id: hash_str(NOTE),
                        alias: None,
                    }],
                )
                .await
                .map(|rows| rows.buffer.len())
        }
    };
    assert_eq!(project(&client).await.unwrap(), ROWS as usize);

    read_faults::inject(owner, 1, ALWAYS, ALWAYS);
    let failed = project(&client).await;
    read_faults::clear(owner);
    assert_read_failure("projection", &failed.unwrap_err(), owner);
    server.shutdown().await;
}

/// A hash query verifies its bucket's candidates with the reader, and the
/// index join probes through the hash query.
#[tokio::test(flavor = "multi_thread")]
async fn hash_queries_and_index_joins_never_answer_with_fewer_rows() {
    let (server, client, owner, _dir) = fixture("read_fault_hash").await;
    let expected: Vec<Id> = (1..=ROWS).filter(|seq| seq % SKUS == 1).map(id).collect();
    let hashed = |client: &IndexedDataClient| {
        let client = client.clone();
        async move {
            client
                .hashed_query(
                    SchemaUid(SCHEMA),
                    hash_str(SKU),
                    &OwnedValue::U64(1),
                    Verify::Predicate,
                )
                .await
        }
    };
    let join = |client: &IndexedDataClient| {
        let client = client.clone();
        async move {
            let mut probe = OwnedValue::Map(OwnedMap::new());
            probe[SKU] = OwnedValue::U64(1);
            let outer = OwnedCell::new_with_id(SchemaVid(SCHEMA), &Id::from_parts(9, 1), probe);
            let mut join = IndexNestedLoopJoin::new(
                BatchRowSource::new(vec![outer]),
                client,
                SchemaUid(SCHEMA),
                hash_str(SKU),
                hash_str(SKU),
            );
            let mut right = vec![];
            while let Some(pair) = join.next_pair().await? {
                right.push(pair.right.id());
            }
            Ok::<_, RPCError>(right)
        }
    };
    assert_eq!(sorted(hashed(&client).await.unwrap().unwrap()), expected);
    assert_eq!(sorted(join(&client).await.unwrap()), expected);

    read_faults::inject(owner, 0, 2, 0);
    let transient = hashed(&client).await;
    read_faults::clear(owner);
    assert_eq!(sorted(transient.unwrap().unwrap()), expected);

    for what in ["hash query", "index join"] {
        read_faults::inject(owner, 0, ALWAYS, ALWAYS);
        let error = if what == "hash query" {
            hashed(&client)
                .await
                .map(|ids| ids.map(|ids| ids.len()))
                .unwrap_err()
        } else {
            join(&client).await.map(|ids| ids.len()).unwrap_err()
        };
        read_faults::clear(owner);
        assert_read_failure(what, &error, owner);
    }
    server.shutdown().await;
}

/// The first block of a range scan reads fine; the read for the next block
/// fails. The cursor must return the error, not end the scan early.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_read_when_the_cursor_advances_is_an_error_not_the_end() {
    let (server, client, owner, _dir) = fixture("read_fault_advance").await;
    let scan = || {
        let client = client.clone();
        async move {
            client
                .range_index_scan(
                    SchemaUid(SCHEMA),
                    hash_str(PRICE),
                    ValueRange {
                        start: ValueRangeTerm::Inclusive(OwnedValue::U64(1).feature()),
                        end: ValueRangeTerm::Inclusive(OwnedValue::U64(ROWS).feature()),
                    },
                    vec![],
                    Expr::nothing(),
                    Expr::nothing(),
                    crate::index::ranged::tree::btree::Ordering::Forward,
                )
                .await
        }
    };
    let mut full = scan().await.unwrap();
    let mut rows = 0;
    while full.next().await.unwrap().is_some() {
        rows += 1;
    }
    assert_eq!(rows, ROWS as usize);

    read_faults::inject(owner, 1, ALWAYS, ALWAYS);
    let mut cursor = scan().await.unwrap();
    let mut before_error = 0;
    let error = loop {
        match cursor.next().await {
            Ok(Some(_)) => before_error += 1,
            Ok(None) => panic!("the scan ended after {before_error} rows instead of failing"),
            Err(error) => break error,
        }
    };
    read_faults::clear(owner);
    assert!(before_error > 0 && before_error < ROWS as usize);
    assert_read_failure("cursor advance", &error, owner);
    server.shutdown().await;
}

/// Two owners: one server answers, the other fails. The rows of the failing
/// owner must come back after a transient failure, and a lasting failure is
/// an error that names that owner.
#[tokio::test(flavor = "multi_thread")]
async fn one_failing_owner_of_two_never_shortens_the_answer() {
    let group = "read_fault_owners";
    let addresses: Vec<String> = (0..2)
        .map(|_| crate::utils::test_port::unique_localhost_addr())
        .collect();
    let opts = ServerOptions {
        chunk_size: 32 * 1024 * 1024,
        db_size: 32 * 1024 * 1024,
        tiered_config: None,
        backup_storage: None,
        wal_storage: None,
        raft_storage: None,
        index_enabled: false,
        services: vec![Service::Cell],
        enable_recovery: false,
        disable_storage_locks: true,
    };
    let mut servers = vec![];
    for address in &addresses {
        servers.push(
            NebServer::new_cluster_from_opts(&opts, address, &addresses, group, async |_| {})
                .await
                .unwrap(),
        );
    }
    let neb =
        crate::client::AsyncClient::new(&servers[0].rpc, &servers[0].membership, &addresses, group)
            .await
            .unwrap();
    neb.reload_slot_owners().await;
    neb.new_schema_with_id(schema(group))
        .await
        .unwrap()
        .unwrap();
    let client = servers[0].indexed_data_client();
    let (first, second) = (servers[0].server_id, servers[1].server_id);
    assert_ne!(first, second);

    // The first member owns every slot after start. Route one locality to the
    // second member and put that locality's cells on it.
    const MOVED: u64 = 6;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while servers[1].chunks().list[0]
            .meta
            .schemas
            .get(&SchemaVid(SCHEMA))
            .is_none()
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the schema reaches the second member");
    assert!(client
        .conshash
        .note_slot_owner(MOVED, second, crate::slots::SLOT_COUNT, u64::MAX / 2));
    let mut ids = vec![];
    for seq in 1..=20 {
        neb.write_cell(row(LOCALITY, seq)).await.unwrap().unwrap();
        servers[1]
            .chunks()
            .write_cell(&mut row(MOVED, seq))
            .unwrap();
        ids.push(Id::from_parts(LOCALITY, seq));
        ids.push(Id::from_parts(MOVED, seq));
    }
    assert_eq!(owner_of(&client, &Id::from_parts(LOCALITY, 1)), first);
    assert_eq!(owner_of(&client, &Id::from_parts(MOVED, 1)), second);
    let read = |client: &IndexedDataClient, ids: Vec<Id>| {
        let client = client.clone();
        async move {
            client
                .read_cells_from_ids_checked(
                    &ids,
                    &vec![],
                    &Expr::nothing(),
                    &Expr::nothing(),
                    &vec![crate::index::verification::IndexCheck::Schema(SchemaUid(
                        SCHEMA,
                    ))],
                )
                .await
                .map(|cells| cells.into_iter().map(|cell| cell.id()).collect::<Vec<_>>())
        }
    };
    assert_eq!(read(&client, ids.clone()).await.unwrap(), ids);

    let healthy = read_faults::inject(first, 0, 0, 0);
    let failing = read_faults::inject(second, 0, 2, 0);
    let transient = read(&client, ids.clone()).await;
    read_faults::clear(second);
    read_faults::clear(first);
    assert_eq!(
        transient.unwrap(),
        ids,
        "the rows of the failing owner were lost"
    );
    assert_eq!(healthy.batch_sends(), 1);
    assert_eq!(failing.batch_sends(), 3);

    read_faults::inject(second, 0, ALWAYS, ALWAYS);
    let failed = read(&client, ids.clone()).await;
    read_faults::clear(second);
    assert_read_failure("second owner", &failed.unwrap_err(), second);
    for server in servers {
        server.shutdown().await;
    }
}

/// A server that read a cell and could not answer for it (an unavailable
/// schema, a selection it could not evaluate) answers with a per-cell error.
/// That cell exists, so the read is an error, not a shorter answer. A cell that
/// fails the check (`NotMatch`) is still dropped.
#[tokio::test(flavor = "multi_thread")]
async fn a_cell_the_server_could_not_answer_for_is_an_error_not_a_dropped_row() {
    use crate::ram::cell::ReadError;
    let (server, client, owner, _dir) = fixture("read_fault_cell_error").await;
    for (error, reason) in [
        (
            ReadError::SchemaDoesNotExisted(SCHEMA),
            "SchemaDoesNotExisted",
        ),
        (
            ReadError::ExecError("index candidate schema is absent".into()),
            "index candidate schema is absent",
        ),
        (
            ReadError::ExecError("cannot evaluate".into()),
            "cannot evaluate",
        ),
        (ReadError::StaleCellPointer, "StaleCellPointer"),
    ] {
        let fault = read_faults::inject_cell_error(owner, id(3), error);
        let query = indexed_query(&client).await;
        let page = resumable_page(&client).await;
        read_faults::clear(owner);
        assert_read_failure_with(
            "indexed query",
            &query.map(|ids| ids.len()).unwrap_err(),
            owner,
            reason,
        );
        assert_read_failure_with(
            "resumable page",
            &page.map(|page| page.ids.len()).unwrap_err(),
            owner,
            reason,
        );
        // The failed cell was read alone once more before the error.
        assert!(fault.single_sends() >= 1, "{fault:?}");
    }

    read_faults::inject_cell_error(owner, id(3), ReadError::NotMatch);
    let query = indexed_query(&client).await;
    read_faults::clear(owner);
    let expected: Vec<Id> = all_ids()
        .into_iter()
        .filter(|other| *other != id(3))
        .collect();
    assert_eq!(
        sorted(query.unwrap()),
        expected,
        "NotMatch is a rejected row"
    );
    server.shutdown().await;
}

/// BM25 phrase boosting reads the candidates. A failed read must fail the
/// search, not rank the candidates as if no phrase matched.
#[tokio::test(flavor = "multi_thread")]
async fn phrase_boosting_never_ranks_from_a_failed_read() {
    const TEXT: &str = "text";
    let (server, client, owner, _dir) = fixture("read_fault_phrase").await;
    let neb = server
        .data_client(&vec![server.rpc.address.clone()])
        .await
        .unwrap();
    let text_schema = SCHEMA + 2;
    neb.new_schema_with_id(Schema::new_with_id(
        text_schema,
        "read_fault_phrase_text",
        None,
        Field::new_schema(vec![Field::new_indexed(
            TEXT,
            Type::String,
            vec![IndexType::Fulltext],
        )]),
        false,
        false,
    ))
    .await
    .unwrap()
    .unwrap();
    for (seq, text) in [
        (1, "a ranked search engine"),
        (2, "search engine ranked by phrase"),
        (3, "cooking without a search"),
    ] {
        let mut data = OwnedValue::Map(OwnedMap::new());
        data[TEXT] = OwnedValue::String(text.into());
        neb.write_cell(OwnedCell::new_with_id(
            SchemaVid(text_schema),
            &Id::from_parts(LOCALITY, 1_000 + seq),
            data,
        ))
        .await
        .unwrap()
        .unwrap();
    }
    server.await_index_tasks().await;
    let search = |client: &IndexedDataClient| {
        let client = client.clone();
        async move {
            client
                .bm25_search(
                    SchemaUid(text_schema),
                    hash_str(TEXT),
                    "search engine",
                    2,
                    true,
                )
                .await
        }
    };
    let ranked = search(&client).await.unwrap().unwrap();
    assert_eq!(ranked.len(), 2);

    read_faults::inject(owner, 0, ALWAYS, ALWAYS);
    let failed = search(&client).await;
    read_faults::clear(owner);
    match failed {
        Ok(Err(crate::ram::cell::ReadError::ExecError(message))) => {
            assert!(message.contains("injected read failure"), "{message}")
        }
        other => panic!("a failed candidate read must fail the search: {other:?}"),
    }
    server.shutdown().await;
}
