async fn g23_schema_fixture(
    name: &str,
    clustered: bool,
) -> (
    Arc<NebRPCService>,
    Arc<crate::server::NebServer>,
    tempfile::TempDir,
    Id,
) {
    let (rpc, server, dir) = idcontract_fixture(name).await;
    if clustered {
        let schema = server
            .database_runtime
            .schemas()
            .get(&crate::ram::schema::SchemaVid(IDCONTRACT_SCHEMA))
            .unwrap();
        rpc.neb_client
            .new_schema_with_id((*schema).clone())
            .await
            .unwrap()
            .unwrap();
    }
    let id = Id::from_parts(2, 903);
    server
        .chunks()
        .write_cell(&mut idcontract_cell(id, 10))
        .unwrap();
    (rpc, server, dir, id)
}

#[tokio::test]
async fn g23_missing_schema_full_read_returns_a_typed_error() {
    let name = "g23_full_missing";
    let (_rpc, server, _dir, id) = g23_schema_fixture(name, false).await;
    server.database_runtime.schemas().evict_test_schema(name);
    assert!(matches!(
        server.chunks().read_cell(&id),
        Err(ReadError::SchemaDoesNotExisted(IDCONTRACT_SCHEMA))
    ));
    server.shutdown().await;
}

#[tokio::test]
async fn g23_missing_schema_selected_read_returns_a_typed_error() {
    let name = "g23_selected_missing";
    let (_rpc, server, _dir, id) = g23_schema_fixture(name, false).await;
    server.database_runtime.schemas().evict_test_schema(name);
    assert!(matches!(
        server
            .chunks()
            .read_selected(&id, &[bifrost_hasher::hash_str("value")], true),
        Err(ReadError::SchemaDoesNotExisted(IDCONTRACT_SCHEMA))
    ));
    server.shutdown().await;
}

#[tokio::test]
async fn g23_reads_refresh_a_missing_schema_once() {
    let name = "g23_refresh";
    let (rpc, server, _dir, id) = g23_schema_fixture(name, true).await;
    let ids = vec![id];
    let fields = vec![bifrost_hasher::hash_str("value")];
    let empty = Expr::nothing();
    for case in 0..6 {
        server.database_runtime.schemas().evict_test_schema(name);
        let cells = match case {
            0 => vec![rpc.read_cell(id).await],
            1 => rpc.read_all_cells(&ids).await,
            2 => vec![rpc.read_cell_select(id, &fields, true).await],
            3 => rpc.read_all_cells_selected(&ids, &fields, true).await,
            4 => {
                rpc.read_all_cells_proced(&ids, &vec![], &empty, &empty)
                    .await
            }
            _ => {
                rpc.read_all_cells_proced(&ids, &fields, &empty, &empty)
                    .await
            }
        };
        assert!(cells.iter().all(Result::is_ok), "case {case}: {cells:?}");
        assert!(server
            .database_runtime
            .schemas()
            .get(&crate::ram::schema::SchemaVid(IDCONTRACT_SCHEMA))
            .is_some());
    }
    server.shutdown().await;
}

#[tokio::test]
async fn g23_refresh_absent_cluster_schema_returns_a_typed_error() {
    let name = "g23_refresh_absent";
    let (rpc, server, _dir, id) = g23_schema_fixture(name, false).await;
    server.database_runtime.schemas().evict_test_schema(name);
    assert!(matches!(
        rpc.read_cell(id).await,
        Err(ReadError::SchemaDoesNotExisted(IDCONTRACT_SCHEMA))
    ));
    server.shutdown().await;
}

#[tokio::test]
async fn g23_schema_delete_between_verification_and_projection_is_typed() {
    use crate::index::verification::IndexCheck;
    let name = "g23_projection_delete";
    let (rpc, server, _dir, id) = g23_schema_fixture(name, false).await;
    let field = bifrost_hasher::hash_str("value");
    let bucket = crate::index::hash::get_hash_id_from_value(
        crate::ram::schema::SchemaUid(IDCONTRACT_SCHEMA),
        field,
        &OwnedValue::U64(10),
    );
    SCHEMA_DELETE_AFTER_CHECK.insert(id, name.to_string());
    let result = rpc
        .read_all_cells_verified(
            &vec![id],
            &vec![field],
            &Expr::nothing(),
            &Expr::nothing(),
            &vec![IndexCheck::Hashed {
                buckets: vec![bucket],
                field,
                value: OwnedValue::U64(10),
            }],
        )
        .await;
    assert!(
        matches!(
            result.as_slice(),
            [Err(ReadError::SchemaDoesNotExisted(IDCONTRACT_SCHEMA))]
        ),
        "{result:?}"
    );
    server.shutdown().await;
}
