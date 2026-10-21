const IDCONTRACT_SCHEMA: u32 = 59217;

async fn idcontract_fixture(
    name: &str,
) -> (
    Arc<NebRPCService>,
    Arc<crate::server::NebServer>,
    tempfile::TempDir,
) {
    use crate::ram::schema::{Field, IndexType, Schema};
    use crate::ram::types::Type;
    use crate::server::{NebServer, ServerOptions};
    let dir = tempfile::TempDir::new().unwrap();
    let address = crate::utils::test_port::unique_localhost_addr();
    let server = NebServer::new_from_opts(
        &ServerOptions {
            chunk_size: 64 * 1024 * 1024,
            db_size: 64 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            raft_storage: Some(dir.path().to_string_lossy().into_owned()),
            services: vec![crate::server::Service::Cell],
            index_enabled: false,
            enable_recovery: false,
            disable_storage_locks: true,
        },
        &address,
        &name.to_string(),
        async |_| {},
    )
    .await
    .unwrap();
    let runtime = server.current_database();
    runtime.schemas().debug_only_new_schema(Schema::new_with_id(
        IDCONTRACT_SCHEMA,
        name,
        None,
        Field::new_schema(vec![Field::new_indexed(
            "value",
            Type::U64,
            vec![IndexType::Hashed],
        )]),
        false,
        false,
    ));
    let rpc = NebRPCService::new(runtime.clone(), runtime.neb_client.clone());
    (rpc, server, dir)
}

fn idcontract_cell(id: Id, value: u64) -> OwnedCell {
    use crate::ram::schema::SchemaVid;
    use dovahkiin::types::{Map, OwnedMap};
    let mut data = OwnedValue::Map(OwnedMap::new());
    data["value"] = OwnedValue::U64(value);
    OwnedCell::new_with_id(SchemaVid(IDCONTRACT_SCHEMA), &id, data)
}

#[tokio::test]
async fn idcontract_presence_uses_only_the_cell_word() {
    let (rpc, server, _dir) = idcontract_fixture("idcontract_word").await;
    let id = Id::from_parts(2, 901);
    let missing = Id::from_parts(2, 902);
    let chunks = server.chunks();
    chunks.write_cell(&mut idcontract_cell(id, 10)).unwrap();
    let chunk = &chunks.list[0];
    let hash = id.bits();
    // An invalid nonzero address proves that existence never accesses a segment.
    let original = {
        let mut word = chunk.cell_index.lock(hash as usize).unwrap();
        let original = *word;
        *word = 1;
        original
    };
    let present = rpc.existing_ids(&vec![id, missing, id]).await;
    *chunk.cell_index.lock(hash as usize).unwrap() = original;
    assert!(matches!(
        present.as_slice(),
        [Ok(true), Ok(false), Ok(true)]
    ));
    chunks.remove_cell(&id).unwrap();
    assert!(matches!(
        rpc.existing_ids(&vec![id]).await.as_slice(),
        [Ok(false)]
    ));
    server.shutdown().await;
}
