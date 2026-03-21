use crate::{
    query::data_client::QueryOrdering,
    ram::{
        cell::OwnedCell,
        schema::{Field, IndexType, Schema},
    },
    server::*,
};
use bifrost_hasher::hash_str;
use dovahkiin::{integrated::lisp::parse_to_serde_expr, types::*};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use rusqlite::{params, Connection};
use std::{collections::BTreeSet, fmt::Write as _, sync::Arc};

const RANGE_A: &str = "RANGE_A";
const RANGE_B: &str = "RANGE_B";
const HASH_VALUE: &str = "HASH_VALUE";
const PLAIN_VALUE: &str = "PLAIN_VALUE";
const NULLABLE_VALUE: &str = "NULLABLE_VALUE";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum AlignField {
    RangeA,
    RangeB,
    HashValue,
    PlainValue,
    NullableValue,
}

impl AlignField {
    fn neb_name(self) -> &'static str {
        match self {
            Self::RangeA => RANGE_A,
            Self::RangeB => RANGE_B,
            Self::HashValue => HASH_VALUE,
            Self::PlainValue => PLAIN_VALUE,
            Self::NullableValue => NULLABLE_VALUE,
        }
    }

    fn sql_name(self) -> &'static str {
        match self {
            Self::RangeA => "range_a",
            Self::RangeB => "range_b",
            Self::HashValue => "hash_value",
            Self::PlainValue => "plain_value",
            Self::NullableValue => "nullable_value",
        }
    }

    fn field_id(self) -> u64 {
        match self {
            Self::RangeA => hash_str(RANGE_A),
            Self::RangeB => hash_str(RANGE_B),
            Self::HashValue => hash_str(HASH_VALUE),
            Self::PlainValue => hash_str(PLAIN_VALUE),
            Self::NullableValue => hash_str(NULLABLE_VALUE),
        }
    }

    fn is_ranged(self) -> bool {
        matches!(self, Self::RangeA | Self::RangeB)
    }
}

#[derive(Clone, Copy, Debug)]
enum AlignOp {
    Eq,
    Gt,
    Ge,
    Lt,
    Le,
}

impl AlignOp {
    fn neb_name(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Gt => ">",
            Self::Ge => ">=",
            Self::Lt => "<",
            Self::Le => "<=",
        }
    }

    fn sql_name(self) -> &'static str {
        self.neb_name()
    }
}

#[derive(Clone, Copy, Debug)]
enum AlignOperand {
    Field(AlignField),
    Literal(u64),
}

#[derive(Clone, Debug)]
enum AlignPredicate {
    Cmp {
        op: AlignOp,
        left: AlignOperand,
        right: AlignOperand,
    },
    And(Vec<AlignPredicate>),
    Or(Vec<AlignPredicate>),
}

impl AlignPredicate {
    fn eq(field: AlignField, value: u64) -> Self {
        Self::Cmp {
            op: AlignOp::Eq,
            left: AlignOperand::Field(field),
            right: AlignOperand::Literal(value),
        }
    }

    fn cmp(field: AlignField, op: AlignOp, value: u64) -> Self {
        Self::Cmp {
            op,
            left: AlignOperand::Field(field),
            right: AlignOperand::Literal(value),
        }
    }

    fn cmp_reversed(field: AlignField, op: AlignOp, value: u64) -> Self {
        Self::Cmp {
            op,
            left: AlignOperand::Literal(value),
            right: AlignOperand::Field(field),
        }
    }

    fn and(parts: Vec<AlignPredicate>) -> Self {
        Self::And(parts)
    }

    fn or(parts: Vec<AlignPredicate>) -> Self {
        Self::Or(parts)
    }

    fn render_lisp(&self) -> String {
        match self {
            Self::Cmp { op, left, right } => format!(
                "({} {} {})",
                op.neb_name(),
                render_lisp_operand(*left),
                render_lisp_operand(*right)
            ),
            Self::And(parts) => format!(
                "(and {})",
                parts.iter().map(Self::render_lisp).collect::<Vec<_>>().join(" ")
            ),
            Self::Or(parts) => format!(
                "(or {})",
                parts.iter().map(Self::render_lisp).collect::<Vec<_>>().join(" ")
            ),
        }
    }

    fn render_sql(&self) -> String {
        match self {
            Self::Cmp { op, left, right } => format!(
                "({} {} {})",
                render_sql_operand(*left),
                op.sql_name(),
                render_sql_operand(*right)
            ),
            Self::And(parts) => format!(
                "({})",
                parts.iter().map(Self::render_sql).collect::<Vec<_>>().join(" AND ")
            ),
            Self::Or(parts) => format!(
                "({})",
                parts.iter().map(Self::render_sql).collect::<Vec<_>>().join(" OR ")
            ),
        }
    }

    fn inferred_order_field(&self) -> Option<AlignField> {
        let mut fields = Vec::new();
        self.collect_inferred_order_fields(&mut fields);
        let mut unique = fields.into_iter().collect::<BTreeSet<_>>().into_iter();
        let first = unique.next()?;
        if unique.next().is_some() {
            None
        } else {
            Some(first)
        }
    }

    fn collect_inferred_order_fields(&self, fields: &mut Vec<AlignField>) {
        match self {
            Self::Cmp {
                op,
                left: AlignOperand::Field(field),
                right: AlignOperand::Literal(_),
            }
            | Self::Cmp {
                op,
                left: AlignOperand::Literal(_),
                right: AlignOperand::Field(field),
            } => {
                if !matches!(op, AlignOp::Eq) && field.is_ranged() {
                    fields.push(*field);
                }
            }
            Self::Cmp { .. } => {}
            Self::And(parts) | Self::Or(parts) => {
                for part in parts {
                    part.collect_inferred_order_fields(fields);
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
struct AlignQuery {
    predicate: AlignPredicate,
    ordering: QueryOrdering,
    order_by_field: Option<AlignField>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Clone, Debug)]
struct AlignRow {
    id: Id,
    range_a: u64,
    range_b: u64,
    hash_value: u64,
    plain_value: u64,
    nullable_value: Option<u64>,
}

#[derive(Clone, Debug)]
struct AlignDataset {
    name: String,
    rows: Vec<AlignRow>,
}

#[derive(Clone, Copy, Debug)]
enum AlignSchemaMode {
    IndexedOnly,
    Scannable,
}

impl AlignSchemaMode {
    fn is_scannable(self) -> bool {
        matches!(self, Self::Scannable)
    }

    fn suffix(self) -> &'static str {
        match self {
            Self::IndexedOnly => "indexed",
            Self::Scannable => "scan",
        }
    }
}

#[derive(Clone, Debug)]
struct SqlPlan {
    sql: String,
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_alignment_fixed_indexed_corpus_matches_neb() {
    let _ = env_logger::try_init();
    let fixed_datasets = vec![edge_case_dataset(), duplicate_heavy_dataset(), interior_range_dataset()];
    let server = create_alignment_test_server(6730).await;
    run_alignment_suite(
        server,
        6730,
        fixed_datasets,
        AlignSchemaMode::IndexedOnly,
        false,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_alignment_generated_indexed_corpus_matches_neb() {
    let _ = env_logger::try_init();
    let datasets = vec![
        generated_dataset(0xA110_0001, 48),
        generated_dataset(0xA110_0002, 56),
        generated_dataset(0xA110_0003, 64),
    ];
    let server = create_alignment_test_server(6731).await;
    run_alignment_suite(
        server,
        6731,
        datasets,
        AlignSchemaMode::IndexedOnly,
        true,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_alignment_fixed_schema_scan_corpus_matches_neb() {
    let _ = env_logger::try_init();
    let fixed_datasets = vec![edge_case_dataset(), duplicate_heavy_dataset(), interior_range_dataset()];
    let server = create_alignment_test_server(6732).await;
    run_alignment_suite(server, 6732, fixed_datasets, AlignSchemaMode::Scannable, false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sqlite_alignment_generated_schema_scan_corpus_matches_neb() {
    let _ = env_logger::try_init();
    let datasets = vec![
        generated_dataset(0xA110_1001, 48),
        generated_dataset(0xA110_1002, 56),
        generated_dataset(0xA110_1003, 64),
    ];
    let server = create_alignment_test_server(6733).await;
    run_alignment_suite(server, 6733, datasets, AlignSchemaMode::Scannable, true).await;
}

async fn run_alignment_suite(
    server: Arc<NebServer>,
    port: u16,
    datasets: Vec<AlignDataset>,
    schema_mode: AlignSchemaMode,
    generated: bool,
) {
    let server_addr = format!("127.0.0.1:{port}");
    let client = server.data_client(&vec![server_addr]).await.unwrap();
    let idx_client = server.indexed_data_client();

    for (dataset_index, dataset) in datasets.into_iter().enumerate() {
        let schema_id = 40_000 + port as u32 * 10 + dataset_index as u32;
        let schema = alignment_schema(
            schema_id,
            &format!("{}_{}", dataset.name, schema_mode.suffix()),
            schema_mode.is_scannable(),
        );
        client.new_schema_with_id(schema).await.unwrap().unwrap();
        materialize_neb_dataset(&client, schema_id, &dataset.rows).await;
        let sqlite = materialize_sqlite_dataset(&dataset.rows);

        let queries = if schema_mode.is_scannable() {
            if generated {
                generated_scan_queries(&dataset)
            } else {
                fixed_scan_queries(&dataset)
            }
        } else if generated {
            generated_queries(&dataset)
        } else {
            fixed_queries(&dataset)
        };
        for query in queries {
            assert_query_alignment(&idx_client, &sqlite, schema_id, &dataset, &query).await;
        }
    }
}

fn alignment_schema(schema_id: u32, name: &str, scannable: bool) -> Schema {
    let fields = Field::new_schema(vec![
        Field::new_indexed(RANGE_A, Type::U64, vec![IndexType::Ranged]),
        Field::new_indexed(RANGE_B, Type::U64, vec![IndexType::Ranged]),
        Field::new_indexed(HASH_VALUE, Type::U64, vec![IndexType::Hashed]),
        Field::new_unindexed(PLAIN_VALUE, Type::U64),
        Field::new_unindexed_nullable(NULLABLE_VALUE, Type::U64),
    ]);
    Schema::new_with_id(schema_id, name, None, fields, false, scannable)
}

async fn materialize_neb_dataset(
    client: &crate::client::AsyncClient,
    schema_id: u32,
    rows: &[AlignRow],
) {
    for row in rows {
        let mut value = OwnedValue::Map(OwnedMap::new());
        value[RANGE_A] = OwnedValue::U64(row.range_a);
        value[RANGE_B] = OwnedValue::U64(row.range_b);
        value[HASH_VALUE] = OwnedValue::U64(row.hash_value);
        value[PLAIN_VALUE] = OwnedValue::U64(row.plain_value);
        value[NULLABLE_VALUE] = row
            .nullable_value
            .map(OwnedValue::U64)
            .unwrap_or(OwnedValue::Null);
        let cell = OwnedCell::new_with_id(schema_id, &row.id, value);
        client.write_cell(cell).await.unwrap().unwrap();
    }
}

fn materialize_sqlite_dataset(rows: &[AlignRow]) -> Connection {
    let sqlite = Connection::open_in_memory().unwrap();
    sqlite
        .execute_batch(
            "CREATE TABLE rows (
                id_higher INTEGER NOT NULL,
                id_lower INTEGER NOT NULL,
                range_a INTEGER NOT NULL,
                range_b INTEGER NOT NULL,
                hash_value INTEGER NOT NULL,
                plain_value INTEGER NOT NULL,
                nullable_value INTEGER NULL,
                PRIMARY KEY (id_higher, id_lower)
            );",
        )
        .unwrap();
    let mut insert = sqlite
        .prepare(
            "INSERT INTO rows (
                id_higher,
                id_lower,
                range_a,
                range_b,
                hash_value,
                plain_value,
                nullable_value
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )
        .unwrap();
    for row in rows {
        insert
            .execute(params![
                row.id.higher as i64,
                row.id.lower as i64,
                row.range_a as i64,
                row.range_b as i64,
                row.hash_value as i64,
                row.plain_value as i64,
                row.nullable_value.map(|value| value as i64),
            ])
            .unwrap();
    }
    drop(insert);
    sqlite
}

async fn assert_query_alignment(
    idx_client: &crate::query::data_client::IndexedDataClient,
    sqlite: &Connection,
    schema_id: u32,
    dataset: &AlignDataset,
    query: &AlignQuery,
) {
    let neb_lisp = query.predicate.render_lisp();
    let selection = parse_to_serde_expr(&neb_lisp).unwrap()[0].clone();
    let mut neb_cursor = idx_client
        .query_ids_with_options(
            schema_id,
            selection,
            query.ordering,
            query.order_by_field.map(AlignField::field_id),
            None,
            query.limit,
            query.offset,
        )
        .await
        .unwrap();
    let mut neb_ids = Vec::new();
    while let Some(id) = neb_cursor.next().await.unwrap() {
        neb_ids.push(id);
    }

    let sql_plan = render_sql_query(query);
    let sqlite_ids = run_sqlite_query(sqlite, &sql_plan);
    if neb_ids != sqlite_ids {
        panic!("{}", build_alignment_diff(dataset, query, &neb_lisp, &sql_plan.sql, &neb_ids, &sqlite_ids));
    }
}

fn render_sql_query(query: &AlignQuery) -> SqlPlan {
    let explicit_order_field = query.order_by_field;
    let inferred_order_field = query.predicate.inferred_order_field();
    let order_field = explicit_order_field.or(inferred_order_field);
    let order_clause = if let Some(field) = order_field {
        let tie_break_order = if explicit_order_field.is_some() {
            query.ordering
        } else {
            QueryOrdering::Asc
        };
        render_sql_order_by_field(field, query.ordering, tie_break_order)
    } else {
        render_sql_order_by_id(query.ordering)
    };

    let mut sql = format!(
        "SELECT id_higher, id_lower FROM rows WHERE {} ORDER BY {}",
        query.predicate.render_sql(),
        order_clause
    );
    if let Some(limit) = query.limit {
        let _ = write!(sql, " LIMIT {}", limit);
    }
    if let Some(offset) = query.offset {
        let _ = write!(sql, " OFFSET {}", offset);
    }

    SqlPlan { sql }
}

fn render_sql_order_by_field(
    field: AlignField,
    ordering: QueryOrdering,
    tie_break_order: QueryOrdering,
) -> String {
    let direction = render_sql_ordering(ordering);
    let tie_break_direction = render_sql_ordering(tie_break_order);
    if matches!(field, AlignField::NullableValue) {
        format!(
            "CASE WHEN {} IS NULL THEN 1 ELSE 0 END ASC, {} {}, id_higher {}, id_lower {}",
            field.sql_name(),
            field.sql_name(),
            direction,
            tie_break_direction,
            tie_break_direction
        )
    } else {
        format!(
            "{} {}, id_higher {}, id_lower {}",
            field.sql_name(),
            direction,
            tie_break_direction,
            tie_break_direction
        )
    }
}

fn render_sql_order_by_id(ordering: QueryOrdering) -> String {
    let direction = render_sql_ordering(ordering);
    format!("id_higher {}, id_lower {}", direction, direction)
}

fn render_sql_ordering(ordering: QueryOrdering) -> &'static str {
    match ordering {
        QueryOrdering::Asc => "ASC",
        QueryOrdering::Desc => "DESC",
    }
}

fn run_sqlite_query(sqlite: &Connection, sql_plan: &SqlPlan) -> Vec<Id> {
    let mut stmt = sqlite.prepare(&sql_plan.sql).unwrap();
    let rows = stmt
        .query_map([], |row| {
            Ok(Id::new(
                row.get::<_, i64>(0)? as u64,
                row.get::<_, i64>(1)? as u64,
            ))
        })
        .unwrap();
    rows.map(Result::unwrap).collect()
}

fn build_alignment_diff(
    dataset: &AlignDataset,
    query: &AlignQuery,
    neb_lisp: &str,
    sql: &str,
    neb_ids: &[Id],
    sqlite_ids: &[Id],
) -> String {
    let mismatch_class = classify_mismatch(query, neb_ids, sqlite_ids);
    format!(
        "sqlite alignment mismatch ({mismatch_class})\n\
         dataset={}\n\
         ordering={:?} order_by={:?} limit={:?} offset={:?}\n\
         neb_lisp={}\n\
         sqlite_sql={}\n\
         neb_ids={:?}\n\
         sqlite_ids={:?}",
        dataset.name,
        query.ordering,
        query.order_by_field,
        query.limit,
        query.offset,
        neb_lisp,
        sql,
        neb_ids,
        sqlite_ids
    )
}

fn classify_mismatch(query: &AlignQuery, neb_ids: &[Id], sqlite_ids: &[Id]) -> &'static str {
    let neb_set: BTreeSet<_> = neb_ids.iter().copied().collect();
    let sqlite_set: BTreeSet<_> = sqlite_ids.iter().copied().collect();
    if neb_set != sqlite_set {
        "filter mismatch"
    } else if query.limit.is_some() || query.offset.is_some() {
        "pagination mismatch"
    } else if matches!(query.order_by_field, Some(AlignField::NullableValue)) {
        "null-order mismatch"
    } else {
        "ordering mismatch"
    }
}

fn render_lisp_operand(operand: AlignOperand) -> String {
    match operand {
        AlignOperand::Field(field) => field.neb_name().to_string(),
        AlignOperand::Literal(value) => format!("{value}u64"),
    }
}

fn render_sql_operand(operand: AlignOperand) -> String {
    match operand {
        AlignOperand::Field(field) => field.sql_name().to_string(),
        AlignOperand::Literal(value) => value.to_string(),
    }
}

fn edge_case_dataset() -> AlignDataset {
    AlignDataset {
        name: "fixed_edge_cases".to_string(),
        rows: vec![
            AlignRow { id: Id::new(7, 1), range_a: 1, range_b: 7, hash_value: 10, plain_value: 3, nullable_value: None },
            AlignRow { id: Id::new(7, 2), range_a: 1, range_b: 4, hash_value: 10, plain_value: 9, nullable_value: Some(8) },
            AlignRow { id: Id::new(7, 3), range_a: 2, range_b: 9, hash_value: 11, plain_value: 3, nullable_value: Some(1) },
            AlignRow { id: Id::new(7, 4), range_a: 5, range_b: 2, hash_value: 12, plain_value: 6, nullable_value: None },
            AlignRow { id: Id::new(7, 5), range_a: 5, range_b: 2, hash_value: 13, plain_value: 6, nullable_value: Some(5) },
            AlignRow { id: Id::new(7, 6), range_a: 8, range_b: 8, hash_value: 13, plain_value: 1, nullable_value: Some(13) },
            AlignRow { id: Id::new(7, 7), range_a: 9, range_b: 3, hash_value: 14, plain_value: 4, nullable_value: None },
            AlignRow { id: Id::new(7, 8), range_a: 9, range_b: 5, hash_value: 15, plain_value: 4, nullable_value: Some(2) },
        ],
    }
}

fn duplicate_heavy_dataset() -> AlignDataset {
    AlignDataset {
        name: "fixed_duplicate_heavy".to_string(),
        rows: vec![
            AlignRow { id: Id::new(8, 10), range_a: 3, range_b: 4, hash_value: 20, plain_value: 1, nullable_value: None },
            AlignRow { id: Id::new(8, 11), range_a: 3, range_b: 4, hash_value: 20, plain_value: 1, nullable_value: Some(7) },
            AlignRow { id: Id::new(8, 12), range_a: 3, range_b: 4, hash_value: 21, plain_value: 5, nullable_value: None },
            AlignRow { id: Id::new(8, 13), range_a: 6, range_b: 4, hash_value: 22, plain_value: 5, nullable_value: Some(9) },
            AlignRow { id: Id::new(8, 14), range_a: 6, range_b: 7, hash_value: 23, plain_value: 5, nullable_value: None },
            AlignRow { id: Id::new(8, 15), range_a: 6, range_b: 7, hash_value: 23, plain_value: 6, nullable_value: Some(3) },
            AlignRow { id: Id::new(8, 16), range_a: 8, range_b: 9, hash_value: 24, plain_value: 6, nullable_value: Some(12) },
        ],
    }
}

fn interior_range_dataset() -> AlignDataset {
    let rows = (0..96u64)
        .map(|i| AlignRow {
            id: Id::new(9, i),
            range_a: i,
            range_b: (i * 7) % 23,
            hash_value: (i % 9) + 1,
            plain_value: (i * 3) % 11,
            nullable_value: if i % 4 == 0 { None } else { Some((i * 5) % 17) },
        })
        .collect();
    AlignDataset {
        name: "fixed_interior_range".to_string(),
        rows,
    }
}

fn generated_dataset(seed: u64, row_count: usize) -> AlignDataset {
    let mut rng = SmallRng::seed_from_u64(seed);
    let rows = (0..row_count)
        .map(|i| AlignRow {
            id: Id::new(seed, i as u64),
            range_a: rng.gen_range(0..24),
            range_b: rng.gen_range(0..32),
            hash_value: rng.gen_range(1..13),
            plain_value: rng.gen_range(0..10),
            nullable_value: if rng.gen_ratio(1, 3) {
                None
            } else {
                Some(rng.gen_range(0..18))
            },
        })
        .collect();
    AlignDataset {
        name: format!("generated_seed_{seed}"),
        rows,
    }
}

fn fixed_queries(dataset: &AlignDataset) -> Vec<AlignQuery> {
    let mid = dataset.rows[dataset.rows.len() / 2].clone();
    let tail = dataset.rows[dataset.rows.len() - 1].clone();
    vec![
        AlignQuery {
            predicate: AlignPredicate::eq(AlignField::HashValue, dataset.rows[0].hash_value),
            ordering: QueryOrdering::Asc,
            order_by_field: None,
            limit: None,
            offset: None,
        },
        AlignQuery {
            predicate: AlignPredicate::cmp(AlignField::RangeA, AlignOp::Ge, mid.range_a),
            ordering: QueryOrdering::Desc,
            order_by_field: None,
            limit: Some(5),
            offset: None,
        },
        AlignQuery {
            predicate: AlignPredicate::cmp_reversed(AlignField::RangeA, AlignOp::Lt, tail.range_a),
            ordering: QueryOrdering::Asc,
            order_by_field: None,
            limit: Some(7),
            offset: Some(2),
        },
        AlignQuery {
            predicate: AlignPredicate::and(vec![
                AlignPredicate::cmp(AlignField::RangeA, AlignOp::Ge, dataset.rows[1].range_a),
                AlignPredicate::eq(AlignField::PlainValue, dataset.rows[1].plain_value),
            ]),
            ordering: QueryOrdering::Asc,
            order_by_field: Some(AlignField::RangeB),
            limit: None,
            offset: None,
        },
        AlignQuery {
            predicate: AlignPredicate::or(vec![
                AlignPredicate::eq(AlignField::HashValue, dataset.rows[2].hash_value),
                AlignPredicate::eq(AlignField::HashValue, dataset.rows[3].hash_value),
            ]),
            ordering: QueryOrdering::Desc,
            order_by_field: Some(AlignField::RangeA),
            limit: Some(4),
            offset: None,
        },
        AlignQuery {
            predicate: AlignPredicate::and(vec![
                AlignPredicate::or(vec![
                    AlignPredicate::eq(AlignField::HashValue, dataset.rows[0].hash_value),
                    AlignPredicate::eq(AlignField::HashValue, tail.hash_value),
                ]),
                AlignPredicate::cmp(AlignField::RangeB, AlignOp::Gt, dataset.rows[0].range_b.saturating_sub(1)),
            ]),
            ordering: QueryOrdering::Desc,
            order_by_field: None,
            limit: Some(6),
            offset: Some(1),
        },
        AlignQuery {
            predicate: AlignPredicate::and(vec![
                AlignPredicate::cmp(AlignField::RangeA, AlignOp::Ge, mid.range_a.saturating_sub(1)),
                AlignPredicate::cmp(AlignField::RangeA, AlignOp::Le, tail.range_a),
                AlignPredicate::eq(AlignField::PlainValue, mid.plain_value),
            ]),
            ordering: QueryOrdering::Asc,
            order_by_field: None,
            limit: None,
            offset: None,
        },
        AlignQuery {
            predicate: AlignPredicate::cmp(AlignField::RangeA, AlignOp::Ge, dataset.rows[0].range_a),
            ordering: QueryOrdering::Asc,
            order_by_field: Some(AlignField::PlainValue),
            limit: Some(8),
            offset: None,
        },
        AlignQuery {
            predicate: AlignPredicate::cmp(AlignField::RangeB, AlignOp::Ge, dataset.rows[0].range_b.min(mid.range_b)),
            ordering: QueryOrdering::Desc,
            order_by_field: Some(AlignField::NullableValue),
            limit: Some(7),
            offset: Some(1),
        },
        AlignQuery {
            predicate: AlignPredicate::cmp(AlignField::RangeA, AlignOp::Ge, dataset.rows[0].range_a),
            ordering: QueryOrdering::Desc,
            order_by_field: Some(AlignField::HashValue),
            limit: Some(6),
            offset: None,
        },
    ]
}

fn generated_queries(dataset: &AlignDataset) -> Vec<AlignQuery> {
    let rows = &dataset.rows;
    let first = &rows[0];
    let quarter = &rows[rows.len() / 4];
    let half = &rows[rows.len() / 2];
    let three_quarters = &rows[(rows.len() * 3) / 4];
    let last = &rows[rows.len() - 1];

    vec![
        AlignQuery {
            predicate: AlignPredicate::eq(AlignField::HashValue, first.hash_value),
            ordering: QueryOrdering::Asc,
            order_by_field: None,
            limit: None,
            offset: None,
        },
        AlignQuery {
            predicate: AlignPredicate::cmp(AlignField::RangeA, AlignOp::Ge, quarter.range_a),
            ordering: QueryOrdering::Asc,
            order_by_field: None,
            limit: Some(8),
            offset: None,
        },
        AlignQuery {
            predicate: AlignPredicate::cmp(AlignField::RangeA, AlignOp::Ge, half.range_a),
            ordering: QueryOrdering::Desc,
            order_by_field: None,
            limit: Some(8),
            offset: None,
        },
        AlignQuery {
            predicate: AlignPredicate::cmp_reversed(AlignField::RangeB, AlignOp::Lt, three_quarters.range_b.saturating_add(1)),
            ordering: QueryOrdering::Asc,
            order_by_field: None,
            limit: Some(10),
            offset: Some(2),
        },
        AlignQuery {
            predicate: AlignPredicate::and(vec![
                AlignPredicate::cmp(AlignField::RangeA, AlignOp::Ge, first.range_a.min(half.range_a)),
                AlignPredicate::cmp(AlignField::RangeA, AlignOp::Le, last.range_a.max(half.range_a)),
                AlignPredicate::eq(AlignField::PlainValue, half.plain_value),
            ]),
            ordering: QueryOrdering::Asc,
            order_by_field: None,
            limit: None,
            offset: None,
        },
        AlignQuery {
            predicate: AlignPredicate::and(vec![
                AlignPredicate::cmp(AlignField::RangeB, AlignOp::Gt, quarter.range_b.saturating_sub(1)),
                AlignPredicate::eq(AlignField::PlainValue, three_quarters.plain_value),
            ]),
            ordering: QueryOrdering::Desc,
            order_by_field: Some(AlignField::RangeA),
            limit: Some(7),
            offset: None,
        },
        AlignQuery {
            predicate: AlignPredicate::or(vec![
                AlignPredicate::eq(AlignField::HashValue, quarter.hash_value),
                AlignPredicate::eq(AlignField::HashValue, last.hash_value),
            ]),
            ordering: QueryOrdering::Desc,
            order_by_field: Some(AlignField::RangeB),
            limit: Some(9),
            offset: Some(1),
        },
        AlignQuery {
            predicate: AlignPredicate::and(vec![
                AlignPredicate::or(vec![
                    AlignPredicate::eq(AlignField::HashValue, first.hash_value),
                    AlignPredicate::eq(AlignField::HashValue, half.hash_value),
                ]),
                AlignPredicate::cmp(AlignField::RangeA, AlignOp::Ge, quarter.range_a),
            ]),
            ordering: QueryOrdering::Desc,
            order_by_field: None,
            limit: Some(6),
            offset: Some(1),
        },
        AlignQuery {
            predicate: AlignPredicate::and(vec![
                AlignPredicate::eq(AlignField::HashValue, last.hash_value),
                AlignPredicate::cmp(AlignField::PlainValue, AlignOp::Ge, 0),
            ]),
            ordering: QueryOrdering::Asc,
            order_by_field: Some(AlignField::RangeA),
            limit: Some(5),
            offset: None,
        },
        AlignQuery {
            predicate: AlignPredicate::cmp(AlignField::RangeA, AlignOp::Ge, first.range_a.min(quarter.range_a)),
            ordering: QueryOrdering::Asc,
            order_by_field: Some(AlignField::PlainValue),
            limit: Some(10),
            offset: Some(1),
        },
        AlignQuery {
            predicate: AlignPredicate::cmp(AlignField::RangeB, AlignOp::Ge, quarter.range_b.min(half.range_b)),
            ordering: QueryOrdering::Desc,
            order_by_field: Some(AlignField::NullableValue),
            limit: Some(9),
            offset: None,
        },
        AlignQuery {
            predicate: AlignPredicate::cmp(AlignField::RangeA, AlignOp::Ge, first.range_a),
            ordering: QueryOrdering::Desc,
            order_by_field: Some(AlignField::HashValue),
            limit: Some(11),
            offset: Some(2),
        },
    ]
}

fn fixed_scan_queries(dataset: &AlignDataset) -> Vec<AlignQuery> {
    let first = &dataset.rows[0];
    let mid = &dataset.rows[dataset.rows.len() / 2];
    let last = &dataset.rows[dataset.rows.len() - 1];
    vec![
        AlignQuery {
            predicate: AlignPredicate::eq(AlignField::PlainValue, first.plain_value),
            ordering: QueryOrdering::Asc,
            order_by_field: None,
            limit: None,
            offset: None,
        },
        AlignQuery {
            predicate: AlignPredicate::or(vec![
                AlignPredicate::eq(AlignField::PlainValue, first.plain_value),
                AlignPredicate::eq(AlignField::PlainValue, mid.plain_value),
            ]),
            ordering: QueryOrdering::Desc,
            order_by_field: None,
            limit: Some(6),
            offset: None,
        },
        AlignQuery {
            predicate: AlignPredicate::eq(AlignField::PlainValue, last.plain_value),
            ordering: QueryOrdering::Asc,
            order_by_field: None,
            limit: Some(4),
            offset: Some(1),
        },
        AlignQuery {
            predicate: AlignPredicate::cmp(AlignField::PlainValue, AlignOp::Ge, first.plain_value.min(mid.plain_value)),
            ordering: QueryOrdering::Desc,
            order_by_field: Some(AlignField::PlainValue),
            limit: Some(8),
            offset: Some(1),
        },
        AlignQuery {
            predicate: AlignPredicate::cmp(AlignField::PlainValue, AlignOp::Ge, first.plain_value.min(last.plain_value)),
            ordering: QueryOrdering::Asc,
            order_by_field: Some(AlignField::NullableValue),
            limit: Some(7),
            offset: None,
        },
        AlignQuery {
            predicate: AlignPredicate::or(vec![
                AlignPredicate::eq(AlignField::PlainValue, first.plain_value),
                AlignPredicate::eq(AlignField::PlainValue, last.plain_value),
            ]),
            ordering: QueryOrdering::Desc,
            order_by_field: Some(AlignField::HashValue),
            limit: Some(6),
            offset: None,
        },
    ]
}

fn generated_scan_queries(dataset: &AlignDataset) -> Vec<AlignQuery> {
    let first = &dataset.rows[0];
    let quarter = &dataset.rows[dataset.rows.len() / 4];
    let half = &dataset.rows[dataset.rows.len() / 2];
    let last = &dataset.rows[dataset.rows.len() - 1];
    vec![
        AlignQuery {
            predicate: AlignPredicate::eq(AlignField::PlainValue, first.plain_value),
            ordering: QueryOrdering::Asc,
            order_by_field: None,
            limit: Some(10),
            offset: None,
        },
        AlignQuery {
            predicate: AlignPredicate::or(vec![
                AlignPredicate::eq(AlignField::PlainValue, quarter.plain_value),
                AlignPredicate::eq(AlignField::PlainValue, half.plain_value),
            ]),
            ordering: QueryOrdering::Desc,
            order_by_field: None,
            limit: Some(12),
            offset: Some(1),
        },
        AlignQuery {
            predicate: AlignPredicate::eq(AlignField::PlainValue, last.plain_value),
            ordering: QueryOrdering::Asc,
            order_by_field: None,
            limit: None,
            offset: None,
        },
        AlignQuery {
            predicate: AlignPredicate::cmp(AlignField::PlainValue, AlignOp::Ge, first.plain_value.min(quarter.plain_value)),
            ordering: QueryOrdering::Asc,
            order_by_field: Some(AlignField::PlainValue),
            limit: Some(10),
            offset: Some(2),
        },
        AlignQuery {
            predicate: AlignPredicate::cmp(AlignField::PlainValue, AlignOp::Ge, quarter.plain_value.min(half.plain_value)),
            ordering: QueryOrdering::Desc,
            order_by_field: Some(AlignField::NullableValue),
            limit: Some(9),
            offset: Some(1),
        },
        AlignQuery {
            predicate: AlignPredicate::or(vec![
                AlignPredicate::eq(AlignField::PlainValue, first.plain_value),
                AlignPredicate::eq(AlignField::PlainValue, last.plain_value),
            ]),
            ordering: QueryOrdering::Desc,
            order_by_field: Some(AlignField::HashValue),
            limit: Some(8),
            offset: None,
        },
    ]
}

async fn create_alignment_test_server(port: u16) -> Arc<NebServer> {
    let server_addr = format!("127.0.0.1:{port}");
    let server_group = format!("sqlite_alignment_test_{port}");
    NebServer::new_from_opts(
        &ServerOptions {
            chunk_count: 8,
            total_size: 512 * 1024 * 1024,
            tiered_config: None,
            backup_storage: None,
            wal_storage: None,
            undo_log_storage: None,
            raft_storage: None,
            index_enabled: true,
            services: vec![Service::Cell, Service::Query],
            enable_recovery: false,
        },
        &server_addr,
        &server_group,
        async |_| {},
    )
    .await
}
