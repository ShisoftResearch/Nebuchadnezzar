use std::{cmp::Ordering as CmpOrdering, collections::HashSet, io};

use bifrost::rpc::RPCError;
use dovahkiin::types::{OwnedValue, Type};
use itertools::Itertools;

use super::{
    AggregateFunction, AggregateOrderBy, AggregateOrderTarget, AggregateQuery, AggregateRow,
    IndexedDataClient, QueryOrdering,
};
use crate::ram::schema::SchemaUid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AggregateValueType {
    Signed,
    Unsigned,
    Float,
    Scalar,
}

#[derive(Clone, Debug)]
pub(super) struct ValidatedAggregateSpec {
    pub(super) spec: super::AggregateSpec,
    pub(super) value_type: Option<AggregateValueType>,
}

#[derive(Clone, Debug)]
enum AggregateState {
    CountStar {
        count: u64,
    },
    CountField {
        count: u64,
    },
    Sum {
        sum: NumericAccum,
        value_type: AggregateValueType,
        seen: bool,
    },
    Avg {
        sum: f64,
        count: u64,
    },
    Min {
        value: Option<OwnedValue>,
    },
    Max {
        value: Option<OwnedValue>,
    },
}

#[derive(Clone, Copy, Debug)]
enum NumericAccum {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
}

#[derive(Clone, Debug)]
pub(super) struct AggregateGroupState {
    group_values: Vec<OwnedValue>,
    states: Vec<AggregateState>,
}

impl IndexedDataClient {
    pub(super) async fn validate_aggregate_query(
        &self,
        schema_id: SchemaUid,
        query: &AggregateQuery,
    ) -> Result<Vec<ValidatedAggregateSpec>, RPCError> {
        if query.aggregates.is_empty() {
            return Err(RPCError::IOError(io::Error::new(
                io::ErrorKind::InvalidInput,
                "aggregate query requires at least one aggregate",
            )));
        }

        let schema = self
            .index_clients
            .neb_client
            .schema_by_id(schema_id.get())
            .await
            .map_err(|e| RPCError::IOError(io::Error::new(io::ErrorKind::Other, e.to_string())))?
            .ok_or_else(|| {
                RPCError::IOError(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("schema {schema_id} not found"),
                ))
            })?;

        for field_id in &query.group_by_fields {
            let field = schema.field_by_id_path(&[*field_id]).ok_or_else(|| {
                RPCError::IOError(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("GROUP BY field {field_id} does not exist in schema {schema_id}"),
                ))
            })?;
            if field.is_array || matches!(field.data_type, Type::Map | Type::Null | Type::NA) {
                return Err(RPCError::IOError(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("GROUP BY field {field_id} must be a scalar field"),
                )));
            }
        }

        let mut validated = Vec::with_capacity(query.aggregates.len());
        let mut aliases = HashSet::new();
        for aggregate in &query.aggregates {
            if !aliases.insert(aggregate.alias.clone()) {
                return Err(RPCError::IOError(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("duplicate aggregate alias {:?}", aggregate.alias),
                )));
            }
            let value_type = match aggregate.func {
                AggregateFunction::CountStar => {
                    if aggregate.field_id.is_some() {
                        return Err(RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "COUNT(*) must not specify a field",
                        )));
                    }
                    None
                }
                AggregateFunction::CountField => {
                    let field_id = aggregate.field_id.ok_or_else(|| {
                        RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "COUNT(field) requires a field",
                        ))
                    })?;
                    schema.field_by_id_path(&[field_id]).ok_or_else(|| {
                        RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "aggregate field {field_id} does not exist in schema {schema_id}"
                            ),
                        ))
                    })?;
                    None
                }
                AggregateFunction::Sum | AggregateFunction::Avg => {
                    let field_id = aggregate.field_id.ok_or_else(|| {
                        RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("{:?} requires a field", aggregate.func),
                        ))
                    })?;
                    let field = schema.field_by_id_path(&[field_id]).ok_or_else(|| {
                        RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "aggregate field {field_id} does not exist in schema {schema_id}"
                            ),
                        ))
                    })?;
                    Some(numeric_value_type(field).ok_or_else(|| {
                        RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "aggregate field {field_id} must be numeric for {:?}",
                                aggregate.func
                            ),
                        ))
                    })?)
                }
                AggregateFunction::Min | AggregateFunction::Max => {
                    let field_id = aggregate.field_id.ok_or_else(|| {
                        RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("{:?} requires a field", aggregate.func),
                        ))
                    })?;
                    let field = schema.field_by_id_path(&[field_id]).ok_or_else(|| {
                        RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "aggregate field {field_id} does not exist in schema {schema_id}"
                            ),
                        ))
                    })?;
                    if field.is_array
                        || matches!(field.data_type, Type::Map | Type::Null | Type::NA)
                    {
                        return Err(RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "aggregate field {field_id} must be scalar for {:?}",
                                aggregate.func
                            ),
                        )));
                    }
                    Some(AggregateValueType::Scalar)
                }
            };
            validated.push(ValidatedAggregateSpec {
                spec: aggregate.clone(),
                value_type,
            });
        }

        if let Some(order_by) = query.order_by.as_ref() {
            match &order_by.target {
                AggregateOrderTarget::GroupField(field_id) => {
                    if !query.group_by_fields.contains(field_id) {
                        return Err(RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("ORDER BY group field {field_id} is not present in GROUP BY"),
                        )));
                    }
                }
                AggregateOrderTarget::AggregateAlias(alias) => {
                    if !aliases.contains(alias) {
                        return Err(RPCError::IOError(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("ORDER BY aggregate alias {:?} does not exist", alias),
                        )));
                    }
                }
            }
        }

        Ok(validated)
    }
}

impl AggregateGroupState {
    pub(super) fn new(group_values: &[OwnedValue], specs: &[ValidatedAggregateSpec]) -> Self {
        Self {
            group_values: group_values.to_vec(),
            states: specs.iter().map(AggregateState::from_spec).collect_vec(),
        }
    }

    pub(super) fn finalize(
        self,
        group_by_fields: &[u64],
        specs: &[ValidatedAggregateSpec],
    ) -> AggregateRow {
        AggregateRow {
            group_values: group_by_fields
                .iter()
                .copied()
                .zip(self.group_values)
                .collect_vec(),
            aggregate_values: self
                .states
                .into_iter()
                .zip(specs.iter())
                .map(|(state, spec)| (spec.spec.alias.clone(), state.finalize()))
                .collect_vec(),
        }
    }

    pub(super) fn accumulate_row(
        &mut self,
        row: &[OwnedValue],
        field_positions: &dovahkiin::ahash::HashMap<u64, usize>,
        specs: &[ValidatedAggregateSpec],
    ) {
        for (state, spec) in self.states.iter_mut().zip(specs.iter()) {
            let value = spec.spec.field_id.and_then(|field_id| {
                field_positions
                    .get(&field_id)
                    .and_then(|index| row.get(*index))
            });
            state.accumulate(value);
        }
    }
}

impl AggregateState {
    fn from_spec(spec: &ValidatedAggregateSpec) -> Self {
        match spec.spec.func {
            AggregateFunction::CountStar => Self::CountStar { count: 0 },
            AggregateFunction::CountField => Self::CountField { count: 0 },
            AggregateFunction::Sum => Self::Sum {
                sum: NumericAccum::zero(spec.value_type.expect("SUM value type validated")),
                value_type: spec.value_type.expect("SUM value type validated"),
                seen: false,
            },
            AggregateFunction::Avg => Self::Avg { sum: 0.0, count: 0 },
            AggregateFunction::Min => Self::Min { value: None },
            AggregateFunction::Max => Self::Max { value: None },
        }
    }

    pub(super) fn accumulate(&mut self, value: Option<&OwnedValue>) {
        match self {
            AggregateState::CountStar { count } => *count += 1,
            AggregateState::CountField { count } => {
                if value.is_some_and(|value| !is_null_like(value)) {
                    *count += 1;
                }
            }
            AggregateState::Sum {
                sum,
                value_type,
                seen,
            } => {
                if let Some(value) = value {
                    if is_null_like(value) {
                        return;
                    }
                    if sum.add(value_type, value) {
                        *seen = true;
                    }
                }
            }
            AggregateState::Avg { sum, count } => {
                if let Some(value) = value {
                    if is_null_like(value) {
                        return;
                    }
                    if let Some(v) = owned_value_to_f64(value) {
                        *sum += v;
                        *count += 1;
                    }
                }
            }
            AggregateState::Min { value: current } => {
                if let Some(value) = value {
                    if is_null_like(value) {
                        return;
                    }
                    if current
                        .as_ref()
                        .map(|existing| compare_owned_values(value, existing, QueryOrdering::Asc))
                        .unwrap_or(CmpOrdering::Less)
                        == CmpOrdering::Less
                    {
                        *current = Some(value.clone());
                    }
                }
            }
            AggregateState::Max { value: current } => {
                if let Some(value) = value {
                    if is_null_like(value) {
                        return;
                    }
                    if current
                        .as_ref()
                        .map(|existing| compare_owned_values(value, existing, QueryOrdering::Desc))
                        .unwrap_or(CmpOrdering::Less)
                        == CmpOrdering::Less
                    {
                        *current = Some(value.clone());
                    }
                }
            }
        }
    }

    fn finalize(self) -> OwnedValue {
        match self {
            AggregateState::CountStar { count } | AggregateState::CountField { count } => {
                OwnedValue::U64(count)
            }
            AggregateState::Sum { sum, seen, .. } => {
                if seen {
                    sum.into_owned_value()
                } else {
                    OwnedValue::Null
                }
            }
            AggregateState::Avg { sum, count } => {
                if count == 0 {
                    OwnedValue::Null
                } else {
                    OwnedValue::F64(sum / count as f64)
                }
            }
            AggregateState::Min { value } | AggregateState::Max { value } => {
                value.unwrap_or(OwnedValue::Null)
            }
        }
    }
}

impl NumericAccum {
    fn zero(value_type: AggregateValueType) -> Self {
        match value_type {
            AggregateValueType::Signed => Self::Signed(0),
            AggregateValueType::Unsigned => Self::Unsigned(0),
            AggregateValueType::Float => Self::Float(0.0),
            AggregateValueType::Scalar => unreachable!("scalar accumulator is not numeric"),
        }
    }

    fn add(&mut self, value_type: &AggregateValueType, value: &OwnedValue) -> bool {
        match (self, value_type) {
            (NumericAccum::Signed(sum), AggregateValueType::Signed) => {
                if let Some(value) = owned_value_to_i64(value) {
                    *sum = sum.saturating_add(value);
                    true
                } else {
                    false
                }
            }
            (NumericAccum::Unsigned(sum), AggregateValueType::Unsigned) => {
                if let Some(value) = owned_value_to_u64(value) {
                    *sum = sum.saturating_add(value);
                    true
                } else {
                    false
                }
            }
            (NumericAccum::Float(sum), AggregateValueType::Float) => {
                if let Some(value) = owned_value_to_f64(value) {
                    *sum += value;
                    true
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    fn into_owned_value(self) -> OwnedValue {
        match self {
            NumericAccum::Signed(sum) => OwnedValue::I64(sum),
            NumericAccum::Unsigned(sum) => OwnedValue::U64(sum),
            NumericAccum::Float(sum) => OwnedValue::F64(sum),
        }
    }
}

pub(super) fn collect_aggregate_required_fields(
    group_by_fields: &[u64],
    aggregates: &[ValidatedAggregateSpec],
) -> Vec<u64> {
    let mut fields = Vec::with_capacity(group_by_fields.len() + aggregates.len());
    for field_id in group_by_fields {
        if !fields.contains(field_id) {
            fields.push(*field_id);
        }
    }
    for aggregate in aggregates {
        if let Some(field_id) = aggregate.spec.field_id {
            if !fields.contains(&field_id) {
                fields.push(field_id);
            }
        }
    }
    fields
}

pub(super) fn serialize_group_key(values: &[OwnedValue]) -> String {
    serde_json::to_string(values).unwrap_or_else(|_| format!("{values:?}"))
}

pub(super) fn sort_aggregate_rows(rows: &mut [AggregateRow], order_by: &AggregateOrderBy) {
    rows.sort_unstable_by(|left, right| {
        let left_value = aggregate_row_value(left, &order_by.target);
        let right_value = aggregate_row_value(right, &order_by.target);
        compare_optional_owned_values(left_value, right_value, order_by.ordering)
            .then_with(|| compare_aggregate_group_values(left, right))
    });
}

fn compare_aggregate_group_values(left: &AggregateRow, right: &AggregateRow) -> CmpOrdering {
    for ((_, left_value), (_, right_value)) in
        left.group_values.iter().zip(right.group_values.iter())
    {
        let ordering = compare_owned_values(left_value, right_value, QueryOrdering::Asc);
        if ordering != CmpOrdering::Equal {
            return ordering;
        }
    }
    left.group_values.len().cmp(&right.group_values.len())
}

fn aggregate_row_value<'a>(
    row: &'a AggregateRow,
    target: &AggregateOrderTarget,
) -> Option<&'a OwnedValue> {
    match target {
        AggregateOrderTarget::GroupField(field_id) => {
            row.group_values
                .iter()
                .find_map(|(candidate_field_id, value)| {
                    (*candidate_field_id == *field_id).then_some(value)
                })
        }
        AggregateOrderTarget::AggregateAlias(alias) => row
            .aggregate_values
            .iter()
            .find_map(|(candidate_alias, value)| (candidate_alias == alias).then_some(value)),
    }
}

fn compare_optional_owned_values(
    left: Option<&OwnedValue>,
    right: Option<&OwnedValue>,
    ordering: QueryOrdering,
) -> CmpOrdering {
    match (left, right) {
        (Some(left), Some(right)) => compare_owned_values(left, right, ordering),
        (Some(_), None) => CmpOrdering::Less,
        (None, Some(_)) => CmpOrdering::Greater,
        (None, None) => CmpOrdering::Equal,
    }
}

fn compare_owned_values(
    left: &OwnedValue,
    right: &OwnedValue,
    ordering: QueryOrdering,
) -> CmpOrdering {
    let cmp = left.partial_cmp(right).unwrap_or(CmpOrdering::Equal);
    match ordering {
        QueryOrdering::Asc => cmp,
        QueryOrdering::Desc => cmp.reverse(),
    }
}

fn is_null_like(value: &OwnedValue) -> bool {
    matches!(value, OwnedValue::Null | OwnedValue::NA)
}

fn numeric_value_type(field: &crate::ram::schema::Field) -> Option<AggregateValueType> {
    if field.is_array {
        return None;
    }
    match field.data_type {
        Type::I8 | Type::I16 | Type::I32 | Type::I64 => Some(AggregateValueType::Signed),
        Type::U8 | Type::U16 | Type::U32 | Type::U64 => Some(AggregateValueType::Unsigned),
        Type::F32 | Type::F64 => Some(AggregateValueType::Float),
        _ => None,
    }
}

fn owned_value_to_i64(value: &OwnedValue) -> Option<i64> {
    match value {
        OwnedValue::I8(value) => Some(*value as i64),
        OwnedValue::I16(value) => Some(*value as i64),
        OwnedValue::I32(value) => Some(*value as i64),
        OwnedValue::I64(value) => Some(*value),
        _ => None,
    }
}

fn owned_value_to_u64(value: &OwnedValue) -> Option<u64> {
    match value {
        OwnedValue::U8(value) => Some(*value as u64),
        OwnedValue::U16(value) => Some(*value as u64),
        OwnedValue::U32(value) => Some(*value as u64),
        OwnedValue::U64(value) => Some(*value),
        _ => None,
    }
}

fn owned_value_to_f64(value: &OwnedValue) -> Option<f64> {
    match value {
        OwnedValue::I8(value) => Some(*value as f64),
        OwnedValue::I16(value) => Some(*value as f64),
        OwnedValue::I32(value) => Some(*value as f64),
        OwnedValue::I64(value) => Some(*value as f64),
        OwnedValue::U8(value) => Some(*value as f64),
        OwnedValue::U16(value) => Some(*value as f64),
        OwnedValue::U32(value) => Some(*value as f64),
        OwnedValue::U64(value) => Some(*value as f64),
        OwnedValue::F32(value) => Some(*value as f64),
        OwnedValue::F64(value) => Some(*value),
        _ => None,
    }
}
