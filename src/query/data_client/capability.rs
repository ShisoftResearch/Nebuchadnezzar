use std::io;

use bifrost::rpc::RPCError;
use dovahkiin::expr::serde::Expr;

use crate::query::planner::{build_indexed_predicate_plan, IndexedPredicatePlan};
use crate::ram::schema::SchemaUid;

use super::{IndexedDataClient, Verify};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredicateRequirement {
    FieldOrder,
    Distinct,
    Resumable,
    Unindexed,
    Residual,
    Relevance,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdQueryCapability {
    pub predicate_requirement: Option<PredicateRequirement>,
    pub inferred_order_field: Option<u64>,
}

impl IdQueryCapability {
    pub fn verify(self) -> Verify {
        #[cfg(test)]
        if std::env::var_os("NEB_TEST_IDCONTRACT_IGNORE_SHAPE").is_some() {
            return Verify::Exists;
        }
        if self.predicate_requirement.is_none() {
            Verify::Exists
        } else {
            Verify::Predicate
        }
    }
}

impl IndexedDataClient {
    /// Inspect the same plan used by id execution before selecting verification.
    /// Metadata errors stay errors; they do not mean an unsupported or empty query.
    pub async fn id_query_capability(
        &self,
        schema: SchemaUid,
        selection: &Expr,
        order_by_field: Option<u64>,
        distinct: bool,
        resumable: bool,
        limit: Option<usize>,
    ) -> Result<IdQueryCapability, RPCError> {
        let metadata = self
            .index_clients
            .neb_client
            .schema_by_id(schema.get())
            .await
            .map_err(|error| RPCError::IOError(io::Error::other(error.to_string())))?
            .ok_or_else(|| {
                RPCError::IOError(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("schema {schema} not found"),
                ))
            })?;
        let statistics = self.index_clients.overall_schema_statistics(schema.get());
        let plan = build_indexed_predicate_plan(
            &metadata,
            selection,
            statistics.as_deref(),
            order_by_field,
            limit,
        );
        Ok(capability(
            selection,
            plan.as_ref(),
            order_by_field,
            distinct,
            resumable,
        ))
    }
}

fn capability(
    selection: &Expr,
    plan: Option<&IndexedPredicatePlan>,
    order_by_field: Option<u64>,
    distinct: bool,
    resumable: bool,
) -> IdQueryCapability {
    let inferred_order_field = if order_by_field.is_none() {
        plan.and_then(IndexedDataClient::infer_query_order_field_from_plan)
    } else {
        None
    };
    let predicate_requirement = if resumable {
        Some(PredicateRequirement::Resumable)
    } else if distinct {
        Some(PredicateRequirement::Distinct)
    } else if order_by_field.is_some() {
        Some(PredicateRequirement::FieldOrder)
    } else if selection.is_empty() {
        None
    } else if let Some(plan) = plan {
        if plan.disjuncts().iter().any(|part| {
            part.clauses()
                .iter()
                .any(|clause| clause.uses_relevance_ranking())
        }) {
            Some(PredicateRequirement::Relevance)
        } else if plan
            .disjuncts()
            .iter()
            .any(|part| !part.residual().is_empty())
        {
            Some(PredicateRequirement::Residual)
        } else {
            None
        }
    } else {
        Some(PredicateRequirement::Unindexed)
    };
    IdQueryCapability {
        predicate_requirement,
        inferred_order_field,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ram::schema::{Field, IndexType, Schema};
    use bifrost_hasher::hash_str;
    use dovahkiin::types::{OwnedValue, Type};

    fn expr(op: &str, children: Vec<Expr>) -> Expr {
        let mut parts = vec![Expr::Symbol(hash_str(op), op.into())];
        parts.extend(children);
        Expr::List(parts)
    }
    fn eq(field: &str) -> Expr {
        expr(
            "=",
            vec![
                Expr::Symbol(hash_str(field), field.into()),
                Expr::Value(OwnedValue::U64(7)),
            ],
        )
    }
    fn shape(
        selection: &Expr,
        order: Option<u64>,
        distinct: bool,
        resumable: bool,
    ) -> IdQueryCapability {
        let schema = Schema::new_with_id(
            1,
            "shape",
            None,
            Field::new_schema(vec![
                Field::new_indexed("hash", Type::U64, vec![IndexType::Hashed]),
                Field::new_indexed("range", Type::U64, vec![IndexType::Ranged]),
                Field::new_unindexed("plain", Type::U64),
            ]),
            false,
            true,
        );
        let plan = build_indexed_predicate_plan(&schema, selection, None, order, Some(10));
        let result = capability(selection, plan.as_ref(), order, distinct, resumable);
        result
    }

    #[test]
    fn idcontract_shape_selects_before_execution() {
        for selection in [
            Expr::nothing(),
            eq("hash"),
            eq("range"),
            expr("or", vec![eq("hash"), eq("range")]),
        ] {
            assert_eq!(
                shape(&selection, None, false, false).verify(),
                Verify::Exists
            );
        }
        assert_eq!(
            shape(&eq("hash"), None, false, false).inferred_order_field,
            None
        );
        assert_eq!(
            shape(&eq("range"), None, false, false).inferred_order_field,
            Some(hash_str("range"))
        );
        for selection in [
            eq("plain"),
            expr("and", vec![eq("hash"), eq("plain")]),
            expr("or", vec![eq("hash"), eq("plain")]),
            expr(
                "text-match",
                vec![
                    Expr::Symbol(hash_str("plain"), "plain".into()),
                    Expr::Value(OwnedValue::String("word".into())),
                ],
            ),
        ] {
            assert_eq!(
                shape(&selection, None, false, false).verify(),
                Verify::Predicate
            );
        }
        assert_eq!(
            shape(&eq("hash"), Some(hash_str("hash")), false, false).verify(),
            Verify::Predicate
        );
        assert_eq!(
            shape(&eq("hash"), None, true, false).verify(),
            Verify::Predicate
        );
        assert_eq!(
            shape(&eq("hash"), None, false, true).verify(),
            Verify::Predicate
        );
    }
}
