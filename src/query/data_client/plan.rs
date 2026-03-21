use std::io;

use bifrost::rpc::RPCError;
use dovahkiin::expr::serde::Expr;

use crate::query::planner::{IndexedClausePlan, IndexedPredicatePlan, build_indexed_predicate_plan};

use super::IndexedDataClient;

impl IndexedDataClient {
    pub(super) async fn indexed_predicate_plan(
        &self,
        schema_id: u32,
        selection: &Expr,
        order_by_field: Option<u64>,
        limit: Option<usize>,
    ) -> Option<IndexedPredicatePlan> {
        if selection.is_empty() {
            return None;
        }
        let schema = self
            .index_clients
            .neb_client
            .schema_by_id(schema_id)
            .await
            .ok()
            .flatten()?;
        let stats = self.index_clients.overall_schema_statistics(schema_id);
        build_indexed_predicate_plan(&schema, selection, stats.as_deref(), order_by_field, limit)
    }

    pub(super) async fn ensure_orderable_field(
        &self,
        schema_id: u32,
        field_id: u64,
    ) -> Result<(), RPCError> {
        let schema = self
            .index_clients
            .neb_client
            .schema_by_id(schema_id)
            .await
            .map_err(|e| RPCError::IOError(io::Error::new(io::ErrorKind::Other, e.to_string())))?
            .ok_or_else(|| {
                RPCError::IOError(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("schema {schema_id} not found"),
                ))
            })?;

        if schema.field_by_id_path(&[field_id]).is_none() {
            return Err(RPCError::IOError(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("ORDER BY field {field_id} does not exist in schema {schema_id}"),
            )));
        }
        Ok(())
    }

    pub(super) async fn ensure_distinct_fields(
        &self,
        schema_id: u32,
        field_ids: &[u64],
    ) -> Result<(), RPCError> {
        if field_ids.is_empty() {
            return Err(RPCError::IOError(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DISTINCT requires at least one field",
            )));
        }
        let schema = self
            .index_clients
            .neb_client
            .schema_by_id(schema_id)
            .await
            .map_err(|e| RPCError::IOError(io::Error::new(io::ErrorKind::Other, e.to_string())))?
            .ok_or_else(|| {
                RPCError::IOError(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("schema {schema_id} not found"),
                ))
            })?;
        for field_id in field_ids {
            if schema.field_by_id_path(&[*field_id]).is_none() {
                return Err(RPCError::IOError(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("DISTINCT field {field_id} does not exist in schema {schema_id}"),
                )));
            }
        }
        Ok(())
    }

    pub(super) fn infer_query_order_field_from_plan(plan: &IndexedPredicatePlan) -> Option<u64> {
        let mut field_id = None;
        for clause in plan.all() {
            let IndexedClausePlan::Ranged {
                field_id: ranged_field_id,
                ..
            } = clause
            else {
                continue;
            };
            match field_id {
                None => field_id = Some(*ranged_field_id),
                Some(existing) if existing == *ranged_field_id => {}
                Some(_) => return None,
            }
        }
        field_id
    }
}
