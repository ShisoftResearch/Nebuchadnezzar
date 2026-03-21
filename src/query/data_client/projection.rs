use bifrost::rpc::RPCError;
use dovahkiin::types::{Id, OwnedValue};

use super::{AggregateRow, IndexedDataClient};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionField {
    pub field_id: u64,
    pub alias: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectionItem {
    Field(ProjectionField),
    Aggregate {
        alias: String,
        output_name: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryRow {
    pub id: Option<Id>,
    pub columns: Vec<(String, OwnedValue)>,
}

pub struct QueryResultCursor {
    pub(super) buffer: Vec<QueryRow>,
    pub(super) pos: usize,
}

impl QueryResultCursor {
    pub async fn next(&mut self) -> Result<Option<QueryRow>, RPCError> {
        if self.buffer.len() <= self.pos {
            return Ok(None);
        }
        let row = self.buffer[self.pos].clone();
        self.pos += 1;
        Ok(Some(row))
    }
}

impl IndexedDataClient {
    pub(super) async fn project_ids_to_rows(
        &self,
        ids: &[Id],
        projection: Vec<ProjectionField>,
    ) -> Result<QueryResultCursor, RPCError> {
        if projection.is_empty() {
            return Ok(QueryResultCursor {
                buffer: ids
                    .iter()
                    .copied()
                    .map(|id| QueryRow {
                        id: Some(id),
                        columns: vec![],
                    })
                    .collect(),
                pos: 0,
            });
        }
        let field_ids = projection
            .iter()
            .map(|field| field.field_id)
            .collect::<Vec<_>>();
        let selected_cells = self.read_selected_cells_from_ids(ids, &field_ids).await;
        let mut rows = Vec::with_capacity(selected_cells.len());
        for cell in selected_cells {
            let columns = projection
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let name = field
                        .alias
                        .clone()
                        .unwrap_or_else(|| field.field_id.to_string());
                    (name, cell[index].clone())
                })
                .collect();
            rows.push(QueryRow {
                id: Some(cell.id()),
                columns,
            });
        }
        Ok(QueryResultCursor {
            buffer: rows,
            pos: 0,
        })
    }

    pub(super) fn shape_aggregate_rows(
        &self,
        rows: Vec<AggregateRow>,
        projection: Vec<ProjectionItem>,
    ) -> QueryResultCursor {
        QueryResultCursor {
            buffer: rows
                .into_iter()
                .map(|row| shape_aggregate_row(row, &projection))
                .collect(),
            pos: 0,
        }
    }
}

fn shape_aggregate_row(row: AggregateRow, projection: &[ProjectionItem]) -> QueryRow {
    let mut columns = Vec::with_capacity(projection.len());
    for item in projection {
        match item {
            ProjectionItem::Field(field) => {
                if let Some((_, value)) = row
                    .group_values
                    .iter()
                    .find(|(field_id, _)| *field_id == field.field_id)
                {
                    let name = field
                        .alias
                        .clone()
                        .unwrap_or_else(|| field.field_id.to_string());
                    columns.push((name, value.clone()));
                }
            }
            ProjectionItem::Aggregate { alias, output_name } => {
                if let Some((_, value)) = row
                    .aggregate_values
                    .iter()
                    .find(|(candidate_alias, _)| candidate_alias == alias)
                {
                    columns.push((
                        output_name.clone().unwrap_or_else(|| alias.clone()),
                        value.clone(),
                    ));
                }
            }
        }
    }
    QueryRow { id: None, columns }
}
