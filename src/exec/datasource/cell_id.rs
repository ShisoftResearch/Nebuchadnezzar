use std::{cell::{RefCell, UnsafeCell}, pin::Pin, task::{Context, Poll}};
use dovahkiin::types::Id;
use futures::{Future, FutureExt, Stream};

use crate::{index::{ranged::lsm::btree::Ordering, IndexerClients}, query::data_client::ValueRange};
use crate::index::ranged::client::cursor::ClientCursor;

use super::DataSource;

const BUFFER_SIZE: u16 = 64;

pub struct CellIds {
    cursor: UnsafeCell<ClientCursor>, // Resolve lifetime problem
    ongoing: RefCell<Option<Pin<Box<dyn Future<Output = Option<Id>>>>>>
}

pub struct CellIdQuery {
    schema: u32,
    field: u64,
    range: ValueRange,
    ordering: Ordering,
    index_client: IndexerClients,
}

impl DataSource<Id, CellIdQuery> for CellIds {
    async fn init(params: CellIdQuery) -> Result<Self, String> {
        let schema = params.schema;
        let field = params.field;
        let ordering = params.ordering;
        let range = params.range.to_key_range(schema, field, ordering);
        let cursor = params.index_client.range_seek(range, BUFFER_SIZE, None).await.map_err(|e| format!("RPC error: {:?}", e))?;
        if let Some(cursor) = cursor {
            return Ok(Self { cursor: UnsafeCell::new(cursor), ongoing: RefCell::new(None) })
        } else {
            return Err("Not found".to_string())
        }
    }
}

impl Stream for CellIds {
    type Item = Id;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            unsafe {
                let mut ongoing_opt = self.ongoing.borrow_mut();
                if let Some(ongoing) = &mut*ongoing_opt {
                    let poll = ongoing.poll_unpin(cx);
                    if poll.is_ready() {
                        *ongoing_opt = None;
                        return poll;
                    } else {
                        return Poll::Pending;
                    }
                }
                {
                    let cursor = &mut*self.cursor.get();
                    let id = async move {
                        let res = cursor.next().await;
                        match res {
                            Ok(res) => return res,
                            Err(e) => {
                                debug!("Error on polling cursor {:?}", e);
                                return None;
                            }
                        }
                    };
                    let fut: Pin<Box<dyn Future<Output = Option<Id>>>> = Box::pin(id);
                    *ongoing_opt = Some(fut);    
                }
            }
        }
    }
}