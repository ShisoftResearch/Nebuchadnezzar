use ram::chunk::Chunks;
use ram::schema::Schemas;
use std::rc::Rc;
use std::sync::Arc;

pub struct ServerOptions {
    pub chunk_count: usize,
    pub memory_size: usize,
    pub schemas: Arc<Schemas>
}

pub struct ServerMeta {
    pub schemas: Arc<Schemas>
}

pub struct Server {
    pub chunks: Chunks,
    pub meta: Rc<ServerMeta>
}

impl Server {
    pub fn new(opt: ServerOptions) -> Server {
        let meta_rc = Rc::<ServerMeta>::new(ServerMeta {
            schemas: opt.schemas
        });
        Server {
            chunks: Chunks::new(opt.chunk_count, opt.memory_size, meta_rc.clone()),
            meta: meta_rc
        }
    }
}