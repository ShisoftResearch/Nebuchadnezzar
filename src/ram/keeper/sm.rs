use std::collections::HashMap;

use dovahkiin::types::OwnedValue;

pub struct Field {
  field_id: u64,
  histogram: [OwnedValue; 10],
  min: OwnedValue,
  max: OwnedValue
}
pub struct Table {
  schema_id: u32,
  fields: HashMap<u64, Field>,
  num_rows: u64,
  num_segs: u64,
  bytes: u64,
  timestamp: u64
}

pub struct Host {
  server_id: u64,
  tables: HashMap<u32, Table>,
  timestamp: u64
}
pub struct KeeperStorage {
  hosts: HashMap<u64, Host>
}