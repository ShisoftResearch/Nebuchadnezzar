use super::ServerOptions;
use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::utils;
use bifrost_hasher::hash_str;
use futures::future;
use futures::prelude::*;
use std::collections::HashMap;
use std::path::Path;

pub static SM_ID_PREFIX: &str = "NEB_DATABASE_CATALOG_SM";

pub fn generate_sm_id(group: &str) -> u64 {
    hash_str(&format!("{}-{}", SM_ID_PREFIX, group))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatabaseCatalogEntry {
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum CreateDatabaseError {
    NameExists(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DeleteDatabaseError {
    DatabaseDoesNotExist(String),
}

pub struct DatabaseCatalogSM {
    sm_id: u64,
    map: HashMap<String, DatabaseCatalogEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatabaseStorageLayout {
    pub backup_storage: Option<String>,
    pub wal_storage: Option<String>,
    pub undo_log_storage: Option<String>,
    pub raft_storage: Option<String>,
}

raft_state_machine! {
    def qry get_all() -> Vec<DatabaseCatalogEntry>;
    def qry get_by_name(name: String) -> Option<DatabaseCatalogEntry>;
    def cmd create_database(entry: DatabaseCatalogEntry) -> Result<(), CreateDatabaseError>;
    def cmd delete_database(name: String) -> Result<(), DeleteDatabaseError>;
}

impl StateMachineCmds for DatabaseCatalogSM {
    fn get_all(&self) -> BoxFuture<'_, Vec<DatabaseCatalogEntry>> {
        future::ready(self.map.values().cloned().collect()).boxed()
    }

    fn get_by_name(&self, name: String) -> BoxFuture<'_, Option<DatabaseCatalogEntry>> {
        future::ready(self.map.get(&name).cloned()).boxed()
    }

    fn create_database(
        &mut self,
        entry: DatabaseCatalogEntry,
    ) -> BoxFuture<'_, Result<(), CreateDatabaseError>> {
        future::ready(match self.map.entry(entry.name.clone()) {
            std::collections::hash_map::Entry::Occupied(_) => {
                Err(CreateDatabaseError::NameExists(entry.name))
            }
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(entry);
                Ok(())
            }
        })
        .boxed()
    }

    fn delete_database(&mut self, name: String) -> BoxFuture<'_, Result<(), DeleteDatabaseError>> {
        future::ready(match self.map.remove(&name) {
            Some(_) => Ok(()),
            None => Err(DeleteDatabaseError::DatabaseDoesNotExist(name)),
        })
        .boxed()
    }
}

impl StateMachineCtl for DatabaseCatalogSM {
    raft_sm_complete!();

    fn id(&self) -> u64 {
        self.sm_id
    }

    fn snapshot(&self) -> Vec<u8> {
        utils::serde::serialize(&self.map)
    }

    fn recover(&mut self, data: Vec<u8>) -> BoxFuture<'_, ()> {
        if let Some(map) = utils::serde::deserialize::<HashMap<String, DatabaseCatalogEntry>>(&data)
        {
            self.map = map;
        }
        future::ready(()).boxed()
    }

    fn recoverable(&self) -> bool {
        true
    }
}

impl DatabaseCatalogSM {
    pub fn new(group: &str) -> Self {
        Self {
            sm_id: generate_sm_id(group),
            map: HashMap::new(),
        }
    }
}

impl DatabaseStorageLayout {
    pub fn from_options(
        opts: &ServerOptions,
        group_name: &str,
        database_name: &str,
    ) -> DatabaseStorageLayout {
        let should_scope = group_name != database_name;
        DatabaseStorageLayout {
            backup_storage: scoped_storage_path(&opts.backup_storage, database_name, should_scope),
            wal_storage: scoped_storage_path(&opts.wal_storage, database_name, should_scope),
            undo_log_storage: scoped_storage_path(
                &opts.undo_log_storage,
                database_name,
                should_scope,
            ),
            raft_storage: scoped_storage_path(&opts.raft_storage, database_name, should_scope),
        }
    }
}

fn scoped_storage_path(
    base_path: &Option<String>,
    database_name: &str,
    should_scope: bool,
) -> Option<String> {
    base_path
        .as_ref()
        .map(|base_path| scoped_storage_path_for_database(base_path, database_name, should_scope))
}

pub fn scoped_storage_path_for_database(
    base_path: &str,
    database_name: &str,
    should_scope: bool,
) -> String {
    if !should_scope {
        return base_path.to_string();
    }

    let sanitized_database = sanitize_database_name(database_name);
    Path::new(base_path)
        .join("databases")
        .join(sanitized_database)
        .to_string_lossy()
        .to_string()
}

pub fn sanitize_database_name(database_name: &str) -> String {
    let mut sanitized = database_name
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => ch,
            _ => '_',
        })
        .collect::<String>();

    if sanitized.is_empty() {
        sanitized.push('_');
    }

    sanitized
}

#[cfg(test)]
mod tests {
    use crate::utils::test_temp::temp_path;
    use super::*;

    fn base_opts() -> ServerOptions {
        ServerOptions {
            chunk_size: 1024,
            db_size: 1024,
            tiered_config: None,
            backup_storage: Some(temp_path("backup")),
            wal_storage: Some(temp_path("wal")),
            undo_log_storage: Some(temp_path("undo")),
            raft_storage: Some(temp_path("raft")),
            services: vec![],
            index_enabled: false,
            enable_recovery: false,
            disable_storage_locks: true,
        }
    }

    #[test]
    fn keeps_legacy_paths_for_single_database_bindings() {
        let opts = base_opts();
        let layout = DatabaseStorageLayout::from_options(&opts, "group_a", "group_a");

        assert_eq!(layout.backup_storage, opts.backup_storage);
        assert_eq!(layout.wal_storage, opts.wal_storage);
        assert_eq!(layout.undo_log_storage, opts.undo_log_storage);
        assert_eq!(layout.raft_storage, opts.raft_storage);
    }

    #[test]
    fn scopes_paths_for_explicit_multi_database_bindings() {
        let layout = DatabaseStorageLayout::from_options(&base_opts(), "group_a", "analytics/db");

        assert_eq!(
            layout.backup_storage,
            Some(scoped_storage_path_for_database(
                &temp_path("backup"),
                "analytics/db",
                true
            ))
        );
        assert_eq!(
            layout.raft_storage,
            Some(scoped_storage_path_for_database(
                &temp_path("raft"),
                "analytics/db",
                true
            ))
        );
    }
}
