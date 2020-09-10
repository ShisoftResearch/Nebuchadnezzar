use super::trees::*;
use crate::ram::types::Id;

raft_state_machine! {
    def qry locate_key(entry: EntryKey) -> Id;
    def cmd split(tree: Id, new_tree: Id, pivot: EntryKey);
    // No subscription for clients
}
