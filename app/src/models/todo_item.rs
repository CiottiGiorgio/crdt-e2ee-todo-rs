use autosurgeon::{Hydrate, Reconcile};
use serde::{Deserialize, Serialize};
use specta::Type;

use super::TodoStatus;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type, Hydrate, Reconcile)]
#[serde(rename_all = "camelCase")]
pub struct TodoItem {
    pub id: String,
    #[autosurgeon(with = "crate::crypto::encrypted_string")]
    pub text: String,
    #[autosurgeon(with = "crate::crypto::encrypted_status")]
    pub status: TodoStatus,
}
