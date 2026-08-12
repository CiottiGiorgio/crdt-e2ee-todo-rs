use serde::{Deserialize, Serialize};
use specta::Type;

#[derive(Debug, Clone, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub struct TodoItem {
    pub id: i32,
    pub text: String,
    pub completed: bool,
    pub in_working_set: bool,
}
