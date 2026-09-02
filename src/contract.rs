//! The reflected service description served on the built-in `nevo.contract` signal.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const CONTRACT_METHOD: &str = "nevo.contract";
pub const CONTRACT_PROTOCOL_VERSION: &str = "1";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractMethod {
    pub signal_name: String,
    pub version: String,
    #[serde(default)]
    pub params_schema: Option<Value>,
    #[serde(default)]
    pub result_schema: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceContract {
    pub protocol: String,
    pub service_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Vec<String>>,
    pub generated_at: u64,
    #[serde(default)]
    pub methods: Vec<ContractMethod>,
}

impl ServiceContract {
    pub fn has_method(&self, signal_name: &str, version: &str) -> bool {
        self.methods
            .iter()
            .any(|m| m.signal_name == signal_name && m.version == version)
    }
}
