//! Envelope types shared with the TypeScript implementation.

pub mod bigint;
mod escape;
pub mod method;

use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

use crate::compression::Encoding;
use crate::error::{Error, ErrorDetails};

pub use bigint::BigInt;
pub use escape::{
    BIGINT_SENTINEL, SENTINEL_PREFIX, STRING_ESCAPE, is_bigint_sentinel, may_contain_sentinel,
};
pub(crate) use escape::{Escaping, Unescaping};
pub use method::{DEFAULT_VERSION, Method};

pub const PROTOCOL_VERSION: &str = "1";
pub const EVENTS_SUFFIX: &str = "-events";
pub const SUBSCRIPTION_SUFFIX: &str = "-events.sub";
pub const BROADCAST_TOPIC: &str = "__broadcast";
pub const DISCOVERY_TOPIC: &str = "__nevo.discovery";
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

pub const HEADER_ATTEMPT: &str = "nevo-attempt";
pub const HEADER_STREAM: &str = "nevo-stream";
pub const HEADER_STREAM_REPLY_TO: &str = "nevo-reply-to";
pub const HEADER_STREAM_END: &str = "nevo-stream-end";
pub const HEADER_ROOM: &str = "room";
pub const HEADER_CONTENT_ENCODING: &str = "content-encoding";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum MessageType {
    Query,
    Emit,
    Sub,
    Broadcast,
    Discovery,
    Health,
    #[serde(other)]
    Other,
}

impl MessageType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Emit => "emit",
            Self::Sub => "sub",
            Self::Broadcast => "broadcast",
            Self::Discovery => "discovery",
            Self::Health => "health",
            Self::Other => "other",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TraceContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracestate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baggage: Option<String>,
}

impl TraceContext {
    pub fn new(traceparent: impl Into<String>) -> Self {
        Self {
            traceparent: Some(traceparent.into()),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Auth {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

impl Auth {
    pub fn token(token: impl Into<String>) -> Self {
        Self {
            token: Some(token.into()),
        }
    }
}

/// Envelope metadata. Unknown keys survive round trips in `extra`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Meta {
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<MessageType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "lenient_u64"
    )]
    pub ts: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<Auth>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace: Option<TraceContext>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "lenient_u32"
    )]
    pub attempt: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "lenient_encoding"
    )]
    pub content_encoding: Option<Encoding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nevo_chain_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nevo_parent_uuid: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl Meta {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.as_ref()?.get(name).map(String::as_str)
    }

    pub fn set_header(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.headers
            .get_or_insert_with(HashMap::new)
            .insert(name.into(), value.into());
    }

    pub fn auth_token(&self) -> Option<&str> {
        self.auth.as_ref()?.token.as_deref()
    }

    pub fn traceparent(&self) -> Option<&str> {
        self.trace.as_ref()?.traceparent.as_deref()
    }

    /// A field rendered the way the TypeScript peer's subscription filters see it:
    /// missing values become an empty string.
    pub fn field_as_string(&self, key: &str) -> String {
        let value = match key {
            "type" => self.kind.map(|k| k.as_str().to_owned()),
            "service" => self.service.clone(),
            "instanceId" => self.instance_id.clone(),
            "ts" => self.ts.map(|v| v.to_string()),
            "version" => self.version.clone(),
            "attempt" => self.attempt.map(|v| v.to_string()),
            "idempotencyKey" => self.idempotency_key.clone(),
            "contentEncoding" => self
                .content_encoding
                .as_ref()
                .map(|e| e.as_str().to_owned()),
            "codec" => self.codec.clone(),
            "tenantId" => self.tenant_id.clone(),
            "nevoChainId" => self.nevo_chain_id.clone(),
            "nevoParentUuid" => self.nevo_parent_uuid.clone(),
            other => self.extra.get(other).map(|v| match v {
                Value::String(s) => s.clone(),
                Value::Null => String::new(),
                other => other.to_string(),
            }),
        };
        value.unwrap_or_default()
    }
}

fn lenient_u64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<u64>, D::Error> {
    let raw = Option::<f64>::deserialize(deserializer)?;
    Ok(raw
        .filter(|v| v.is_finite() && *v >= 0.0)
        .map(|v| v.round() as u64))
}

fn lenient_u32<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<u32>, D::Error> {
    let raw = Option::<f64>::deserialize(deserializer)?;
    Ok(raw
        .filter(|v| v.is_finite() && *v >= 0.0 && *v <= f64::from(u32::MAX))
        .map(|v| v.round() as u32))
}

fn lenient_encoding<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Encoding>, D::Error> {
    let raw = Option::<String>::deserialize(deserializer)?;
    Ok(raw.as_deref().and_then(Encoding::parse))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Request<P> {
    #[serde(default)]
    pub uuid: String,
    #[serde(default)]
    pub method: String,
    pub params: P,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Meta>,
}

impl<P> Request<P> {
    pub fn new(uuid: impl Into<String>, method: impl Into<String>, params: P) -> Self {
        Self {
            uuid: uuid.into(),
            method: method.into(),
            params,
            meta: None,
        }
    }

    pub fn with_meta(mut self, meta: Meta) -> Self {
        self.meta = Some(meta);
        self
    }
}

/// The envelope minus `params`, used to route before the payload type is known.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RequestHead {
    #[serde(default)]
    pub uuid: String,
    #[serde(default)]
    pub method: String,
    #[serde(default)]
    pub meta: Option<Meta>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseParams<R> {
    pub result: R,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDetails>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Response<R> {
    #[serde(default)]
    pub uuid: String,
    #[serde(default)]
    pub method: String,
    pub params: ResponseParams<R>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Meta>,
}

pub const ERROR_RESULT_MARKER: &str = "error";

impl<R> Response<R> {
    pub fn success(uuid: impl Into<String>, method: impl Into<String>, result: R) -> Self {
        Self {
            uuid: uuid.into(),
            method: method.into(),
            params: ResponseParams {
                result,
                error: None,
            },
            meta: None,
        }
    }

    pub fn with_meta(mut self, meta: Option<Meta>) -> Self {
        self.meta = meta;
        self
    }
}

impl Response<&'static str> {
    pub fn failure(
        uuid: impl Into<String>,
        method: impl Into<String>,
        error: ErrorDetails,
    ) -> Self {
        Self {
            uuid: uuid.into(),
            method: method.into(),
            params: ResponseParams {
                result: ERROR_RESULT_MARKER,
                error: Some(error),
            },
            meta: None,
        }
    }

    pub fn from_error(uuid: impl Into<String>, method: impl Into<String>, error: &Error) -> Self {
        Self::failure(uuid, method, error.to_wire())
    }
}

/// A response with the result left undecoded; tells success from failure cheaply.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ResponseProbe {
    #[serde(default)]
    pub uuid: String,
    #[serde(default)]
    pub method: String,
    #[serde(default)]
    pub params: ProbeParams,
    #[serde(default)]
    pub meta: Option<Meta>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ProbeParams {
    #[serde(default)]
    pub error: Option<ErrorDetails>,
}

impl ResponseProbe {
    pub fn is_error(&self) -> bool {
        self.params.error.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_round_trips_unknown_keys_and_lenient_numbers() {
        let json = r#"{"type":"query","service":"a","ts":1725000000000.0,"attempt":"x","custom":{"k":1},"headers":{"h":"v"}}"#;
        let json = json.replace("\"attempt\":\"x\",", "");
        let meta: Meta = serde_json::from_str(&json).unwrap();
        assert_eq!(meta.kind, Some(MessageType::Query));
        assert_eq!(meta.ts, Some(1_725_000_000_000));
        assert_eq!(meta.extra["custom"]["k"], 1);
        assert_eq!(meta.header("h"), Some("v"));
        let out = serde_json::to_value(&meta).unwrap();
        assert_eq!(out["custom"]["k"], 1);
        assert_eq!(out["type"], "query");
        assert!(out.get("auth").is_none());
    }

    #[test]
    fn unknown_message_type_does_not_break_meta() {
        let meta: Meta = serde_json::from_str(r#"{"type":"stream"}"#).unwrap();
        assert_eq!(meta.kind, Some(MessageType::Other));
    }

    #[test]
    fn error_response_shape() {
        let err = Error::unauthorized("nope").with_service("svc");
        let resp = Response::from_error("u1", "m", &err);
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["params"]["result"], "error");
        assert_eq!(json["params"]["error"]["code"], 1);
        assert_eq!(json["params"]["error"]["service"], "svc");
        let probe: ResponseProbe = serde_json::from_value(json).unwrap();
        assert!(probe.is_error());
    }

    #[test]
    fn field_as_string_mirrors_typescript_semantics() {
        let mut meta = Meta::new();
        meta.tenant_id = Some("t1".into());
        meta.extra.insert("flag".into(), Value::Bool(true));
        assert_eq!(meta.field_as_string("tenantId"), "t1");
        assert_eq!(meta.field_as_string("flag"), "true");
        assert_eq!(meta.field_as_string("missing"), "");
    }
}
