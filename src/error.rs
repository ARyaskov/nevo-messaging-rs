use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};

/// Stable numeric error codes shared with the TypeScript implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorCode {
    Unknown,
    Unauthorized,
    Timeout,
    MethodNotFound,
    ServiceNotFound,
    ServiceUnavailable,
    BadRequest,
    ValidationFailed,
    RateLimited,
    CircuitOpen,
    PayloadTooLarge,
    ParseError,
    ReplayDetected,
    IdempotentReplay,
    ConnectionLost,
    Internal,
    Cancelled,
    UnsupportedVersion,
    AckFailed,
    RemoteError,
    IdempotencyKeyConflict,
    /// A code this crate does not know about; preserved verbatim on the wire.
    Other(u16),
}

impl ErrorCode {
    pub const fn as_u16(self) -> u16 {
        match self {
            Self::Unknown => 0,
            Self::Unauthorized => 1,
            Self::Timeout => 2,
            Self::MethodNotFound => 3,
            Self::ServiceNotFound => 4,
            Self::ServiceUnavailable => 5,
            Self::BadRequest => 6,
            Self::ValidationFailed => 7,
            Self::RateLimited => 8,
            Self::CircuitOpen => 9,
            Self::PayloadTooLarge => 10,
            Self::ParseError => 11,
            Self::ReplayDetected => 12,
            Self::IdempotentReplay => 13,
            Self::ConnectionLost => 14,
            Self::Internal => 15,
            Self::Cancelled => 16,
            Self::UnsupportedVersion => 17,
            Self::AckFailed => 18,
            Self::RemoteError => 19,
            Self::IdempotencyKeyConflict => 20,
            Self::Other(code) => code,
        }
    }

    pub const fn from_u16(code: u16) -> Self {
        match code {
            0 => Self::Unknown,
            1 => Self::Unauthorized,
            2 => Self::Timeout,
            3 => Self::MethodNotFound,
            4 => Self::ServiceNotFound,
            5 => Self::ServiceUnavailable,
            6 => Self::BadRequest,
            7 => Self::ValidationFailed,
            8 => Self::RateLimited,
            9 => Self::CircuitOpen,
            10 => Self::PayloadTooLarge,
            11 => Self::ParseError,
            12 => Self::ReplayDetected,
            13 => Self::IdempotentReplay,
            14 => Self::ConnectionLost,
            15 => Self::Internal,
            16 => Self::Cancelled,
            17 => Self::UnsupportedVersion,
            18 => Self::AckFailed,
            19 => Self::RemoteError,
            20 => Self::IdempotencyKeyConflict,
            other => Self::Other(other),
        }
    }

    /// Codes that are transient by default (`INTERNAL` is deliberately excluded).
    pub const fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::Timeout | Self::ServiceUnavailable | Self::ConnectionLost
        )
    }

    pub const fn default_message(self) -> &'static str {
        match self {
            Self::Unknown | Self::Other(_) => "An unknown error occurred",
            Self::Unauthorized => "Access denied",
            Self::Timeout => "Request timed out",
            Self::MethodNotFound => "Method not found",
            Self::ServiceNotFound => "Service not registered",
            Self::ServiceUnavailable => "Service unavailable",
            Self::BadRequest => "Bad request",
            Self::ValidationFailed => "Validation failed",
            Self::RateLimited => "Rate limit exceeded",
            Self::CircuitOpen => "Circuit breaker is open",
            Self::PayloadTooLarge => "Payload exceeds size limit",
            Self::ParseError => "Failed to parse message",
            Self::ReplayDetected => "Replay attack detected",
            Self::IdempotentReplay => "Idempotent request already processed",
            Self::ConnectionLost => "Connection to broker lost",
            Self::Internal => "Internal error",
            Self::Cancelled => "Request cancelled",
            Self::UnsupportedVersion => "Unsupported method version",
            Self::AckFailed => "Failed to acknowledge message",
            Self::RemoteError => "Remote returned an error response",
            Self::IdempotencyKeyConflict => "Idempotency key reused with a different payload",
        }
    }
}

impl From<u16> for ErrorCode {
    fn from(code: u16) -> Self {
        Self::from_u16(code)
    }
}

impl From<ErrorCode> for u16 {
    fn from(code: ErrorCode) -> Self {
        code.as_u16()
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}({})", self.as_u16())
    }
}

impl Serialize for ErrorCode {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u16(self.as_u16())
    }
}

impl<'de> Deserialize<'de> for ErrorCode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = f64::deserialize(deserializer)?;
        if raw.is_finite() && raw >= 0.0 && raw <= f64::from(u16::MAX) {
            Ok(Self::from_u16(raw as u16))
        } else {
            Ok(Self::Unknown)
        }
    }
}

/// Wire representation of an error inside a response envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorDetails {
    pub code: ErrorCode,
    #[serde(default)]
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stack: Option<String>,
}

/// The error type used across the crate and carried inside response envelopes.
#[derive(thiserror::Error)]
pub struct Error {
    code: ErrorCode,
    message: String,
    details: Map<String, Value>,
    service: Option<String>,
    retryable: Option<bool>,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl Error {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: Map::new(),
            service: None,
            retryable: None,
            source: None,
        }
    }

    pub fn from_code(code: ErrorCode) -> Self {
        Self::new(code, code.default_message())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::BadRequest, message)
    }

    pub fn validation(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::ValidationFailed, message)
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unauthorized, message)
    }

    pub fn parse(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::ParseError, message)
    }

    pub fn timeout(service: &str, method: &str, after: std::time::Duration) -> Self {
        Self::new(
            ErrorCode::Timeout,
            format!(
                "Request to {service}.{method} timed out after {}ms",
                after.as_millis()
            ),
        )
        .with_service(service)
        .with_retryable(true)
    }

    pub fn circuit_open(service: &str, method: &str) -> Self {
        Self::new(
            ErrorCode::CircuitOpen,
            format!("Circuit breaker open for {service}.{method}"),
        )
        .with_service(service)
        .with_retryable(false)
    }

    pub fn payload_too_large(size: usize, limit: usize) -> Self {
        Self::new(
            ErrorCode::PayloadTooLarge,
            format!("Payload size {size}B exceeds limit {limit}B"),
        )
        .with_detail("size", size)
        .with_detail("limit", limit)
    }

    pub fn with_detail(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.details.insert(key.into(), value.into());
        self
    }

    pub fn with_details(mut self, details: Map<String, Value>) -> Self {
        self.details.extend(details);
        self
    }

    pub fn with_service(mut self, service: impl Into<String>) -> Self {
        self.service = Some(service.into());
        self
    }

    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = Some(retryable);
        self
    }

    pub fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    pub fn code(&self) -> ErrorCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn details(&self) -> &Map<String, Value> {
        &self.details
    }

    pub fn detail(&self, key: &str) -> Option<&Value> {
        self.details.get(key)
    }

    pub fn service(&self) -> Option<&str> {
        self.service.as_deref()
    }

    /// Explicit retry hint, when one was set locally or received from the peer.
    pub fn retryable_hint(&self) -> Option<bool> {
        self.retryable
    }

    /// Whether a retry policy should consider this error transient.
    pub fn is_retryable(&self) -> bool {
        self.retryable.unwrap_or_else(|| self.code.is_retryable())
    }

    /// Server-provided `retryAfterMs` hint, capped to 30 seconds.
    pub fn retry_after(&self) -> Option<std::time::Duration> {
        let hint = self.details.get("retryAfterMs")?.as_f64()?;
        if !hint.is_finite() || hint <= 0.0 {
            return None;
        }
        Some(std::time::Duration::from_millis(hint.min(30_000.0) as u64))
    }

    pub fn is_code(&self, code: ErrorCode) -> bool {
        self.code == code
    }

    pub fn to_wire(&self) -> ErrorDetails {
        let mut details = self.details.clone();
        if let Some(retryable) = self.retryable {
            details.insert("retryable".into(), Value::Bool(retryable));
        }
        ErrorDetails {
            code: self.code,
            message: self.message.clone(),
            details: Some(details),
            service: self.service.clone(),
            stack: None,
        }
    }

    pub fn from_wire(wire: ErrorDetails, fallback_service: Option<&str>) -> Self {
        let mut details = wire.details.unwrap_or_default();
        let retryable = details.remove("retryable").and_then(|v| v.as_bool());
        let message = if wire.message.is_empty() {
            wire.code.default_message().to_owned()
        } else {
            wire.message
        };
        Self {
            code: wire.code,
            message,
            details,
            service: wire.service.or_else(|| fallback_service.map(str::to_owned)),
            retryable,
            source: None,
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut dbg = f.debug_struct("Error");
        dbg.field("code", &self.code)
            .field("message", &self.message);
        if !self.details.is_empty() {
            dbg.field("details", &self.details);
        }
        if let Some(service) = &self.service {
            dbg.field("service", service);
        }
        if let Some(retryable) = self.retryable {
            dbg.field("retryable", &retryable);
        }
        if let Some(source) = &self.source {
            dbg.field("source", source);
        }
        dbg.finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.service {
            Some(service) => write!(f, "[{}] {} ({service})", self.code, self.message),
            None => write!(f, "[{}] {}", self.code, self.message),
        }
    }
}

impl From<ErrorCode> for Error {
    fn from(code: ErrorCode) -> Self {
        Self::from_code(code)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::parse(format!("JSON error: {err}")).with_source(err)
    }
}

impl From<tokio::time::error::Elapsed> for Error {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::from_code(ErrorCode::Timeout).with_retryable(true)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_round_trip_through_u16() {
        for code in 0..=25u16 {
            assert_eq!(ErrorCode::from_u16(code).as_u16(), code);
        }
        assert_eq!(ErrorCode::from_u16(99), ErrorCode::Other(99));
    }

    #[test]
    fn wire_round_trip_preserves_retryable_and_details() {
        let err = Error::new(ErrorCode::RateLimited, "slow down")
            .with_detail("retryAfterMs", 250)
            .with_retryable(true)
            .with_service("user");
        let wire = err.to_wire();
        assert_eq!(wire.code, ErrorCode::RateLimited);
        assert_eq!(
            wire.details.as_ref().unwrap()["retryable"],
            Value::Bool(true)
        );
        let back = Error::from_wire(wire, None);
        assert_eq!(back.retryable_hint(), Some(true));
        assert_eq!(
            back.retry_after(),
            Some(std::time::Duration::from_millis(250))
        );
        assert_eq!(back.service(), Some("user"));
        assert!(back.detail("retryable").is_none());
    }

    #[test]
    fn code_deserializes_from_float_and_int() {
        assert_eq!(
            serde_json::from_str::<ErrorCode>("2").unwrap(),
            ErrorCode::Timeout
        );
        assert_eq!(
            serde_json::from_str::<ErrorCode>("2.0").unwrap(),
            ErrorCode::Timeout
        );
        assert_eq!(
            serde_json::from_str::<ErrorCode>("-5").unwrap(),
            ErrorCode::Unknown
        );
    }
}
