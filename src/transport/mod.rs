//! The transport contract every broker binding implements. The client, router
//! and resilience layers are written against it, so a Kafka or HTTP binding
//! only needs to map addresses and move bytes.

#[cfg(feature = "nats")]
pub mod nats;

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;

use crate::compression::Encoding;
use crate::error::Result;
use crate::wire::HEADER_CONTENT_ENCODING;

pub type Headers = HashMap<String, String>;

#[derive(Debug, Clone, Default)]
pub struct Outbound {
    pub payload: Bytes,
    pub headers: Headers,
}

impl Outbound {
    pub fn new(payload: Bytes) -> Self {
        Self {
            payload,
            headers: Headers::new(),
        }
    }

    pub fn with_headers(mut self, headers: Headers) -> Self {
        self.headers = headers;
        self
    }

    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    pub fn encoded(mut self, encoding: Encoding) -> Self {
        if !encoding.is_identity() {
            self.headers.insert(
                HEADER_CONTENT_ENCODING.to_owned(),
                encoding.as_str().to_owned(),
            );
        }
        self
    }
}

#[derive(Debug, Clone)]
pub struct Inbound {
    pub subject: String,
    pub reply: Option<String>,
    pub payload: Bytes,
    pub headers: Headers,
}

impl Inbound {
    pub fn new(subject: impl Into<String>, payload: Bytes) -> Self {
        Self {
            subject: subject.into(),
            reply: None,
            payload,
            headers: Headers::new(),
        }
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }

    /// The transport-level `content-encoding`; unknown values surface as `None`.
    pub fn content_encoding(&self) -> Option<Encoding> {
        match self.header(HEADER_CONTENT_ENCODING) {
            None => Some(Encoding::Identity),
            Some(raw) => Encoding::parse(raw),
        }
    }
}

/// Address mapping and byte movement for one broker.
pub trait Transport: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    /// Request/reply against the service's request address.
    fn request<'a>(
        &'a self,
        service: &'a str,
        message: Outbound,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Inbound>>;

    /// Fire-and-forget delivery to the service's request address.
    fn send<'a>(&'a self, service: &'a str, message: Outbound) -> BoxFuture<'a, Result<()>>;

    /// Fan-out to the method-scoped pub/sub address.
    fn publish<'a>(
        &'a self,
        service: &'a str,
        method: &'a str,
        message: Outbound,
    ) -> BoxFuture<'a, Result<()>>;

    fn broadcast<'a>(&'a self, method: &'a str, message: Outbound) -> BoxFuture<'a, Result<()>>;

    /// Subscribes to a service's pub/sub address; dropping the stream unsubscribes.
    fn subscribe<'a>(
        &'a self,
        service: &'a str,
        method: &'a str,
        queue_group: Option<&'a str>,
    ) -> BoxFuture<'a, Result<BoxStream<'static, Inbound>>>;

    fn publish_raw<'a>(&'a self, topic: &'a str, message: Outbound) -> BoxFuture<'a, Result<()>>;

    fn subscribe_raw<'a>(
        &'a self,
        topic: &'a str,
    ) -> BoxFuture<'a, Result<BoxStream<'static, Inbound>>>;

    fn flush(&self) -> BoxFuture<'_, Result<()>>;

    fn close(&self) -> BoxFuture<'_, Result<()>>;
}
