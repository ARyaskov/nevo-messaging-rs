//! NATS binding built on `async-nats`.

mod client;
mod jetstream;
mod server;
pub mod subject;

use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use async_nats::header::{HeaderMap, HeaderName, HeaderValue};
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;

use crate::error::{Error, ErrorCode, Result};
use crate::transport::{Headers, Inbound, Outbound, Transport};

pub use client::{NatsClient, NatsClientBuilder, QueryStream, RequestManyOptions};
pub use jetstream::{
    ConsumerSpec, DeliverPolicy, JetStream, JsInfo, JsMessage, JsSubscription, PubAck,
    PublishOptions, PullOptions, Retention, StreamSpec,
};
pub use server::{NatsServer, NatsServerBuilder, ServeConfig};

pub const DEFAULT_SERVER: &str = "nats://127.0.0.1:4222";

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub enum NatsAuth {
    #[default]
    None,
    Token(String),
    UserPassword {
        user: String,
        password: String,
    },
    Nkey(String),
    CredentialsFile(PathBuf),
    Credentials(String),
}

/// Connection settings for [`Nats::connect`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct NatsConfig {
    pub servers: Vec<String>,
    pub name: Option<String>,
    pub auth: NatsAuth,
    /// `None` reconnects forever.
    pub max_reconnects: Option<usize>,
    pub reconnect_delay: Duration,
    pub reconnect_jitter: Duration,
    pub connection_timeout: Duration,
    /// Keep retrying the first connection in the background instead of failing `connect`.
    pub retry_on_initial_connect: bool,
    pub require_tls: bool,
    /// Flush after every `emit`/`publish` so the broker has the message before the call returns.
    pub flush_after_publish: bool,
    pub inbox_prefix: Option<String>,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            servers: vec![DEFAULT_SERVER.to_owned()],
            name: None,
            auth: NatsAuth::None,
            max_reconnects: None,
            reconnect_delay: Duration::from_secs(5),
            reconnect_jitter: Duration::from_millis(100),
            connection_timeout: Duration::from_secs(5),
            retry_on_initial_connect: false,
            require_tls: false,
            flush_after_publish: true,
            inbox_prefix: None,
        }
    }
}

impl NatsConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn servers(mut self, servers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.servers = servers.into_iter().map(Into::into).collect();
        if self.servers.is_empty() {
            self.servers.push(DEFAULT_SERVER.to_owned());
        }
        self
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn auth(mut self, auth: NatsAuth) -> Self {
        self.auth = auth;
        self
    }

    pub fn max_reconnects(mut self, max: Option<usize>) -> Self {
        self.max_reconnects = max;
        self
    }

    pub fn reconnect_delay(mut self, delay: Duration) -> Self {
        self.reconnect_delay = delay;
        self
    }

    pub fn reconnect_jitter(mut self, jitter: Duration) -> Self {
        self.reconnect_jitter = jitter;
        self
    }

    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    pub fn retry_on_initial_connect(mut self, retry: bool) -> Self {
        self.retry_on_initial_connect = retry;
        self
    }

    pub fn require_tls(mut self, required: bool) -> Self {
        self.require_tls = required;
        self
    }

    pub fn flush_after_publish(mut self, flush: bool) -> Self {
        self.flush_after_publish = flush;
        self
    }

    pub fn inbox_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.inbox_prefix = Some(prefix.into());
        self
    }
}

/// A NATS connection implementing [`Transport`].
#[derive(Clone)]
pub struct Nats {
    client: async_nats::Client,
    flush_after_publish: bool,
}

impl fmt::Debug for Nats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Nats")
            .field("state", &self.client.connection_state())
            .finish()
    }
}

impl Nats {
    pub async fn connect(config: &NatsConfig) -> Result<Self> {
        let mut opts = async_nats::ConnectOptions::new();
        opts = match &config.auth {
            NatsAuth::None => opts,
            NatsAuth::Token(token) => opts.token(token.clone()),
            NatsAuth::UserPassword { user, password } => {
                opts.user_and_password(user.clone(), password.clone())
            }
            NatsAuth::Nkey(seed) => opts.nkey(seed.clone()),
            NatsAuth::CredentialsFile(path) => opts
                .credentials_file(path)
                .await
                .map_err(|e| Error::unauthorized(format!("cannot read credentials file: {e}")))?,
            NatsAuth::Credentials(creds) => opts
                .credentials(creds)
                .map_err(|e| Error::unauthorized(format!("invalid credentials: {e}")))?,
        };
        if let Some(name) = &config.name {
            opts = opts.name(name);
        }
        if let Some(prefix) = &config.inbox_prefix {
            opts = opts.custom_inbox_prefix(prefix);
        }
        let delay = config.reconnect_delay;
        let jitter = config.reconnect_jitter;
        opts = opts
            .max_reconnects(config.max_reconnects)
            .connection_timeout(config.connection_timeout)
            .require_tls(config.require_tls)
            .reconnect_delay_callback(move |attempts| reconnect_delay(delay, jitter, attempts))
            .event_callback(|event| async move {
                match &event {
                    async_nats::Event::Connected => {
                        tracing::info!(event = "nats.status", status = "connected")
                    }
                    async_nats::Event::Disconnected | async_nats::Event::Closed => {
                        tracing::warn!(event = "nats.status", status = %event)
                    }
                    async_nats::Event::ServerError(_) | async_nats::Event::ClientError(_) => {
                        tracing::error!(event = "nats.status", status = %event)
                    }
                    other => tracing::debug!(event = "nats.status", status = %other),
                }
            });
        if config.retry_on_initial_connect {
            opts = opts.retry_on_initial_connect();
        }
        let client = opts
            .connect(config.servers.join(","))
            .await
            .map_err(connect_error)?;
        Ok(Self {
            client,
            flush_after_publish: config.flush_after_publish,
        })
    }

    pub fn from_client(client: async_nats::Client) -> Self {
        Self {
            client,
            flush_after_publish: true,
        }
    }

    pub fn flush_after_publish(mut self, flush: bool) -> Self {
        self.flush_after_publish = flush;
        self
    }

    pub fn client(&self) -> &async_nats::Client {
        &self.client
    }

    pub fn jetstream(&self, codec: crate::codec::Codec) -> JetStream {
        JetStream::new(self.client.clone(), codec)
    }

    async fn publish_to(&self, subject: String, message: Outbound, flush: bool) -> Result<()> {
        match to_header_map(&message.headers) {
            Some(headers) => self
                .client
                .publish_with_headers(subject, headers, message.payload)
                .await
                .map_err(publish_error)?,
            None => self
                .client
                .publish(subject, message.payload)
                .await
                .map_err(publish_error)?,
        }
        if flush {
            self.client.flush().await.map_err(other_error)?;
        }
        Ok(())
    }

    async fn subscribe_to(
        &self,
        subject: String,
        queue_group: Option<&str>,
    ) -> Result<BoxStream<'static, Inbound>> {
        let subscriber = match queue_group {
            Some(group) => self.client.queue_subscribe(subject, group.to_owned()).await,
            None => self.client.subscribe(subject).await,
        }
        .map_err(subscribe_error)?;
        Ok(subscriber.map(from_message).boxed())
    }
}

impl Transport for Nats {
    fn name(&self) -> &'static str {
        "nats"
    }

    fn request<'a>(
        &'a self,
        service: &'a str,
        message: Outbound,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Inbound>> {
        Box::pin(async move {
            let mut request = async_nats::Request::new()
                .payload(message.payload)
                .timeout(Some(timeout));
            if let Some(headers) = to_header_map(&message.headers) {
                request = request.headers(headers);
            }
            let reply = self
                .client
                .send_request(subject::request_subject(service), request)
                .await
                .map_err(request_error)?;
            Ok(from_message(reply))
        })
    }

    fn send<'a>(&'a self, service: &'a str, message: Outbound) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.publish_to(
            subject::request_subject(service),
            message,
            self.flush_after_publish,
        ))
    }

    fn publish<'a>(
        &'a self,
        service: &'a str,
        method: &'a str,
        message: Outbound,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.publish_to(
            subject::publish_subject(service, method),
            message,
            self.flush_after_publish,
        ))
    }

    fn broadcast<'a>(&'a self, method: &'a str, message: Outbound) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.publish_to(subject::broadcast_subject(method), message, false))
    }

    fn subscribe<'a>(
        &'a self,
        service: &'a str,
        method: &'a str,
        queue_group: Option<&'a str>,
    ) -> BoxFuture<'a, Result<BoxStream<'static, Inbound>>> {
        Box::pin(self.subscribe_to(subject::subscribe_subject(service, method), queue_group))
    }

    fn publish_raw<'a>(&'a self, topic: &'a str, message: Outbound) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.publish_to(topic.to_owned(), message, false))
    }

    fn subscribe_raw<'a>(
        &'a self,
        topic: &'a str,
    ) -> BoxFuture<'a, Result<BoxStream<'static, Inbound>>> {
        Box::pin(self.subscribe_to(topic.to_owned(), None))
    }

    fn flush(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { self.client.flush().await.map_err(other_error) })
    }

    fn close(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { self.client.drain().await.map_err(other_error) })
    }
}

/// `async-nats` counts the initial connection as attempt 1; only reconnects wait.
fn reconnect_delay(delay: Duration, jitter: Duration, attempts: usize) -> Duration {
    if attempts <= 1 {
        return Duration::ZERO;
    }
    let extra = if jitter.is_zero() {
        Duration::ZERO
    } else {
        Duration::from_millis(rand::random_range(0..=jitter.as_millis() as u64))
    };
    delay + extra
}

pub(crate) fn to_header_map(headers: &Headers) -> Option<HeaderMap> {
    if headers.is_empty() {
        return None;
    }
    let mut map = HeaderMap::new();
    for (name, value) in headers {
        match (HeaderName::from_str(name), HeaderValue::from_str(value)) {
            (Ok(name), Ok(value)) => map.insert(name, value),
            _ => tracing::warn!(
                event = "nats.header_skipped",
                name,
                "header is not valid for NATS"
            ),
        }
    }
    Some(map)
}

pub(crate) fn from_message(message: async_nats::Message) -> Inbound {
    let mut headers = Headers::new();
    if let Some(map) = &message.headers {
        for (name, values) in map.iter() {
            if let Some(value) = values.first() {
                headers.insert(name.to_string(), value.as_str().to_owned());
            }
        }
    }
    Inbound {
        subject: message.subject.to_string(),
        reply: message.reply.map(|r| r.to_string()),
        payload: message.payload,
        headers,
    }
}

fn connect_error(err: async_nats::ConnectError) -> Error {
    use async_nats::ConnectErrorKind as Kind;
    let code = match err.kind() {
        Kind::Authentication | Kind::AuthorizationViolation => ErrorCode::Unauthorized,
        Kind::TimedOut => ErrorCode::Timeout,
        Kind::ServerParse | Kind::Dns => ErrorCode::BadRequest,
        _ => ErrorCode::ConnectionLost,
    };
    Error::new(code, format!("NATS connect failed: {err}"))
        .with_retryable(matches!(
            code,
            ErrorCode::Timeout | ErrorCode::ConnectionLost
        ))
        .with_source(err)
}

fn request_error(err: async_nats::RequestError) -> Error {
    use async_nats::RequestErrorKind as Kind;
    match err.kind() {
        Kind::TimedOut => Error::from_code(ErrorCode::Timeout).with_retryable(true),
        Kind::NoResponders => {
            Error::new(ErrorCode::ServiceUnavailable, "No responders for request")
                .with_retryable(true)
        }
        Kind::MaxPayloadExceeded => Error::new(ErrorCode::PayloadTooLarge, err.to_string()),
        Kind::InvalidSubject => Error::bad_request(err.to_string()),
        Kind::Other => Error::new(ErrorCode::ConnectionLost, err.to_string()).with_retryable(true),
    }
    .with_source(err)
}

fn publish_error(err: async_nats::PublishError) -> Error {
    use async_nats::PublishErrorKind as Kind;
    match err.kind() {
        Kind::MaxPayloadExceeded => Error::new(ErrorCode::PayloadTooLarge, err.to_string()),
        Kind::InvalidSubject => Error::bad_request(err.to_string()),
        Kind::Send => Error::new(ErrorCode::ConnectionLost, err.to_string()).with_retryable(true),
    }
    .with_source(err)
}

fn subscribe_error(err: async_nats::SubscribeError) -> Error {
    Error::new(
        ErrorCode::ConnectionLost,
        format!("NATS subscribe failed: {err}"),
    )
    .with_source(err)
}

fn other_error(err: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(ErrorCode::ConnectionLost, err.to_string())
        .with_retryable(true)
        .with_source(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_connection_attempt_is_not_delayed() {
        let delay = Duration::from_secs(5);
        assert_eq!(reconnect_delay(delay, Duration::ZERO, 0), Duration::ZERO);
        assert_eq!(reconnect_delay(delay, Duration::ZERO, 1), Duration::ZERO);
        assert_eq!(reconnect_delay(delay, Duration::ZERO, 2), delay);
        let jittered = reconnect_delay(delay, Duration::from_millis(100), 3);
        assert!(jittered >= delay && jittered <= delay + Duration::from_millis(100));
    }
}
