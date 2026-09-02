use std::fmt;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use futures::{Stream, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::{Nats, NatsAuth, NatsConfig, from_message, subject, to_header_map};
use crate::client::{CallOptions, Client, ClientConfig, Core, Subscription, Unpacker};
use crate::codec::Codec;
use crate::compression::Compression;
use crate::discovery::DiscoveryConfig;
use crate::error::{Error, ErrorCode, Result};
use crate::idempotency::IdempotencyConfig;
use crate::resilience::{CircuitBreakerConfig, HedgePolicy, RetryPolicy};
use crate::wire::{
    HEADER_STREAM, HEADER_STREAM_END, HEADER_STREAM_REPLY_TO, MessageType, RequestHead,
};

/// A [`Client`] over NATS.
pub type NatsClient = Client<Nats>;

/// Builds a connected [`NatsClient`].
#[derive(Debug, Clone, Default)]
pub struct NatsClientBuilder {
    nats: NatsConfig,
    client: ClientConfig,
}

impl NatsClientBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn nats_config(mut self, config: NatsConfig) -> Self {
        self.nats = config;
        self
    }

    pub fn client_config(mut self, config: ClientConfig) -> Self {
        self.client = config;
        self
    }

    pub fn servers(mut self, servers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.nats = self.nats.servers(servers);
        self
    }

    pub fn server(self, server: impl Into<String>) -> Self {
        self.servers([server])
    }

    pub fn connection_name(mut self, name: impl Into<String>) -> Self {
        self.nats = self.nats.name(name);
        self
    }

    pub fn auth(mut self, auth: NatsAuth) -> Self {
        self.nats = self.nats.auth(auth);
        self
    }

    pub fn max_reconnects(mut self, max: Option<usize>) -> Self {
        self.nats = self.nats.max_reconnects(max);
        self
    }

    pub fn reconnect_delay(mut self, delay: Duration) -> Self {
        self.nats = self.nats.reconnect_delay(delay);
        self
    }

    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.nats = self.nats.connection_timeout(timeout);
        self
    }

    pub fn retry_on_initial_connect(mut self, retry: bool) -> Self {
        self.nats = self.nats.retry_on_initial_connect(retry);
        self
    }

    pub fn require_tls(mut self, required: bool) -> Self {
        self.nats = self.nats.require_tls(required);
        self
    }

    pub fn flush_after_publish(mut self, flush: bool) -> Self {
        self.nats = self.nats.flush_after_publish(flush);
        self
    }

    pub fn service_name(mut self, name: impl Into<String>) -> Self {
        self.client = self.client.service_name(name);
        self
    }

    pub fn instance_id(mut self, id: impl Into<String>) -> Self {
        self.client = self.client.instance_id(id);
        self
    }

    pub fn auth_token(mut self, token: impl Into<String>) -> Self {
        self.client = self.client.auth_token(token);
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.client = self.client.timeout(timeout);
        self
    }

    pub fn codec(mut self, codec: Codec) -> Self {
        self.client = self.client.codec(codec);
        self
    }

    pub fn compression(mut self, compression: Compression) -> Self {
        self.client = self.client.compression(compression);
        self
    }

    pub fn max_payload_bytes(mut self, bytes: usize) -> Self {
        self.client = self.client.max_payload_bytes(bytes);
        self
    }

    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.client = self.client.retry(policy);
        self
    }

    pub fn circuit_breaker(mut self, config: CircuitBreakerConfig) -> Self {
        self.client = self.client.circuit_breaker(config);
        self
    }

    pub fn idempotency(mut self, config: IdempotencyConfig) -> Self {
        self.client = self.client.idempotency(config);
        self
    }

    pub fn default_version(mut self, version: impl Into<String>) -> Self {
        self.client = self.client.default_version(version);
        self
    }

    pub fn trace_context(mut self, enabled: bool) -> Self {
        self.client = self.client.trace_context(enabled);
        self
    }

    pub fn hedge(mut self, policy: HedgePolicy) -> Self {
        self.client = self.client.hedge(policy);
        self
    }

    pub fn services(mut self, names: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        self.client = self.client.services(names);
        self
    }

    pub fn discovery(mut self, config: DiscoveryConfig) -> Self {
        self.client = self.client.discovery(config);
        self
    }

    pub async fn connect(self) -> Result<NatsClient> {
        let nats = Nats::connect(&self.nats).await?;
        Ok(Client::new(nats, self.client))
    }

    /// Reuses an already-connected `async_nats::Client`.
    pub fn with_connection(self, connection: async_nats::Client) -> NatsClient {
        let nats = Nats::from_client(connection).flush_after_publish(self.nats.flush_after_publish);
        Client::new(nats, self.client)
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RequestManyOptions {
    pub max_messages: usize,
    pub max_wait: Option<Duration>,
    pub call: CallOptions,
}

impl Default for RequestManyOptions {
    fn default() -> Self {
        Self {
            max_messages: 10,
            max_wait: None,
            call: CallOptions::default(),
        }
    }
}

impl RequestManyOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn max_messages(mut self, max: usize) -> Self {
        self.max_messages = max;
        self
    }

    pub fn max_wait(mut self, wait: Duration) -> Self {
        self.max_wait = Some(wait);
        self
    }

    pub fn call(mut self, call: CallOptions) -> Self {
        self.call = call;
        self
    }
}

/// Chunks of a streaming reply; ends on the `nevo-stream-end` marker or after an idle gap.
pub struct QueryStream<R> {
    subscriber: async_nats::Subscriber,
    idle: Pin<Box<tokio::time::Sleep>>,
    timeout: Duration,
    unpacker: Arc<dyn Unpacker>,
    service: String,
    method: String,
    count: usize,
    done: bool,
    _result: PhantomData<fn() -> R>,
}

impl<R> fmt::Debug for QueryStream<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryStream")
            .field("service", &self.service)
            .field("method", &self.method)
            .field("count", &self.count)
            .finish()
    }
}

impl<R> QueryStream<R> {
    pub fn count(&self) -> usize {
        self.count
    }

    pub async fn cancel(mut self) {
        self.done = true;
        let _ = self.subscriber.unsubscribe().await;
    }
}

impl<R: DeserializeOwned> Stream for QueryStream<R> {
    type Item = Result<R>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        loop {
            match Pin::new(&mut this.subscriber).poll_next(cx) {
                Poll::Ready(Some(message)) => {
                    this.idle
                        .as_mut()
                        .reset(tokio::time::Instant::now() + this.timeout);
                    let inbound = from_message(message);
                    let Ok((codec, raw)) = this.unpacker.unpack(&inbound) else {
                        continue;
                    };
                    let Ok(head) = codec.decode::<RequestHead>(&raw) else {
                        continue;
                    };
                    let ended =
                        head.meta.as_ref().and_then(|m| m.header(HEADER_STREAM_END)) == Some("1");
                    if ended {
                        this.done = true;
                        return Poll::Ready(None);
                    }
                    match Core::unwrap_response::<R>(codec, &raw, &this.service) {
                        Ok(chunk) => {
                            this.count += 1;
                            return Poll::Ready(Some(Ok(chunk)));
                        }
                        Err(_) => continue,
                    }
                }
                Poll::Ready(None) => {
                    this.done = true;
                    return Poll::Ready(None);
                }
                Poll::Pending => {
                    if this.idle.as_mut().poll(cx).is_ready() {
                        this.done = true;
                        return Poll::Ready(Some(Err(Error::timeout(
                            &this.service,
                            &this.method,
                            this.timeout,
                        ))));
                    }
                    return Poll::Pending;
                }
            }
        }
    }
}

impl Client<Nats> {
    pub fn builder() -> NatsClientBuilder {
        NatsClientBuilder::new()
    }

    /// The underlying `async_nats::Client`, for anything this crate does not wrap.
    pub fn nats(&self) -> &async_nats::Client {
        self.transport().client()
    }

    pub fn jetstream(&self) -> super::JetStream {
        self.transport().jetstream(self.config().codec)
    }

    /// Scatter/gather: every responder on the service subject may reply; error replies are dropped.
    pub async fn request_many<P, R>(
        &self,
        service: &str,
        method: &str,
        params: &P,
        opts: RequestManyOptions,
    ) -> Result<Vec<R>>
    where
        P: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let service = self.target(service)?;
        let core = self.core();
        let encoded = core.encode_request(method, params, MessageType::Query, &opts.call)?;
        let nats = self.nats();
        let inbox = nats.new_inbox();
        let mut subscriber = nats
            .subscribe(inbox.clone())
            .await
            .map_err(super::subscribe_error)?;
        let outbound = encoded.outbound();
        let subject = subject::request_subject(&service);
        match to_header_map(&outbound.headers) {
            Some(headers) => {
                nats.publish_with_reply_and_headers(subject, inbox, headers, outbound.payload)
                    .await
            }
            None => {
                nats.publish_with_reply(subject, inbox, outbound.payload)
                    .await
            }
        }
        .map_err(super::publish_error)?;

        let max_wait = opts
            .max_wait
            .or(opts.call.timeout)
            .unwrap_or(core.config.timeout);
        let deadline = tokio::time::Instant::now() + max_wait;
        let mut results = Vec::new();
        while results.len() < opts.max_messages.max(1) {
            let message = match tokio::time::timeout_at(deadline, subscriber.next()).await {
                Ok(Some(message)) => message,
                _ => break,
            };
            if message.status == Some(async_nats::StatusCode::NO_RESPONDERS) {
                break;
            }
            let inbound = from_message(message);
            let decoded = core
                .unpack(&inbound)
                .and_then(|(codec, raw)| Core::unwrap_response::<R>(codec, &raw, &service));
            match decoded {
                Ok(value) => results.push(value),
                Err(err) => tracing::debug!(event = "nats.request_many.skipped", error = %err),
            }
        }
        let _ = subscriber.unsubscribe().await;
        Ok(results)
    }

    /// Subscribes to a raw subject pattern (`orders.>`), decoding Nevo envelopes.
    pub async fn subscribe_wildcard<P: DeserializeOwned>(
        &self,
        pattern: &str,
    ) -> Result<Subscription<P>> {
        let stream = self
            .nats()
            .subscribe(pattern.to_owned())
            .await
            .map_err(super::subscribe_error)?
            .map(from_message)
            .boxed();
        Ok(Subscription::raw(stream, self.unpacker()))
    }

    /// A request whose responder streams several replies to a dedicated inbox.
    pub async fn query_stream<P, R>(
        &self,
        service: &str,
        method: &str,
        params: &P,
        opts: CallOptions,
    ) -> Result<QueryStream<R>>
    where
        P: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let service = self.target(service)?;
        let core = self.core();
        let nats = self.nats();
        let inbox = nats.new_inbox();
        let timeout = opts.timeout.unwrap_or(core.config.timeout);
        let mut opts = opts;
        opts.headers
            .insert(HEADER_STREAM.to_owned(), "1".to_owned());
        opts.headers
            .insert(HEADER_STREAM_REPLY_TO.to_owned(), inbox.clone());
        let encoded = core.encode_request(method, params, MessageType::Query, &opts)?;
        let subscriber = nats
            .subscribe(inbox.clone())
            .await
            .map_err(super::subscribe_error)?;
        let outbound = encoded.outbound();
        let subject = subject::request_subject(&service);
        match to_header_map(&outbound.headers) {
            Some(headers) => {
                nats.publish_with_reply_and_headers(subject, inbox, headers, outbound.payload)
                    .await
            }
            None => {
                nats.publish_with_reply(subject, inbox, outbound.payload)
                    .await
            }
        }
        .map_err(super::publish_error)?;
        Ok(QueryStream {
            subscriber,
            idle: Box::pin(tokio::time::sleep(timeout)),
            timeout,
            unpacker: self.unpacker(),
            service,
            method: method.to_owned(),
            count: 0,
            done: false,
            _result: PhantomData,
        })
    }

    /// Round trip to the server, as a readiness probe.
    pub async fn ping(&self) -> Result<()> {
        self.nats()
            .flush()
            .await
            .map_err(|e| Error::new(ErrorCode::ConnectionLost, e.to_string()).with_retryable(true))
    }
}
