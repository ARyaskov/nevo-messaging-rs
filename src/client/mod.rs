//! The transport-agnostic client: envelope construction, retry, circuit
//! breaking, idempotent replay, hedging and pub/sub decoding.

use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
use futures::stream::BoxStream;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::Instrument;

use crate::codec::Codec;
use crate::compression::{Compression, Encoding, compress, decompress};
use crate::context;
use crate::discovery::{Announcement, DiscoveryConfig, DiscoveryEntry, DiscoveryRegistry};
use crate::error::{Error, ErrorCode, Result};
use crate::filter::SubscriptionFilter;
use crate::idempotency::{IdempotencyConfig, IdempotencyEntry, ResponseCache, client_key};
use crate::metrics;
use crate::resilience::{
    CircuitBreaker, CircuitBreakerConfig, HedgePolicy, RetryPolicy, hedge, with_retry,
};
use crate::shutdown::Inflight;
use crate::time::unix_millis;
use crate::transport::{Inbound, Outbound, Transport};
use crate::wire::{
    Auth, BROADCAST_TOPIC, DEFAULT_MAX_PAYLOAD_BYTES, DEFAULT_VERSION, DISCOVERY_TOPIC,
    HEADER_ATTEMPT, HEADER_ROOM, MessageType, Meta, Method, Request, Response, ResponseProbe,
    TraceContext, method,
};

/// Per-call overrides.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct CallOptions {
    pub version: Option<String>,
    pub idempotency_key: Option<String>,
    pub headers: HashMap<String, String>,
    pub tenant_id: Option<String>,
    pub timeout: Option<Duration>,
    pub chain_id: Option<String>,
    pub hedge: Option<HedgePolicy>,
    pub retry: Option<RetryPolicy>,
}

impl CallOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    pub fn tenant_id(mut self, tenant: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant.into());
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn chain_id(mut self, chain_id: impl Into<String>) -> Self {
        self.chain_id = Some(chain_id.into());
        self
    }

    pub fn hedge(mut self, policy: HedgePolicy) -> Self {
        self.hedge = Some(policy);
        self
    }

    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.retry = Some(policy);
        self
    }
}

/// Transport-independent client settings.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ClientConfig {
    pub service_name: Option<String>,
    pub instance_id: String,
    pub auth_token: Option<String>,
    pub timeout: Duration,
    pub codec: Codec,
    pub compression: Compression,
    pub max_payload_bytes: usize,
    pub retry: RetryPolicy,
    pub circuit_breaker: CircuitBreakerConfig,
    pub idempotency: IdempotencyConfig,
    pub default_version: String,
    pub trace_context: bool,
    pub hedge: Option<HedgePolicy>,
    pub services: Vec<String>,
    pub discovery: Option<DiscoveryConfig>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            service_name: None,
            instance_id: uuid::Uuid::new_v4().to_string(),
            auth_token: None,
            timeout: Duration::from_secs(20),
            codec: Codec::default(),
            compression: Compression::default(),
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            retry: RetryPolicy::default(),
            circuit_breaker: CircuitBreakerConfig::default(),
            idempotency: IdempotencyConfig::default(),
            default_version: DEFAULT_VERSION.to_owned(),
            trace_context: false,
            hedge: None,
            services: Vec::new(),
            discovery: None,
        }
    }
}

impl ClientConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Identity stamped into `meta.service`; peers use it for ACL decisions.
    pub fn service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = Some(name.into());
        self
    }

    pub fn instance_id(mut self, id: impl Into<String>) -> Self {
        self.instance_id = id.into();
        self
    }

    pub fn auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn codec(mut self, codec: Codec) -> Self {
        self.codec = codec;
        self
    }

    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    pub fn max_payload_bytes(mut self, bytes: usize) -> Self {
        self.max_payload_bytes = bytes;
        self
    }

    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }

    pub fn circuit_breaker(mut self, config: CircuitBreakerConfig) -> Self {
        self.circuit_breaker = config;
        self
    }

    pub fn idempotency(mut self, config: IdempotencyConfig) -> Self {
        self.idempotency = config;
        self
    }

    pub fn default_version(mut self, version: impl Into<String>) -> Self {
        self.default_version = version.into();
        self
    }

    /// Stamps a W3C `traceparent` on every envelope, continuing the inbound trace inside handlers.
    pub fn trace_context(mut self, enabled: bool) -> Self {
        self.trace_context = enabled;
        self
    }

    pub fn hedge(mut self, policy: HedgePolicy) -> Self {
        self.hedge = Some(policy);
        self
    }

    /// Restricts calls to the listed services; an empty list allows any target.
    pub fn services(mut self, names: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        self.services = names
            .into_iter()
            .map(|n| n.as_ref().to_ascii_lowercase())
            .collect();
        self
    }

    pub fn discovery(mut self, config: DiscoveryConfig) -> Self {
        self.discovery = Some(config);
        self
    }
}

/// An envelope ready to leave the process.
#[derive(Debug, Clone)]
pub struct Encoded {
    pub uuid: String,
    pub method: String,
    pub meta: Meta,
    pub payload: Bytes,
    pub encoding: Encoding,
}

impl Encoded {
    pub fn outbound(&self) -> Outbound {
        let headers = self.meta.headers.clone().unwrap_or_default();
        Outbound::new(self.payload.clone())
            .with_headers(headers)
            .encoded(self.encoding)
    }
}

pub(crate) struct Core {
    pub config: ClientConfig,
    pub breaker: CircuitBreaker,
    pub cache: ResponseCache,
    pub inflight: Inflight,
}

impl Core {
    pub fn new(config: ClientConfig) -> Self {
        Self {
            breaker: CircuitBreaker::new(config.circuit_breaker.clone()),
            cache: ResponseCache::new(&config.idempotency),
            inflight: Inflight::new(),
            config,
        }
    }

    pub fn build_meta(&self, kind: MessageType, opts: &CallOptions) -> Meta {
        let mut meta = Meta::new();
        meta.kind = Some(kind);
        meta.service = self.config.service_name.clone();
        meta.instance_id = Some(self.config.instance_id.clone());
        meta.ts = Some(unix_millis());
        meta.version = Some(
            opts.version
                .clone()
                .unwrap_or_else(|| self.config.default_version.clone()),
        );
        meta.auth = self.config.auth_token.clone().map(Auth::token);
        meta.codec = Some(self.config.codec.name().to_owned());
        meta.idempotency_key = opts.idempotency_key.clone();
        meta.tenant_id = opts.tenant_id.clone();
        meta.headers = (!opts.headers.is_empty()).then(|| opts.headers.clone());
        meta.nevo_chain_id = Some(context::resolve_outbound(opts.chain_id.as_deref()));
        let current = context::current();
        meta.nevo_parent_uuid = current.as_ref().and_then(|c| c.parent_uuid.clone());
        if self.config.trace_context {
            let inherited = current
                .as_ref()
                .and_then(|c| c.trace.as_ref())
                .and_then(|t| t.traceparent.as_deref())
                .and_then(context::child_traceparent);
            let mut trace = TraceContext::new(inherited.unwrap_or_else(context::new_traceparent));
            trace.tracestate = current
                .as_ref()
                .and_then(|c| c.trace.as_ref())
                .and_then(|t| t.tracestate.clone());
            meta.trace = Some(trace);
        }
        meta
    }

    pub fn encode_request<P: Serialize + ?Sized>(
        &self,
        method_name: &str,
        params: &P,
        kind: MessageType,
        opts: &CallOptions,
    ) -> Result<Encoded> {
        let uuid = uuid::Uuid::now_v7().to_string();
        let meta = self.build_meta(kind, opts);
        let versioned = method::with_version(
            method_name,
            opts.version
                .as_deref()
                .unwrap_or(&self.config.default_version),
        );
        let envelope =
            Request::new(uuid.clone(), versioned.clone(), params).with_meta(meta.clone());
        let raw = self.config.codec.encode(&envelope)?;
        if raw.len() > self.config.max_payload_bytes {
            return Err(Error::payload_too_large(
                raw.len(),
                self.config.max_payload_bytes,
            ));
        }
        let (payload, encoding) = compress(raw, &self.config.compression)?;
        metrics::observe(
            metrics::PAYLOAD_BYTES,
            vec![
                ("direction", "out".into()),
                (
                    "service",
                    self.config.service_name.clone().unwrap_or_default(),
                ),
            ],
            payload.len() as f64,
        );
        let mut meta = meta;
        meta.content_encoding = Some(encoding);
        Ok(Encoded {
            uuid,
            method: versioned,
            meta,
            payload,
            encoding,
        })
    }

    /// Decompresses and sniffs an inbound envelope, returning the codec and raw bytes.
    pub fn unpack(&self, inbound: &Inbound) -> Result<(Codec, Bytes)> {
        let encoding = inbound
            .content_encoding()
            .ok_or_else(|| Error::bad_request("Unsupported content-encoding on inbound message"))?;
        let raw = decompress(
            inbound.payload.clone(),
            encoding,
            self.config.max_payload_bytes,
        )?;
        if raw.len() > self.config.max_payload_bytes {
            return Err(Error::payload_too_large(
                raw.len(),
                self.config.max_payload_bytes,
            ));
        }
        metrics::observe(
            metrics::PAYLOAD_BYTES,
            vec![
                ("direction", "in".into()),
                (
                    "service",
                    self.config.service_name.clone().unwrap_or_default(),
                ),
            ],
            raw.len() as f64,
        );
        Ok((Codec::detect(&raw).unwrap_or(self.config.codec), raw))
    }

    pub fn unwrap_response<R: DeserializeOwned>(
        codec: Codec,
        raw: &[u8],
        service: &str,
    ) -> Result<R> {
        let probe: ResponseProbe = codec.decode(raw)?;
        if let Some(wire) = probe.params.error {
            return Err(Error::from_wire(wire, Some(service)));
        }
        let response: Response<R> = codec.decode(raw)?;
        Ok(response.params.result)
    }
}

impl fmt::Debug for Core {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Core")
            .field("config", &self.config)
            .finish()
    }
}

struct DiscoveryState {
    registry: Arc<DiscoveryRegistry>,
    config: DiscoveryConfig,
}

struct Inner<T> {
    core: Core,
    transport: T,
    discovery: Option<DiscoveryState>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

/// A messaging client over any [`Transport`]. Cheap to clone.
pub struct Client<T: Transport> {
    inner: Arc<Inner<T>>,
}

impl<T: Transport> Clone for Client<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T: Transport> fmt::Debug for Client<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("transport", &self.inner.transport.name())
            .field("service", &self.inner.core.config.service_name)
            .finish()
    }
}

/// Options for [`Client::subscribe`].
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct SubscribeOptions {
    pub queue_group: Option<String>,
    pub filter: Option<SubscriptionFilter>,
    pub room: Option<String>,
}

impl SubscribeOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Load-balances deliveries across subscribers sharing the group.
    pub fn queue_group(mut self, group: impl Into<String>) -> Self {
        self.queue_group = Some(group.into());
        self
    }

    pub fn filter(mut self, filter: SubscriptionFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn room(mut self, room: impl Into<String>) -> Self {
        self.room = Some(room.into());
        self
    }
}

/// A decoded pub/sub or broadcast delivery.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Message<P> {
    pub params: P,
    pub meta: Meta,
    pub uuid: String,
    pub method: String,
    pub subject: String,
}

/// Decoded messages from a subscription; dropping it unsubscribes.
pub struct Subscription<P> {
    stream: BoxStream<'static, Inbound>,
    core: Arc<dyn Unpacker>,
    method: Option<String>,
    filter: Option<SubscriptionFilter>,
    room: Option<String>,
    _params: PhantomData<fn() -> P>,
}

pub(crate) trait Unpacker: Send + Sync {
    fn unpack(&self, inbound: &Inbound) -> Result<(Codec, Bytes)>;
}

impl<T: Transport> Unpacker for Inner<T> {
    fn unpack(&self, inbound: &Inbound) -> Result<(Codec, Bytes)> {
        self.core.unpack(inbound)
    }
}

impl<P> fmt::Debug for Subscription<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription")
            .field("method", &self.method)
            .finish()
    }
}

impl<P> Subscription<P> {
    #[cfg_attr(not(feature = "nats"), allow(dead_code))]
    pub(crate) fn raw(stream: BoxStream<'static, Inbound>, unpacker: Arc<dyn Unpacker>) -> Self {
        Self {
            stream,
            core: unpacker,
            method: None,
            filter: None,
            room: None,
            _params: PhantomData,
        }
    }
}

impl<P: DeserializeOwned> Subscription<P> {
    fn decode(&self, inbound: Inbound) -> Result<Option<Message<P>>> {
        let (codec, raw) = self.core.unpack(&inbound)?;
        let head: crate::wire::RequestHead = codec.decode(&raw)?;
        if let Some(wanted) = &self.method {
            let base = Method::parse(&head.method).name;
            if head.method != *wanted && base != wanted {
                return Ok(None);
            }
        }
        let meta = head.meta.unwrap_or_default();
        if let Some(filter) = &self.filter
            && !filter.matches(Some(&meta))
        {
            return Ok(None);
        }
        if let Some(room) = &self.room
            && meta.header(HEADER_ROOM) != Some(room.as_str())
        {
            return Ok(None);
        }
        let request: Request<P> = codec.decode(&raw)?;
        Ok(Some(Message {
            params: request.params,
            meta,
            uuid: head.uuid,
            method: head.method,
            subject: inbound.subject,
        }))
    }
}

impl<P: DeserializeOwned> Stream for Subscription<P> {
    type Item = Result<Message<P>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(inbound)) => match self.decode(inbound) {
                    Ok(Some(message)) => return Poll::Ready(Some(Ok(message))),
                    Ok(None) => continue,
                    Err(err) => return Poll::Ready(Some(Err(err))),
                },
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<T: Transport> Client<T> {
    /// Wraps a connected transport. Must be called inside a Tokio runtime.
    pub fn new(transport: T, config: ClientConfig) -> Self {
        let discovery = config.discovery.clone().map(|config| DiscoveryState {
            registry: Arc::new(DiscoveryRegistry::with_capacity(
                config.ttl,
                config.max_entries,
            )),
            config,
        });
        let client = Self {
            inner: Arc::new(Inner {
                core: Core::new(config),
                transport,
                discovery,
                tasks: Mutex::new(Vec::new()),
            }),
        };
        client.start_discovery();
        client
    }

    pub fn transport(&self) -> &T {
        &self.inner.transport
    }

    pub fn config(&self) -> &ClientConfig {
        &self.inner.core.config
    }

    pub fn instance_id(&self) -> &str {
        &self.inner.core.config.instance_id
    }

    pub fn circuit_breaker(&self) -> &CircuitBreaker {
        &self.inner.core.breaker
    }

    pub fn response_cache(&self) -> &ResponseCache {
        &self.inner.core.cache
    }

    #[cfg_attr(not(feature = "nats"), allow(dead_code))]
    pub(crate) fn core(&self) -> &Core {
        &self.inner.core
    }

    #[cfg_attr(not(feature = "nats"), allow(dead_code))]
    pub(crate) fn unpacker(&self) -> Arc<dyn Unpacker> {
        self.inner.clone()
    }

    /// Validates and normalises a target service name.
    pub fn target(&self, service: &str) -> Result<String> {
        let normalized = service.to_ascii_lowercase();
        let services = &self.inner.core.config.services;
        if normalized != BROADCAST_TOPIC && !services.is_empty() && !services.contains(&normalized)
        {
            return Err(Error::new(
                ErrorCode::ServiceNotFound,
                format!("Service {service:?} is not registered in this client"),
            )
            .with_detail("availableServices", services.clone()));
        }
        Ok(normalized)
    }

    pub async fn query<P, R>(&self, service: &str, method: &str, params: &P) -> Result<R>
    where
        P: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.query_with(service, method, params, CallOptions::default())
            .await
    }

    pub async fn query_with<P, R>(
        &self,
        service: &str,
        method: &str,
        params: &P,
        opts: CallOptions,
    ) -> Result<R>
    where
        P: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let service = self.target(service)?;
        let core = &self.inner.core;
        let transport = &self.inner.transport;
        let timeout = opts.timeout.unwrap_or(core.config.timeout);
        let cache_key = opts
            .idempotency_key
            .as_deref()
            .filter(|_| core.cache.is_enabled())
            .map(|k| client_key(&service, method, k));
        if let Some(key) = &cache_key
            && let Some(entry) = core.cache.get(key)
        {
            return Core::unwrap_response(entry.codec, &entry.payload, &service);
        }

        let retry_key = opts
            .idempotency_key
            .clone()
            .unwrap_or_else(|| uuid::Uuid::now_v7().to_string());
        let breaker_key = format!("{service}:{method}");
        core.breaker.before(&breaker_key)?;
        let retry = opts
            .retry
            .clone()
            .unwrap_or_else(|| core.config.retry.clone());
        let hedge_policy = opts.hedge.or(core.config.hedge);

        let attempt = |attempt: u32| {
            let mut call = opts.clone();
            call.idempotency_key = Some(retry_key.clone());
            call.headers
                .insert(HEADER_ATTEMPT.to_owned(), attempt.to_string());
            let service = service.clone();
            let cache_key = cache_key.clone();
            async move {
                let encoded = core.encode_request(method, params, MessageType::Query, &call)?;
                let span = tracing::debug_span!(
                    "nevo.client.query",
                    service = %service,
                    method = %method,
                    uuid = %encoded.uuid,
                    attempt,
                );
                let inbound = transport
                    .request(&service, encoded.outbound(), timeout)
                    .instrument(span)
                    .await
                    .map_err(|err| remap_transport_error(err, &service, method, timeout))?;
                let (codec, raw) = core.unpack(&inbound)?;
                let value = Core::unwrap_response::<R>(codec, &raw, &service)?;
                if let Some(key) = &cache_key {
                    core.cache.insert(
                        key,
                        IdempotencyEntry {
                            codec,
                            payload: raw,
                            fingerprint: None,
                        },
                    );
                }
                metrics::increment(
                    metrics::REQUESTS_TOTAL,
                    vec![
                        ("transport", transport.name().to_owned()),
                        ("service", service.clone()),
                        ("method", Method::parse(method).name.to_owned()),
                        ("role", "client".into()),
                    ],
                );
                if attempt > 1 {
                    metrics::increment(
                        metrics::RETRIES_TOTAL,
                        vec![
                            ("transport", transport.name().to_owned()),
                            ("service", service.clone()),
                            ("method", Method::parse(method).name.to_owned()),
                        ],
                    );
                }
                Ok(value)
            }
        };

        let attempt = &attempt;
        let result = core
            .inflight
            .track(with_retry(&retry, |n| async move {
                match hedge_policy {
                    Some(policy) => hedge(policy, |_| attempt(n)).await,
                    None => attempt(n).await,
                }
            }))
            .await;
        match &result {
            Ok(_) => core.breaker.on_success(&breaker_key),
            Err(err) => core.breaker.on_failure(&breaker_key, err),
        }
        result
    }

    pub async fn emit<P: Serialize + ?Sized>(
        &self,
        service: &str,
        method: &str,
        params: &P,
    ) -> Result<()> {
        self.emit_with(service, method, params, CallOptions::default())
            .await
    }

    pub async fn emit_with<P: Serialize + ?Sized>(
        &self,
        service: &str,
        method: &str,
        params: &P,
        opts: CallOptions,
    ) -> Result<()> {
        let service = self.target(service)?;
        self.dispatch(
            &service,
            method,
            params,
            MessageType::Emit,
            opts,
            |encoded| self.inner.transport.send(&service, encoded.outbound()),
        )
        .await
    }

    pub async fn publish<P: Serialize + ?Sized>(
        &self,
        service: &str,
        method: &str,
        params: &P,
    ) -> Result<()> {
        self.publish_with(service, method, params, CallOptions::default())
            .await
    }

    pub async fn publish_with<P: Serialize + ?Sized>(
        &self,
        service: &str,
        method: &str,
        params: &P,
        opts: CallOptions,
    ) -> Result<()> {
        let service = self.target(service)?;
        self.dispatch(
            &service,
            method,
            params,
            MessageType::Sub,
            opts,
            |encoded| {
                self.inner
                    .transport
                    .publish(&service, method, encoded.outbound())
            },
        )
        .await
    }

    async fn dispatch<'a, P, F, Fut>(
        &'a self,
        service: &'a str,
        method: &'a str,
        params: &'a P,
        kind: MessageType,
        opts: CallOptions,
        send: F,
    ) -> Result<()>
    where
        P: Serialize + ?Sized,
        F: Fn(&Encoded) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let core = &self.inner.core;
        let mut opts = opts;
        if opts.idempotency_key.is_none() {
            opts.idempotency_key = Some(uuid::Uuid::now_v7().to_string());
        }
        let breaker_key = format!("{service}:{method}");
        core.breaker.before(&breaker_key)?;
        let retry = opts
            .retry
            .clone()
            .unwrap_or_else(|| core.config.retry.clone());
        let result = core
            .inflight
            .track(with_retry(&retry, |_| async {
                let encoded = core.encode_request(method, params, kind, &opts)?;
                send(&encoded).await
            }))
            .await;
        match &result {
            Ok(_) => core.breaker.on_success(&breaker_key),
            Err(err) => core.breaker.on_failure(&breaker_key, err),
        }
        result
    }

    pub async fn broadcast<P: Serialize + ?Sized>(&self, method: &str, params: &P) -> Result<()> {
        self.broadcast_with(method, params, CallOptions::default())
            .await
    }

    pub async fn broadcast_with<P: Serialize + ?Sized>(
        &self,
        method: &str,
        params: &P,
        opts: CallOptions,
    ) -> Result<()> {
        let encoded =
            self.inner
                .core
                .encode_request(method, params, MessageType::Broadcast, &opts)?;
        self.inner
            .transport
            .broadcast(method, encoded.outbound())
            .await
    }

    /// Subscribes to `service`'s published `method`; use `"__broadcast"` for broadcasts.
    pub async fn subscribe<P: DeserializeOwned>(
        &self,
        service: &str,
        method: &str,
        opts: SubscribeOptions,
    ) -> Result<Subscription<P>> {
        let service = self.target(service)?;
        let stream = self
            .inner
            .transport
            .subscribe(&service, method, opts.queue_group.as_deref())
            .await?;
        let is_pattern = method.contains(['*', '>']);
        Ok(Subscription {
            stream,
            core: self.inner.clone(),
            method: (!method.is_empty() && !is_pattern).then(|| method.to_owned()),
            filter: opts.filter.filter(|f| !f.is_empty()),
            room: opts.room,
            _params: PhantomData,
        })
    }

    pub fn discovered(&self) -> Vec<DiscoveryEntry> {
        self.inner
            .discovery
            .as_ref()
            .map(|d| {
                d.registry.prune();
                d.registry.list()
            })
            .unwrap_or_default()
    }

    pub fn is_service_available(&self, service: &str) -> bool {
        self.inner
            .discovery
            .as_ref()
            .is_some_and(|d| d.registry.is_available(&service.to_ascii_lowercase()))
    }

    pub fn discovery_registry(&self) -> Option<&Arc<DiscoveryRegistry>> {
        self.inner.discovery.as_ref().map(|d| &d.registry)
    }

    pub async fn flush(&self) -> Result<()> {
        self.inner.transport.flush().await
    }

    /// Drains in-flight calls (bounded by `timeout`), stops background tasks and closes the transport.
    pub async fn close(&self, timeout: Duration) -> Result<()> {
        let core = &self.inner.core;
        core.inflight.begin_shutdown();
        if !core.inflight.drain(timeout).await {
            tracing::warn!(
                event = "client.drain_timeout",
                remaining = core.inflight.count(),
                "closing with calls still in flight"
            );
        }
        for task in self.inner.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
        self.inner.transport.close().await
    }

    fn start_discovery(&self) {
        let Some(state) = &self.inner.discovery else {
            return;
        };
        let inner = Arc::clone(&self.inner);
        let registry = Arc::clone(&state.registry);
        let listener = tokio::spawn(async move {
            let mut stream = match inner.transport.subscribe_raw(DISCOVERY_TOPIC).await {
                Ok(stream) => stream,
                Err(err) => {
                    tracing::error!(event = "discovery.subscribe_failed", error = %err);
                    return;
                }
            };
            use futures::StreamExt;
            while let Some(inbound) = stream.next().await {
                let outcome = inner
                    .core
                    .unpack(&inbound)
                    .and_then(|(codec, raw)| codec.decode::<Announcement>(&raw));
                match outcome {
                    Ok(announcement) => registry.update(announcement),
                    Err(err) => tracing::debug!(event = "discovery.parse_error", error = %err),
                }
            }
        });
        let inner = Arc::clone(&self.inner);
        let heartbeat = tokio::spawn(async move {
            let Some(state) = &inner.discovery else {
                return;
            };
            let mut ticker = tokio::time::interval(state.config.heartbeat);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                state.registry.prune();
                let config = &inner.core.config;
                let mut announcement = Announcement::new(
                    config
                        .service_name
                        .clone()
                        .unwrap_or_else(|| "unknown".into()),
                    config.instance_id.clone(),
                    inner.transport.name(),
                );
                announcement.client_id = config.service_name.clone();
                announcement.host = state.config.host.clone();
                announcement.port = state.config.port;
                announcement.version = state.config.version.clone();
                announcement.capabilities = (!state.config.capabilities.is_empty())
                    .then(|| state.config.capabilities.clone());
                let payload = match config.codec.encode(&announcement) {
                    Ok(payload) => payload,
                    Err(err) => {
                        tracing::error!(event = "discovery.encode_failed", error = %err);
                        continue;
                    }
                };
                if let Err(err) = inner
                    .transport
                    .publish_raw(DISCOVERY_TOPIC, Outbound::new(payload))
                    .await
                {
                    tracing::warn!(event = "discovery.publish_failed", error = %err);
                }
            }
        });
        self.inner
            .tasks
            .lock()
            .unwrap()
            .extend([listener, heartbeat]);
    }
}

fn remap_transport_error(err: Error, service: &str, method: &str, timeout: Duration) -> Error {
    match err.code() {
        ErrorCode::Timeout if err.service().is_none() => Error::timeout(service, method, timeout),
        _ if err.service().is_none() => err.with_service(service),
        _ => err,
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_carries_identity_and_chain() {
        let core = Core::new(
            ClientConfig::new()
                .service_name("caller")
                .auth_token("t")
                .trace_context(true),
        );
        let opts = CallOptions::new()
            .idempotency_key("k")
            .tenant_id("t1")
            .header("x", "y");
        let meta = core.build_meta(MessageType::Query, &opts);
        assert_eq!(meta.kind, Some(MessageType::Query));
        assert_eq!(meta.service.as_deref(), Some("caller"));
        assert_eq!(meta.auth_token(), Some("t"));
        assert_eq!(meta.idempotency_key.as_deref(), Some("k"));
        assert_eq!(meta.tenant_id.as_deref(), Some("t1"));
        assert_eq!(meta.header("x"), Some("y"));
        assert_eq!(meta.nevo_chain_id.as_ref().unwrap().len(), 36);
        assert_eq!(meta.traceparent().unwrap().len(), 55);
    }

    #[test]
    fn encode_request_versions_method_and_respects_payload_limit() {
        let core = Core::new(ClientConfig::new().codec(Codec::Json));
        let encoded = core
            .encode_request(
                "user.get",
                &serde_json::json!({"id": 1}),
                MessageType::Query,
                &CallOptions::new(),
            )
            .unwrap();
        assert_eq!(encoded.method, "user.get@v1");
        let envelope: Request<serde_json::Value> = Codec::Json.decode(&encoded.payload).unwrap();
        assert_eq!(envelope.method, "user.get@v1");
        assert_eq!(envelope.meta.unwrap().kind, Some(MessageType::Query));
        let tiny = Core::new(ClientConfig::new().max_payload_bytes(8));
        let err = tiny
            .encode_request(
                "m",
                &"x".repeat(100),
                MessageType::Emit,
                &CallOptions::new(),
            )
            .unwrap_err();
        assert_eq!(err.code(), ErrorCode::PayloadTooLarge);
    }

    #[test]
    fn unwrap_response_surfaces_remote_errors() {
        let err = Error::new(ErrorCode::ValidationFailed, "bad").with_service("remote");
        let raw = Codec::Json
            .encode(&Response::from_error("u", "m", &err))
            .unwrap();
        let out = Core::unwrap_response::<u8>(Codec::Json, &raw, "svc").unwrap_err();
        assert_eq!(out.code(), ErrorCode::ValidationFailed);
        assert_eq!(out.service(), Some("remote"));
        let ok = Codec::Json
            .encode(&Response::success("u", "m", 7u8))
            .unwrap();
        assert_eq!(
            Core::unwrap_response::<u8>(Codec::Json, &ok, "svc").unwrap(),
            7
        );
    }
}
