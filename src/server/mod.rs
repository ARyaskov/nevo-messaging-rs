//! The transport-agnostic request pipeline: replay guard → caller identity →
//! rate limit → ACL → built-ins → idempotency claim → interceptors → handler,
//! with dead-letter and metrics bookkeeping.

mod fingerprint;
mod suggest;

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::Instrument;

use crate::acl::AccessControl;
use crate::codec::Codec;
use crate::compression::{Compression, compress, decompress};
use crate::context::{self, ChainContext};
use crate::contract::{
    CONTRACT_METHOD, CONTRACT_PROTOCOL_VERSION, ContractMethod, ServiceContract,
};
use crate::dlq::{DlqEntry, DlqRouter};
use crate::error::{Error, ErrorCode, ErrorDetails, Result};
use crate::health::{HEALTH_METHOD, HealthRegistry, LIVENESS_METHOD, READINESS_METHOD};
use crate::idempotency::{
    Begin, IdempotencyConfig, IdempotencyEntry, IdempotencyStore, TwoTierIdempotency, server_key,
};
use crate::metrics;
use crate::rate_limit::{RateLimitConfig, RateLimitContext, RateLimiter};
use crate::replay::ReplayGuard;
use crate::shutdown::Inflight;
use crate::time::unix_millis;
use crate::transport::{Inbound, Outbound};
use crate::wire::{
    DEFAULT_MAX_PAYLOAD_BYTES, DEFAULT_VERSION, EVENTS_SUFFIX, Meta, Method, Request, RequestHead,
    Response,
};

pub use fingerprint::Fingerprint;

/// Everything a handler knows about the inbound envelope besides its params.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Context {
    pub uuid: String,
    pub method: String,
    pub name: String,
    pub version: String,
    pub service: String,
    pub topic: String,
    pub meta: Meta,
    pub caller: Option<String>,
    pub chain_id: String,
    pub received_at: Instant,
}

impl Context {
    pub fn tenant_id(&self) -> Option<&str> {
        self.meta.tenant_id.as_deref()
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.meta.header(name)
    }

    pub fn idempotency_key(&self) -> Option<&str> {
        self.meta.idempotency_key.as_deref()
    }
}

/// An async function from `(Context, params)` to a serialisable result.
pub trait Handler<P, R>: Send + Sync + 'static {
    fn call(&self, ctx: Context, params: P) -> BoxFuture<'static, Result<R>>;
}

impl<F, Fut, P, R> Handler<P, R> for F
where
    F: Fn(Context, P) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<R>> + Send + 'static,
    P: Send + 'static,
    R: Send + 'static,
{
    fn call(&self, ctx: Context, params: P) -> BoxFuture<'static, Result<R>> {
        Box::pin(self(ctx, params))
    }
}

/// What a handler produced, as seen by [`Interceptor::after`].
#[derive(Debug)]
pub struct Outcome<'a> {
    pub error: Option<&'a Error>,
    pub duration: Duration,
}

impl Outcome<'_> {
    pub fn is_ok(&self) -> bool {
        self.error.is_none()
    }
}

/// Hooks around every dispatched request. `before` may reject the request.
pub trait Interceptor: Send + Sync + 'static {
    fn before<'a>(&'a self, ctx: &'a Context) -> BoxFuture<'a, Result<()>> {
        let _ = ctx;
        Box::pin(async { Ok(()) })
    }

    fn after<'a>(&'a self, ctx: &'a Context, outcome: &'a Outcome<'a>) -> BoxFuture<'a, ()> {
        let _ = (ctx, outcome);
        Box::pin(async {})
    }
}

type ErasedHandler =
    Arc<dyn Fn(Codec, Bytes, Context) -> BoxFuture<'static, Result<Bytes>> + Send + Sync>;

struct Route {
    version: String,
    handler: ErasedHandler,
}

fn erase<P, R, H>(handler: H) -> ErasedHandler
where
    P: DeserializeOwned + Send + 'static,
    R: Serialize + Send + 'static,
    H: Handler<P, R>,
{
    let handler = Arc::new(handler);
    Arc::new(move |codec, raw, ctx| {
        let handler = Arc::clone(&handler);
        Box::pin(async move {
            let request: Request<P> = codec.decode(&raw).map_err(|e| {
                Error::validation(format!("Invalid params for {}: {}", ctx.name, e.message()))
            })?;
            let uuid = ctx.uuid.clone();
            let method = ctx.method.clone();
            let meta = ctx.meta.clone();
            let result = handler.call(ctx, request.params).await?;
            codec.encode(&Response::success(uuid, method, result).with_meta(Some(meta)))
        })
    })
}

const PUBLIC_ERROR_DETAIL_KEYS: &[&str] = &[
    "retryable",
    "retryAfterMs",
    "size",
    "limit",
    "httpStatus",
    "maxPhysicalCalls",
    "errors",
];

fn is_production_env() -> bool {
    ["NODE_ENV", "NEVO_ENV", "MODE"].iter().any(|var| {
        std::env::var(var)
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "production" | "prod"))
            .unwrap_or(false)
    })
}

/// A service: its signal handlers plus the request-processing policy around them.
pub struct Router {
    service: String,
    topic: String,
    routes: HashMap<String, Vec<Route>>,
    signal_names: Vec<String>,
    codec: Codec,
    compression: Compression,
    max_payload_bytes: usize,
    replay: ReplayGuard,
    acl: Option<AccessControl>,
    rate_limiter: Option<RateLimiter>,
    idempotency_config: IdempotencyConfig,
    idempotency_store: Option<Arc<dyn IdempotencyStore>>,
    idempotency: TwoTierIdempotency,
    dlq: DlqRouter,
    health: Option<HealthRegistry>,
    interceptors: Vec<Arc<dyn Interceptor>>,
    builtins_enabled: bool,
    expose_errors: bool,
    service_version: Option<String>,
    instance_id: String,
    capabilities: Vec<String>,
    default_version: String,
    inflight: Inflight,
}

impl fmt::Debug for Router {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Router")
            .field("service", &self.service)
            .field("topic", &self.topic)
            .field("signals", &self.signal_names)
            .field("codec", &self.codec)
            .finish()
    }
}

impl Router {
    pub fn new(service: impl AsRef<str>) -> Self {
        let service = service.as_ref().to_ascii_lowercase();
        let topic = format!("{service}{EVENTS_SUFFIX}");
        let idempotency_config = IdempotencyConfig::default();
        Self {
            service,
            topic,
            routes: HashMap::new(),
            signal_names: Vec::new(),
            codec: Codec::default(),
            compression: Compression::default(),
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            replay: ReplayGuard::disabled(),
            acl: None,
            rate_limiter: None,
            idempotency: TwoTierIdempotency::new(&idempotency_config, None),
            idempotency_config,
            idempotency_store: None,
            dlq: DlqRouter::disabled(),
            health: None,
            interceptors: Vec::new(),
            builtins_enabled: true,
            expose_errors: !is_production_env(),
            service_version: None,
            instance_id: uuid::Uuid::new_v4().to_string(),
            capabilities: Vec::new(),
            default_version: DEFAULT_VERSION.to_owned(),
            inflight: Inflight::new(),
        }
    }

    /// Registers a handler for `signal`, optionally versioned as `name@vN`.
    pub fn route<P, R, H>(mut self, signal: &str, handler: H) -> Self
    where
        P: DeserializeOwned + Send + 'static,
        R: Serialize + Send + 'static,
        H: Handler<P, R>,
    {
        let parsed = Method::parse(signal);
        let version = parsed
            .version
            .filter(|v| !v.is_empty())
            .unwrap_or(&self.default_version)
            .to_owned();
        let name = parsed.name.to_owned();
        let routes = self.routes.entry(name.clone()).or_default();
        routes.retain(|r| r.version != version);
        routes.push(Route {
            version,
            handler: erase(handler),
        });
        if !name.starts_with("nevo.") && !self.signal_names.contains(&name) {
            self.signal_names.push(name);
            self.signal_names.sort();
        }
        self
    }

    /// Overrides the request address (defaults to `<service>-events`).
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = topic.into();
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

    pub fn replay_window(mut self, window: Duration) -> Self {
        self.replay = ReplayGuard::new(window);
        self
    }

    pub fn access_control(mut self, acl: AccessControl) -> Self {
        self.acl = Some(acl);
        self
    }

    pub fn rate_limit(mut self, config: RateLimitConfig) -> Self {
        self.rate_limiter = Some(RateLimiter::new(config));
        self
    }

    pub fn idempotency(mut self, config: IdempotencyConfig) -> Self {
        self.idempotency_config = config;
        self.idempotency =
            TwoTierIdempotency::new(&self.idempotency_config, self.idempotency_store.clone());
        self
    }

    pub fn idempotency_store(mut self, store: Arc<dyn IdempotencyStore>) -> Self {
        self.idempotency_store = Some(store);
        self.idempotency =
            TwoTierIdempotency::new(&self.idempotency_config, self.idempotency_store.clone());
        self
    }

    pub fn dlq(mut self, dlq: DlqRouter) -> Self {
        self.dlq = dlq;
        self
    }

    pub fn health(mut self, registry: HealthRegistry) -> Self {
        self.health = Some(registry);
        self
    }

    pub fn interceptor(mut self, interceptor: impl Interceptor) -> Self {
        self.interceptors.push(Arc::new(interceptor));
        self
    }

    pub fn disable_builtins(mut self) -> Self {
        self.builtins_enabled = false;
        self
    }

    /// Whether error details and did-you-mean hints leave the process (default: off in production).
    pub fn expose_errors(mut self, expose: bool) -> Self {
        self.expose_errors = expose;
        self
    }

    pub fn service_version(mut self, version: impl Into<String>) -> Self {
        self.service_version = Some(version.into());
        self
    }

    pub fn instance_id(mut self, id: impl Into<String>) -> Self {
        self.instance_id = id.into();
        self
    }

    pub fn capabilities(mut self, caps: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.capabilities = caps.into_iter().map(Into::into).collect();
        self
    }

    pub fn default_version(mut self, version: impl Into<String>) -> Self {
        self.default_version = version.into();
        self
    }

    pub fn service_name(&self) -> &str {
        &self.service
    }

    pub fn request_topic(&self) -> &str {
        &self.topic
    }

    pub fn signals(&self) -> &[String] {
        &self.signal_names
    }

    pub fn inflight(&self) -> &Inflight {
        &self.inflight
    }

    pub fn contract(&self) -> ServiceContract {
        let mut methods: Vec<ContractMethod> = self
            .routes
            .iter()
            .filter(|(name, _)| !name.starts_with("nevo."))
            .flat_map(|(name, routes)| {
                routes.iter().map(|r| ContractMethod {
                    signal_name: name.clone(),
                    version: r.version.clone(),
                    params_schema: None,
                    result_schema: None,
                })
            })
            .collect();
        methods.sort_by(|a, b| {
            a.signal_name
                .cmp(&b.signal_name)
                .then(a.version.cmp(&b.version))
        });
        ServiceContract {
            protocol: CONTRACT_PROTOCOL_VERSION.to_owned(),
            service_name: self.service.clone(),
            service_version: self.service_version.clone(),
            instance_id: Some(self.instance_id.clone()),
            capabilities: (!self.capabilities.is_empty()).then(|| self.capabilities.clone()),
            generated_at: unix_millis(),
            methods,
        }
    }

    /// Processes one inbound envelope and returns the encoded response.
    pub async fn handle(&self, inbound: Inbound) -> Outbound {
        let started = Instant::now();
        let (codec, bytes) = match self.prepare(&inbound) {
            Ok((codec, raw, head)) => {
                let meta = head.meta.as_ref();
                let chain = ChainContext::new(context::resolve_inbound(
                    meta.and_then(|m| m.nevo_chain_id.as_deref()),
                ))
                .with_parent(head.uuid.clone())
                .with_trace(meta.and_then(|m| m.trace.clone()));
                let bytes = context::scope(chain, self.dispatch(codec, raw, head, started)).await;
                (codec, bytes)
            }
            Err((codec, err)) => {
                tracing::warn!(event = "dispatch.undecodable", error = %err, topic = %self.topic);
                self.record(None, &err.code(), started);
                (codec, self.error_bytes(codec, "", "", None, &err))
            }
        };
        self.finish(codec, bytes)
    }

    fn prepare(&self, inbound: &Inbound) -> Result<(Codec, Bytes, RequestHead), (Codec, Error)> {
        let encoding = inbound.content_encoding().ok_or_else(|| {
            (
                self.codec,
                Error::bad_request("Unsupported content-encoding"),
            )
        })?;
        let raw = decompress(inbound.payload.clone(), encoding, self.max_payload_bytes)
            .map_err(|err| (self.codec, err))?;
        if raw.len() > self.max_payload_bytes {
            return Err((
                self.codec,
                Error::payload_too_large(raw.len(), self.max_payload_bytes),
            ));
        }
        metrics::observe(
            metrics::PAYLOAD_BYTES,
            vec![
                ("direction", "in".into()),
                ("service", self.service.clone()),
            ],
            raw.len() as f64,
        );
        let codec = Codec::detect(&raw).unwrap_or(self.codec);
        let head: RequestHead = codec.decode(&raw).map_err(|err| (codec, err))?;
        if head.method.is_empty() {
            return Err((codec, Error::bad_request("Invalid message format")));
        }
        Ok((codec, raw, head))
    }

    fn finish(&self, _codec: Codec, bytes: Bytes) -> Outbound {
        match compress(bytes, &self.compression) {
            Ok((payload, encoding)) => Outbound::new(payload).encoded(encoding),
            Err(err) => {
                tracing::error!(event = "dispatch.compress_failed", error = %err);
                Outbound::new(Bytes::new())
            }
        }
    }

    fn public_wire(&self, err: &Error) -> ErrorDetails {
        let mut wire = err.to_wire();
        if !self.expose_errors
            && let Some(details) = &mut wire.details
        {
            details.retain(|k, _| PUBLIC_ERROR_DETAIL_KEYS.contains(&k.as_str()));
        }
        if wire.service.is_none() {
            wire.service = Some(self.service.clone());
        }
        wire
    }

    fn error_bytes(
        &self,
        codec: Codec,
        uuid: &str,
        method: &str,
        meta: Option<&Meta>,
        err: &Error,
    ) -> Bytes {
        let response =
            Response::failure(uuid, method, self.public_wire(err)).with_meta(meta.cloned());
        codec.encode(&response).unwrap_or_else(|encode_err| {
            tracing::error!(event = "dispatch.encode_failed", error = %encode_err);
            Bytes::from_static(b"{\"params\":{\"result\":\"error\",\"error\":{\"code\":15,\"message\":\"Internal error\"}}}")
        })
    }

    fn record(&self, method: Option<&str>, code: &ErrorCode, started: Instant) {
        self.record_outcome(method, Some(*code), started);
    }

    fn record_outcome(&self, method: Option<&str>, code: Option<ErrorCode>, started: Instant) {
        let label = method
            .map(|m| Method::parse(m).name)
            .filter(|name| self.routes.contains_key(*name) || name.starts_with("nevo."))
            .unwrap_or(metrics::UNKNOWN_METHOD_LABEL)
            .to_owned();
        let status = if code.is_some() { "error" } else { "ok" };
        let labels = || {
            vec![
                ("service", self.service.clone()),
                ("method", label.clone()),
                ("status", status.to_owned()),
            ]
        };
        metrics::increment(metrics::REQUESTS_TOTAL, labels());
        if code.is_some() {
            metrics::increment(metrics::REQUEST_ERRORS_TOTAL, labels());
        }
        metrics::observe(
            metrics::REQUEST_DURATION_SECONDS,
            labels(),
            started.elapsed().as_secs_f64(),
        );
    }

    async fn dead_letter(&self, reason: &str, err: &Error, meta: &Meta, raw: &Bytes) {
        if !self.dlq.is_enabled() {
            return;
        }
        self.dlq
            .route(
                DlqEntry::new(self.topic.clone(), reason)
                    .error(err)
                    .meta(Some(meta.clone()))
                    .raw_payload(raw.clone()),
            )
            .await;
    }

    async fn builtin(
        &self,
        codec: Codec,
        name: &str,
        uuid: &str,
        method: &str,
        meta: &Meta,
    ) -> Option<Result<Bytes>> {
        let encode = |value: &dyn erased::Payload| value.encode_response(codec, uuid, method, meta);
        if name == CONTRACT_METHOD {
            return Some(encode(&self.contract()));
        }
        let health = self.health.as_ref()?;
        let report = match name {
            HEALTH_METHOD => health.report(None).await,
            LIVENESS_METHOD => health.liveness().await,
            READINESS_METHOD => health.readiness().await,
            _ => return None,
        };
        Some(encode(&report))
    }

    async fn dispatch(
        &self,
        codec: Codec,
        raw: Bytes,
        head: RequestHead,
        started: Instant,
    ) -> Bytes {
        let RequestHead { uuid, method, meta } = head;
        let meta = meta.unwrap_or_default();
        let parsed = Method::parse(&method);
        let name = parsed.name.to_owned();
        let fail = |err: &Error| {
            self.record(Some(&method), &err.code(), started);
            self.error_bytes(codec, &uuid, &method, Some(&meta), err)
        };

        if let Err(err) = self.replay.check(&uuid, meta.ts) {
            self.dead_letter("replay", &err, &meta, &raw).await;
            return fail(&err);
        }

        let caller = match &self.acl {
            Some(acl) => match acl.extract_caller(Some(&meta)).await {
                Ok(caller) => caller,
                Err(err) => return fail(&err),
            },
            None => meta.service.clone(),
        };

        if let Some(limiter) = &self.rate_limiter
            && let Err(err) = limiter.check(&RateLimitContext {
                topic: &self.topic,
                method: &name,
                caller_service: caller.as_deref(),
                tenant_id: meta.tenant_id.as_deref(),
                meta: Some(&meta),
            })
        {
            return fail(&err);
        }

        if let Some(acl) = &self.acl
            && !acl.is_allowed(&self.topic, &name, caller.as_deref())
        {
            acl.log_denial(&self.topic, &method, &self.service, caller.as_deref());
            return fail(&AccessControl::denied_error(
                &method,
                &self.service,
                caller.as_deref(),
            ));
        }

        if self.builtins_enabled
            && let Some(outcome) = self.builtin(codec, &name, &uuid, &method, &meta).await
        {
            return match outcome {
                Ok(bytes) => {
                    self.record_outcome(Some(&method), None, started);
                    bytes
                }
                Err(err) => fail(&err),
            };
        }

        let supplied_key = meta.idempotency_key.as_deref().filter(|k| !k.is_empty());
        let idem_key = if self.idempotency.is_enabled() {
            server_key(
                caller.as_deref(),
                meta.tenant_id.as_deref(),
                &method,
                supplied_key,
                &uuid,
            )
        } else {
            None
        };
        let fingerprint = match (&idem_key, supplied_key) {
            (Some(_), Some(_)) => fingerprint::of_params(codec, &raw),
            _ => None,
        };
        let mut claimed = false;
        if let Some(key) = &idem_key {
            match self.idempotency.begin(key, fingerprint.as_deref()).await {
                Ok(Begin::Hit(entry)) => {
                    self.record_outcome(Some(&method), None, started);
                    return entry.payload;
                }
                Ok(Begin::Conflict) => {
                    return fail(&Error::new(
                        ErrorCode::IdempotencyKeyConflict,
                        format!(
                            "Idempotency key was already used for a different request payload on {method}"
                        ),
                    ));
                }
                Ok(Begin::Execute) => claimed = true,
                Err(err) => return fail(&err),
            }
        }

        let ctx = Context {
            uuid: uuid.clone(),
            method: method.clone(),
            name: name.clone(),
            version: parsed.version.unwrap_or(&self.default_version).to_owned(),
            service: self.service.clone(),
            topic: self.topic.clone(),
            meta: meta.clone(),
            caller,
            chain_id: context::current_chain_id().unwrap_or_default(),
            received_at: started,
        };

        let route = match self.resolve_route(&parsed) {
            Ok(route) => route,
            Err(err) => {
                if claimed && let Some(key) = &idem_key {
                    self.idempotency.release(key).await;
                }
                return fail(&err);
            }
        };

        for interceptor in &self.interceptors {
            if let Err(err) = interceptor.before(&ctx).await {
                if claimed && let Some(key) = &idem_key {
                    self.idempotency.release(key).await;
                }
                return fail(&err);
            }
        }

        let span = tracing::info_span!(
            "nevo.serve",
            service = %self.service,
            method = %method,
            uuid = %uuid,
            caller = ctx.caller.as_deref().unwrap_or(""),
        );
        let outcome = self
            .inflight
            .track((route.handler)(codec, raw.clone(), ctx.clone()))
            .instrument(span)
            .await;

        let report = Outcome {
            error: outcome.as_ref().err(),
            duration: started.elapsed(),
        };
        for interceptor in &self.interceptors {
            interceptor.after(&ctx, &report).await;
        }

        match outcome {
            Ok(bytes) => {
                if claimed && let Some(key) = &idem_key {
                    self.idempotency
                        .commit(
                            key,
                            IdempotencyEntry {
                                codec,
                                payload: bytes.clone(),
                                fingerprint,
                            },
                        )
                        .await;
                }
                self.record_outcome(Some(&method), None, started);
                bytes
            }
            Err(err) => {
                if claimed && let Some(key) = &idem_key {
                    self.idempotency.release(key).await;
                }
                if err.code() != ErrorCode::RateLimited {
                    tracing::error!(
                        event = "dispatch.error",
                        topic = %self.topic,
                        method = %method,
                        code = ?err.code(),
                        error = %err,
                        "handler failed"
                    );
                    self.dead_letter("handler-error", &err, &meta, &raw).await;
                }
                fail(&err)
            }
        }
    }

    fn resolve_route(&self, parsed: &Method<'_>) -> Result<&Route> {
        let Some(candidates) = self.routes.get(parsed.name).filter(|c| !c.is_empty()) else {
            let suggestion = self
                .expose_errors
                .then(|| {
                    suggest::closest(parsed.name, self.signal_names.iter().map(String::as_str))
                })
                .flatten();
            let message = match suggestion {
                Some(s) => format!("Invalid method name '{}', did you mean '{s}'?", parsed.name),
                None => format!("Method {} not found", parsed.name),
            };
            return Err(Error::new(ErrorCode::MethodNotFound, message).with_service(&self.service));
        };
        match parsed.version {
            Some(wanted) => candidates
                .iter()
                .find(|r| r.version == wanted)
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::UnsupportedVersion,
                        format!("No handler matching version {wanted} for {}", parsed.name),
                    )
                    .with_service(&self.service)
                }),
            None => Ok(candidates
                .iter()
                .find(|r| r.version == self.default_version)
                .unwrap_or(&candidates[0])),
        }
    }
}

mod erased {
    use bytes::Bytes;
    use serde::Serialize;

    use crate::codec::Codec;
    use crate::error::Result;
    use crate::wire::{Meta, Response};

    pub(super) trait Payload {
        fn encode_response(
            &self,
            codec: Codec,
            uuid: &str,
            method: &str,
            meta: &Meta,
        ) -> Result<Bytes>;
    }

    impl<T: Serialize> Payload for T {
        fn encode_response(
            &self,
            codec: Codec,
            uuid: &str,
            method: &str,
            meta: &Meta,
        ) -> Result<Bytes> {
            codec.encode(&Response::success(uuid, method, self).with_meta(Some(meta.clone())))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::ResponseProbe;
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Deserialize)]
    struct GetById {
        id: u64,
    }

    #[derive(Serialize)]
    struct User {
        id: u64,
        name: String,
    }

    fn router() -> Router {
        Router::new("User")
            .route("user.getById", |_ctx: Context, p: GetById| async move {
                Ok(User {
                    id: p.id,
                    name: "Eddie".into(),
                })
            })
            .route("user.getById@v2", |_ctx: Context, p: GetById| async move {
                Ok(json!({"id": p.id, "v": 2}))
            })
            .route(
                "user.fail",
                |_ctx: Context, _p: serde_json::Value| async move {
                    Err::<(), _>(Error::internal("boom"))
                },
            )
    }

    async fn call(
        router: &Router,
        codec: Codec,
        req: Request<serde_json::Value>,
    ) -> (Codec, serde_json::Value) {
        let raw = codec.encode(&req).unwrap();
        let out = router.handle(Inbound::new("user-events", raw)).await;
        let codec = Codec::detect(&out.payload).unwrap();
        (codec, codec.decode(&out.payload).unwrap())
    }

    #[tokio::test]
    async fn dispatches_and_echoes_meta_in_the_inbound_codec() {
        let router = router();
        let mut meta = Meta::new();
        meta.service = Some("caller".into());
        let req = Request::new("u1", "user.getById@v1", json!({"id": 7})).with_meta(meta);
        let (codec, out) = call(&router, Codec::Json, req).await;
        assert_eq!(codec, Codec::Json);
        assert_eq!(out["uuid"], "u1");
        assert_eq!(out["method"], "user.getById@v1");
        assert_eq!(out["params"]["result"]["name"], "Eddie");
        assert_eq!(out["meta"]["service"], "caller");
        assert_eq!(router.request_topic(), "user-events");
    }

    #[tokio::test]
    async fn version_dispatch_and_errors() {
        let router = router();
        let (_, v2) = call(
            &router,
            Codec::Json,
            Request::new("u", "user.getById@v2", json!({"id": 1})),
        )
        .await;
        assert_eq!(v2["params"]["result"]["v"], 2);
        let (_, missing) = call(
            &router,
            Codec::Json,
            Request::new("u", "user.getByI", json!({})),
        )
        .await;
        assert_eq!(missing["params"]["result"], "error");
        assert_eq!(missing["params"]["error"]["code"], 3);
        assert!(
            missing["params"]["error"]["message"]
                .as_str()
                .unwrap()
                .contains("did you mean")
        );
        let (_, bad_version) = call(
            &router,
            Codec::Json,
            Request::new("u", "user.getById@v9", json!({"id": 1})),
        )
        .await;
        assert_eq!(bad_version["params"]["error"]["code"], 17);
        let (_, invalid) = call(
            &router,
            Codec::Json,
            Request::new("u", "user.getById", json!({"id": "x"})),
        )
        .await;
        assert_eq!(invalid["params"]["error"]["code"], 7);
        let (_, failed) = call(
            &router,
            Codec::Json,
            Request::new("u", "user.fail", json!({})),
        )
        .await;
        assert_eq!(failed["params"]["error"]["code"], 15);
        assert_eq!(failed["params"]["error"]["message"], "boom");
        assert_eq!(failed["params"]["error"]["service"], "user");
    }

    #[tokio::test]
    async fn builtins_and_bad_envelopes() {
        let router = router().health(HealthRegistry::new("user"));
        let (_, contract) = call(
            &router,
            Codec::Json,
            Request::new("u", "nevo.contract", json!({})),
        )
        .await;
        let methods = contract["params"]["result"]["methods"].as_array().unwrap();
        assert_eq!(methods.len(), 3);
        assert_eq!(contract["params"]["result"]["protocol"], "1");
        let (_, health) = call(
            &router,
            Codec::Json,
            Request::new("u", "nevo.ready", json!({})),
        )
        .await;
        assert_eq!(health["params"]["result"]["status"], "ok");
        let out = router
            .handle(Inbound::new("user-events", Bytes::from_static(b"garbage")))
            .await;
        let codec = Codec::detect(&out.payload).unwrap();
        let probe: ResponseProbe = codec.decode(&out.payload).unwrap();
        assert_eq!(probe.params.error.unwrap().code, ErrorCode::ParseError);
        let out = router
            .handle(Inbound::new(
                "user-events",
                Bytes::from_static(b"{\"uuid\":\"x\"}"),
            ))
            .await;
        let probe: ResponseProbe = Codec::Json.decode(&out.payload).unwrap();
        assert_eq!(probe.params.error.unwrap().code, ErrorCode::BadRequest);
    }

    #[tokio::test]
    async fn idempotent_replay_and_conflict() {
        let counter = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let seen = Arc::clone(&counter);
        let router =
            Router::new("pay").route("pay.charge", move |_ctx: Context, p: serde_json::Value| {
                let seen = Arc::clone(&seen);
                async move {
                    seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(json!({"charged": p["amount"]}))
                }
            });
        let mut meta = Meta::new();
        meta.idempotency_key = Some("k1".into());
        let first = Request::new("u1", "pay.charge", json!({"amount": 10})).with_meta(meta.clone());
        let retry = Request::new("u2", "pay.charge", json!({"amount": 10})).with_meta(meta.clone());
        let different = Request::new("u3", "pay.charge", json!({"amount": 99})).with_meta(meta);
        let (_, a) = call(&router, Codec::Json, first).await;
        let (_, b) = call(&router, Codec::Json, retry).await;
        assert_eq!(a["params"]["result"]["charged"], 10);
        assert_eq!(b["params"]["result"]["charged"], 10);
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);
        let (_, c) = call(&router, Codec::Json, different).await;
        assert_eq!(c["params"]["error"]["code"], 20);
    }

    #[tokio::test]
    async fn acl_rate_limit_and_replay_guard() {
        use crate::acl::{AccessControl, AccessRule};
        let router = Router::new("user")
            .route(
                "user.get",
                |_ctx: Context, _p: serde_json::Value| async move { Ok(1) },
            )
            .access_control(AccessControl::new().rule(AccessRule::new().allow(["frontend"])))
            .rate_limit(RateLimitConfig::new(1.0, 0.0))
            .replay_window(Duration::from_secs(60));
        let mut meta = Meta::new();
        meta.service = Some("frontend".into());
        meta.ts = Some(unix_millis());
        let ok = Request::new("r1", "user.get", json!({})).with_meta(meta.clone());
        let (_, out) = call(&router, Codec::Json, ok.clone()).await;
        assert_eq!(out["params"]["result"], 1);
        let (_, replay) = call(&router, Codec::Json, ok).await;
        assert_eq!(replay["params"]["error"]["code"], 12);
        let limited = Request::new("r2", "user.get", json!({})).with_meta(meta.clone());
        let (_, out) = call(&router, Codec::Json, limited).await;
        assert_eq!(out["params"]["error"]["code"], 8);
        meta.service = Some("stranger".into());
        let denied = Request::new("r3", "user.get", json!({})).with_meta(meta);
        let (_, out) = call(&router, Codec::Json, denied).await;
        assert_eq!(out["params"]["error"]["code"], 1);
    }

    #[cfg(feature = "msgpack")]
    #[tokio::test]
    async fn msgpack_request_gets_msgpack_reply() {
        let router = router();
        let (codec, out) = call(
            &router,
            Codec::MsgPack,
            Request::new("u", "user.getById", json!({"id": 3})),
        )
        .await;
        assert_eq!(codec, Codec::MsgPack);
        assert_eq!(out["params"]["result"]["id"], 3);
    }
}
