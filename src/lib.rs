#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod acl;
pub mod client;
pub mod codec;
pub mod compression;
pub mod context;
pub mod contract;
pub mod discovery;
pub mod dlq;
pub mod error;
pub mod filter;
pub mod health;
pub mod idempotency;
mod lru;
pub mod metrics;
pub mod rate_limit;
pub mod redact;
pub mod replay;
pub mod resilience;
pub mod server;
pub mod shutdown;
mod time;
pub mod transport;
pub mod wire;

pub use client::{CallOptions, Client, ClientConfig, Message, SubscribeOptions, Subscription};
pub use codec::Codec;
pub use compression::{Compression, Encoding};
pub use error::{Error, ErrorCode, ErrorDetails, Result};
pub use server::{Context, Handler, Interceptor, Outcome, Router};
#[cfg(feature = "nats")]
#[cfg_attr(docsrs, doc(cfg(feature = "nats")))]
pub use transport::nats::{
    Nats, NatsAuth, NatsClient, NatsClientBuilder, NatsConfig, NatsServer, NatsServerBuilder,
};
pub use wire::{BigInt, MessageType, Meta, Request, Response};

/// The names most services need.
pub mod prelude {
    pub use crate::client::{CallOptions, Client, ClientConfig, Message, SubscribeOptions};
    pub use crate::codec::Codec;
    pub use crate::compression::Compression;
    pub use crate::error::{Error, ErrorCode, Result};
    pub use crate::filter::SubscriptionFilter;
    pub use crate::health::{CheckResult, HealthKind, HealthRegistry};
    pub use crate::resilience::{CircuitBreakerConfig, HedgePolicy, RetryPolicy};
    pub use crate::server::{Context, Interceptor, Router};
    #[cfg(feature = "nats")]
    pub use crate::transport::nats::{NatsClient, NatsConfig, NatsServer};
    pub use crate::wire::{BigInt, Meta};
}
