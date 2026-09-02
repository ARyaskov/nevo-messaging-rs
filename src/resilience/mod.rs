//! Client-side resilience primitives: retry with backoff, circuit breakers and hedging.

mod circuit;
mod hedge;
mod retry;

pub use circuit::{CircuitBreaker, CircuitBreakerConfig, CircuitState, SlidingWindow};
pub use hedge::{HedgePolicy, hedge};
pub use retry::{RetryPolicy, with_retry};
