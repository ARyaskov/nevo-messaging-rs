use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::error::{Error, ErrorCode, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half-open",
        }
    }
}

/// Error-rate accounting over a rolling window instead of consecutive failures.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct SlidingWindow {
    pub window: Duration,
    pub bucket: Duration,
    pub error_rate_threshold: f64,
    pub min_sample_size: u32,
}

impl Default for SlidingWindow {
    fn default() -> Self {
        Self {
            window: Duration::from_secs(10),
            bucket: Duration::from_secs(1),
            error_rate_threshold: 0.5,
            min_sample_size: 20,
        }
    }
}

impl SlidingWindow {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn window(mut self, window: Duration) -> Self {
        self.window = window;
        self
    }

    pub fn error_rate_threshold(mut self, rate: f64) -> Self {
        self.error_rate_threshold = rate;
        self
    }

    pub fn min_sample_size(mut self, size: u32) -> Self {
        self.min_sample_size = size;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct CircuitBreakerConfig {
    pub enabled: bool,
    pub failure_threshold: u32,
    pub reset_timeout: Duration,
    pub half_open_success_threshold: u32,
    pub max_keys: usize,
    pub sliding: Option<SlidingWindow>,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            failure_threshold: 5,
            reset_timeout: Duration::from_secs(10),
            half_open_success_threshold: 1,
            max_keys: 10_000,
            sliding: None,
        }
    }
}

impl CircuitBreakerConfig {
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Opens after `failure_threshold` consecutive failures.
    pub fn consecutive(failure_threshold: u32) -> Self {
        Self {
            enabled: true,
            failure_threshold,
            ..Self::default()
        }
    }

    pub fn sliding(window: SlidingWindow) -> Self {
        Self {
            enabled: true,
            sliding: Some(window),
            ..Self::default()
        }
    }

    pub fn reset_timeout(mut self, timeout: Duration) -> Self {
        self.reset_timeout = timeout;
        self
    }

    pub fn half_open_success_threshold(mut self, successes: u32) -> Self {
        self.half_open_success_threshold = successes.max(1);
        self
    }

    pub fn max_keys(mut self, keys: usize) -> Self {
        self.max_keys = keys.max(1);
        self
    }
}

#[derive(Debug)]
struct Bucket {
    started: Instant,
    success: u32,
    failure: u32,
}

#[derive(Debug)]
struct Circuit {
    state: CircuitState,
    opened_at: Instant,
    half_open_inflight: bool,
    probe_at: Instant,
    consecutive_failures: u32,
    half_open_successes: u32,
    buckets: VecDeque<Bucket>,
}

impl Circuit {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            state: CircuitState::Closed,
            opened_at: now,
            half_open_inflight: false,
            probe_at: now,
            consecutive_failures: 0,
            half_open_successes: 0,
            buckets: VecDeque::new(),
        }
    }
}

/// Per-key breaker registry keyed by `service:method`.
#[derive(Debug)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    circuits: Mutex<HashMap<String, Circuit>>,
}

impl CircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            circuits: Mutex::new(HashMap::new()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn config(&self) -> &CircuitBreakerConfig {
        &self.config
    }

    pub fn state(&self, key: &str) -> Option<CircuitState> {
        self.circuits.lock().unwrap().get(key).map(|c| c.state)
    }

    pub fn snapshot(&self) -> Vec<(String, CircuitState)> {
        self.circuits
            .lock()
            .unwrap()
            .iter()
            .map(|(k, c)| (k.clone(), c.state))
            .collect()
    }

    fn open_error(key: &str) -> Error {
        let (service, method) = key.split_once(':').unwrap_or((key, "unknown"));
        Error::circuit_open(service, method)
    }

    fn with_circuit<T>(&self, key: &str, f: impl FnOnce(&mut Circuit) -> T) -> T {
        let mut circuits = self.circuits.lock().unwrap();
        if !circuits.contains_key(key) && circuits.len() >= self.config.max_keys {
            let victim = circuits
                .iter()
                .find(|(_, c)| c.state == CircuitState::Closed)
                .or_else(|| circuits.iter().next())
                .map(|(k, _)| k.clone());
            if let Some(victim) = victim {
                circuits.remove(&victim);
            }
        }
        let circuit = circuits.entry(key.to_owned()).or_insert_with(Circuit::new);
        f(circuit)
    }

    /// Admission check; fails fast with `CIRCUIT_OPEN` while the circuit is open.
    pub fn before(&self, key: &str) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }
        let reset = self.config.reset_timeout;
        self.with_circuit(key, |c| {
            let now = Instant::now();
            if c.state == CircuitState::Open {
                if now.duration_since(c.opened_at) >= reset {
                    transition(key, c, CircuitState::HalfOpen);
                    c.half_open_successes = 0;
                    c.half_open_inflight = false;
                } else {
                    return Err(Self::open_error(key));
                }
            }
            if c.state == CircuitState::HalfOpen {
                if c.half_open_inflight && now.duration_since(c.probe_at) < reset {
                    return Err(Self::open_error(key));
                }
                c.half_open_inflight = true;
                c.probe_at = now;
            }
            Ok(())
        })
    }

    pub fn on_success(&self, key: &str) {
        if !self.config.enabled {
            return;
        }
        let threshold = self.config.half_open_success_threshold;
        let sliding = self.config.sliding.clone();
        self.with_circuit(key, |c| {
            record(c, sliding.as_ref(), true);
            if c.state == CircuitState::HalfOpen {
                c.half_open_inflight = false;
                c.half_open_successes += 1;
                if c.half_open_successes >= threshold {
                    transition(key, c, CircuitState::Closed);
                    c.consecutive_failures = 0;
                    c.half_open_successes = 0;
                    c.buckets.clear();
                }
            }
        });
    }

    pub fn on_failure(&self, key: &str, err: &Error) {
        if !self.config.enabled {
            return;
        }
        if matches!(
            err.code(),
            ErrorCode::ValidationFailed | ErrorCode::Unauthorized
        ) {
            if let Some(c) = self.circuits.lock().unwrap().get_mut(key)
                && c.state == CircuitState::HalfOpen
            {
                c.half_open_inflight = false;
            }
            return;
        }
        let config = &self.config;
        self.with_circuit(key, |c| {
            record(c, config.sliding.as_ref(), false);
            match c.state {
                CircuitState::HalfOpen => {
                    c.half_open_inflight = false;
                    transition(key, c, CircuitState::Open);
                    c.opened_at = Instant::now();
                }
                CircuitState::Closed if should_open(c, config) => {
                    transition(key, c, CircuitState::Open);
                    c.opened_at = Instant::now();
                }
                _ => {}
            }
        });
    }
}

fn transition(key: &str, c: &mut Circuit, next: CircuitState) {
    let prev = c.state;
    if prev == next {
        return;
    }
    c.state = next;
    tracing::info!(
        event = "circuit.transition",
        key,
        from = prev.as_str(),
        to = next.as_str(),
        failures = c.consecutive_failures,
        "circuit breaker state changed"
    );
}

fn record(c: &mut Circuit, sliding: Option<&SlidingWindow>, ok: bool) {
    if ok {
        if c.state == CircuitState::Closed {
            c.consecutive_failures = 0;
        }
    } else {
        c.consecutive_failures += 1;
    }
    if let Some(window) = sliding {
        let now = Instant::now();
        let needs_bucket = c
            .buckets
            .back()
            .is_none_or(|b| now.duration_since(b.started) >= window.bucket);
        if needs_bucket {
            c.buckets.push_back(Bucket {
                started: now,
                success: 0,
                failure: 0,
            });
            while c
                .buckets
                .front()
                .is_some_and(|b| now.duration_since(b.started) > window.window)
            {
                c.buckets.pop_front();
            }
        }
        if let Some(bucket) = c.buckets.back_mut() {
            if ok {
                bucket.success += 1;
            } else {
                bucket.failure += 1;
            }
        }
    }
}

fn should_open(c: &Circuit, config: &CircuitBreakerConfig) -> bool {
    match &config.sliding {
        None => c.consecutive_failures >= config.failure_threshold,
        Some(window) => {
            let now = Instant::now();
            let (success, failure) = c
                .buckets
                .iter()
                .filter(|b| now.duration_since(b.started) <= window.window)
                .fold((0u32, 0u32), |(s, f), b| (s + b.success, f + b.failure));
            let total = success + failure;
            total >= window.min_sample_size
                && f64::from(failure) / f64::from(total.max(1)) >= window.error_rate_threshold
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opens_after_consecutive_failures_and_recovers() {
        let breaker = CircuitBreaker::new(
            CircuitBreakerConfig::consecutive(2).reset_timeout(Duration::from_millis(20)),
        );
        let err = Error::from_code(ErrorCode::Timeout);
        breaker.before("svc:m").unwrap();
        breaker.on_failure("svc:m", &err);
        breaker.on_failure("svc:m", &err);
        assert_eq!(breaker.state("svc:m"), Some(CircuitState::Open));
        assert_eq!(
            breaker.before("svc:m").unwrap_err().code(),
            ErrorCode::CircuitOpen
        );
        std::thread::sleep(Duration::from_millis(25));
        breaker.before("svc:m").unwrap();
        assert_eq!(breaker.state("svc:m"), Some(CircuitState::HalfOpen));
        assert_eq!(
            breaker.before("svc:m").unwrap_err().code(),
            ErrorCode::CircuitOpen
        );
        breaker.on_success("svc:m");
        assert_eq!(breaker.state("svc:m"), Some(CircuitState::Closed));
    }

    #[test]
    fn client_errors_do_not_trip() {
        let breaker = CircuitBreaker::new(CircuitBreakerConfig::consecutive(1));
        breaker.on_failure("k", &Error::from_code(ErrorCode::ValidationFailed));
        assert_ne!(breaker.state("k"), Some(CircuitState::Open));
    }

    #[test]
    fn sliding_window_uses_error_rate() {
        let breaker = CircuitBreaker::new(CircuitBreakerConfig::sliding(
            SlidingWindow::new()
                .min_sample_size(4)
                .error_rate_threshold(0.5),
        ));
        let err = Error::from_code(ErrorCode::Internal);
        breaker.on_success("k");
        breaker.on_failure("k", &err);
        breaker.on_success("k");
        assert_eq!(breaker.state("k"), Some(CircuitState::Closed));
        breaker.on_failure("k", &err);
        assert_eq!(breaker.state("k"), Some(CircuitState::Open));
    }

    #[test]
    fn disabled_breaker_is_transparent() {
        let breaker = CircuitBreaker::new(CircuitBreakerConfig::disabled());
        for _ in 0..10 {
            breaker.on_failure("k", &Error::from_code(ErrorCode::Timeout));
        }
        breaker.before("k").unwrap();
        assert!(breaker.snapshot().is_empty());
    }
}
