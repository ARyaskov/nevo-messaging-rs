//! Metric names shared with the TypeScript implementation. Emission goes through
//! the `metrics` facade when the `metrics` feature is enabled.

pub const REQUESTS_TOTAL: &str = "nevo_messaging_requests_total";
pub const REQUEST_ERRORS_TOTAL: &str = "nevo_messaging_request_errors_total";
pub const REQUEST_DURATION_SECONDS: &str = "nevo_messaging_request_duration_seconds";
pub const RETRIES_TOTAL: &str = "nevo_messaging_retries_total";
pub const PAYLOAD_BYTES: &str = "nevo_messaging_payload_bytes";
pub const UNKNOWN_METHOD_LABEL: &str = "<unknown>";

pub(crate) type Labels = Vec<(&'static str, String)>;

#[cfg(feature = "metrics")]
fn to_labels(labels: Labels) -> Vec<metrics::Label> {
    labels
        .into_iter()
        .map(|(k, v)| metrics::Label::new(k, v))
        .collect()
}

#[cfg(feature = "metrics")]
pub(crate) fn increment(name: &'static str, labels: Labels) {
    metrics::counter!(name, to_labels(labels)).increment(1);
}

#[cfg(feature = "metrics")]
pub(crate) fn observe(name: &'static str, labels: Labels, value: f64) {
    metrics::histogram!(name, to_labels(labels)).record(value);
}

#[cfg(not(feature = "metrics"))]
pub(crate) fn increment(_name: &'static str, _labels: Labels) {}

#[cfg(not(feature = "metrics"))]
pub(crate) fn observe(_name: &'static str, _labels: Labels, _value: f64) {}
