//! Consumer-side subscription filters over envelope headers and metadata.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::wire::Meta;

#[derive(Clone)]
pub enum Matcher {
    Exact(String),
    Prefix(String),
    Predicate(Arc<dyn Fn(&str) -> bool + Send + Sync>),
}

impl Matcher {
    pub fn predicate(f: impl Fn(&str) -> bool + Send + Sync + 'static) -> Self {
        Self::Predicate(Arc::new(f))
    }

    pub fn matches(&self, actual: &str) -> bool {
        match self {
            Self::Exact(expected) => expected == actual,
            Self::Prefix(prefix) => actual.starts_with(prefix.as_str()),
            Self::Predicate(f) => f(actual),
        }
    }
}

impl fmt::Debug for Matcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exact(v) => f.debug_tuple("Exact").field(v).finish(),
            Self::Prefix(v) => f.debug_tuple("Prefix").field(v).finish(),
            Self::Predicate(_) => f.write_str("Predicate(..)"),
        }
    }
}

impl From<&str> for Matcher {
    fn from(value: &str) -> Self {
        Self::Exact(value.to_owned())
    }
}

impl From<String> for Matcher {
    fn from(value: String) -> Self {
        Self::Exact(value)
    }
}

/// Every predicate must pass; `headers` looks at `meta.headers`, `meta` at top-level meta fields.
#[derive(Debug, Clone, Default)]
pub struct SubscriptionFilter {
    headers: HashMap<String, Matcher>,
    meta: HashMap<String, Matcher>,
}

impl SubscriptionFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn header(mut self, name: impl Into<String>, matcher: impl Into<Matcher>) -> Self {
        self.headers.insert(name.into(), matcher.into());
        self
    }

    pub fn meta(mut self, key: impl Into<String>, matcher: impl Into<Matcher>) -> Self {
        self.meta.insert(key.into(), matcher.into());
        self
    }

    pub fn is_empty(&self) -> bool {
        self.headers.is_empty() && self.meta.is_empty()
    }

    pub fn matches(&self, meta: Option<&Meta>) -> bool {
        if !self.headers.is_empty() {
            let Some(headers) = meta.and_then(|m| m.headers.as_ref()) else {
                return false;
            };
            for (name, matcher) in &self.headers {
                match headers.get(name) {
                    Some(actual) if matcher.matches(actual) => {}
                    _ => return false,
                }
            }
        }
        if !self.meta.is_empty() {
            let Some(meta) = meta else {
                return false;
            };
            for (key, matcher) in &self.meta {
                if !matcher.matches(&meta.field_as_string(key)) {
                    return false;
                }
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_and_meta_predicates_are_anded() {
        let filter = SubscriptionFilter::new()
            .header("region", Matcher::Prefix("eu-".into()))
            .meta("tenantId", "t-42");
        let mut meta = Meta::new();
        meta.set_header("region", "eu-west");
        meta.tenant_id = Some("t-42".into());
        assert!(filter.matches(Some(&meta)));
        meta.tenant_id = Some("t-1".into());
        assert!(!filter.matches(Some(&meta)));
        assert!(!filter.matches(None));
        assert!(SubscriptionFilter::new().matches(None));
    }

    #[test]
    fn missing_header_fails_even_with_predicate() {
        let filter = SubscriptionFilter::new().header("x", Matcher::predicate(|_| true));
        let mut meta = Meta::new();
        assert!(!filter.matches(Some(&meta)));
        meta.set_header("x", "");
        assert!(filter.matches(Some(&meta)));
    }
}
