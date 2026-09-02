//! Per-task chain context: a correlation id shared by every hop of one logical
//! request, propagated implicitly from inbound handlers to outbound calls.

use std::future::Future;

use crate::wire::TraceContext;

const MAX_CHAIN_ID_LEN: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ChainContext {
    pub chain_id: String,
    pub parent_uuid: Option<String>,
    pub trace: Option<TraceContext>,
}

impl ChainContext {
    pub fn new(chain_id: impl Into<String>) -> Self {
        Self {
            chain_id: chain_id.into(),
            parent_uuid: None,
            trace: None,
        }
    }

    pub fn fresh() -> Self {
        Self::new(new_chain_id())
    }

    pub fn with_parent(mut self, uuid: impl Into<String>) -> Self {
        self.parent_uuid = Some(uuid.into());
        self
    }

    pub fn with_trace(mut self, trace: Option<TraceContext>) -> Self {
        self.trace = trace;
        self
    }
}

tokio::task_local! {
    static CURRENT: ChainContext;
}

/// Runs `future` with `ctx` visible to every descendant `.await` through [`current`].
pub async fn scope<F: Future>(ctx: ChainContext, future: F) -> F::Output {
    CURRENT.scope(ctx, future).await
}

pub fn current() -> Option<ChainContext> {
    CURRENT.try_with(Clone::clone).ok()
}

pub fn current_chain_id() -> Option<String> {
    CURRENT.try_with(|ctx| ctx.chain_id.clone()).ok()
}

pub fn new_chain_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

pub fn is_valid_chain_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_CHAIN_ID_LEN
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
}

/// Inbound: honour a valid caller-supplied id, else inherit, else mint.
pub fn resolve_inbound(meta_chain_id: Option<&str>) -> String {
    match meta_chain_id {
        Some(id) if is_valid_chain_id(id) => id.to_owned(),
        _ => current_chain_id().unwrap_or_else(new_chain_id),
    }
}

/// Outbound: explicit override, else inherit, else mint.
pub fn resolve_outbound(explicit: Option<&str>) -> String {
    match explicit {
        Some(id) if !id.is_empty() => id.to_owned(),
        _ => current_chain_id().unwrap_or_else(new_chain_id),
    }
}

/// A fresh W3C `traceparent` with a random trace id and span id.
pub fn new_traceparent() -> String {
    let trace_id = rand::random::<u128>().max(1);
    let span_id = rand::random::<u64>().max(1);
    format!("00-{trace_id:032x}-{span_id:016x}-01")
}

/// A child `traceparent` continuing `parent`'s trace id with a new span id.
pub fn child_traceparent(parent: &str) -> Option<String> {
    let mut parts = parent.split('-');
    let version = parts.next()?;
    let trace_id = parts.next()?;
    let _span = parts.next()?;
    let flags = parts.next().unwrap_or("01");
    if trace_id.len() != 32 || !trace_id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let span_id = rand::random::<u64>().max(1);
    Some(format!("{version}-{trace_id}-{span_id:016x}-{flags}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scope_propagates_across_awaits() {
        assert!(current().is_none());
        let seen = scope(ChainContext::new("abc"), async {
            tokio::task::yield_now().await;
            current_chain_id()
        })
        .await;
        assert_eq!(seen.as_deref(), Some("abc"));
        assert_eq!(resolve_outbound(None).len(), 36);
    }

    #[tokio::test]
    async fn inbound_resolution_prefers_valid_caller_id() {
        assert_eq!(resolve_inbound(Some("chain-1")), "chain-1");
        assert_ne!(resolve_inbound(Some("bad id!")), "bad id!");
        let inherited = scope(ChainContext::new("parent"), async {
            resolve_inbound(Some("***"))
        })
        .await;
        assert_eq!(inherited, "parent");
    }

    #[test]
    fn traceparent_shapes() {
        let tp = new_traceparent();
        assert_eq!(tp.len(), 55);
        let child = child_traceparent(&tp).unwrap();
        assert_eq!(&child[..36], &tp[..36]);
        assert_ne!(child, tp);
        assert!(child_traceparent("garbage").is_none());
    }
}
