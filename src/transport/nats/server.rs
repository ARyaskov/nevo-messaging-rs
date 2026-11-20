use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;

use super::{Nats, NatsConfig, from_message, to_header_map};
use crate::error::Result;
use crate::server::Router;

/// How a router is pumped from its NATS subscription.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ServeConfig {
    /// Load-balance requests across instances; `None` delivers every message to every instance,
    /// which is what the TypeScript router does.
    pub queue_group: Option<String>,
    pub concurrency: usize,
    pub drain_timeout: Duration,
}

impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            queue_group: None,
            concurrency: 1024,
            drain_timeout: Duration::from_secs(30),
        }
    }
}

/// Hosts one or more [`Router`]s on a NATS connection.
pub struct NatsServer {
    nats: Nats,
    owned: bool,
    routers: Vec<Arc<Router>>,
    pumps: Vec<JoinHandle<()>>,
    stop: watch::Sender<bool>,
    config: ServeConfig,
    /// Accepted handler tasks spawned by the pumps but not yet reaped.
    retained: Arc<AtomicUsize>,
}

impl fmt::Debug for NatsServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NatsServer")
            .field(
                "services",
                &self
                    .routers
                    .iter()
                    .map(|r| r.service_name())
                    .collect::<Vec<_>>(),
            )
            .field("owned_connection", &self.owned)
            .finish()
    }
}

#[derive(Debug, Default)]
pub struct NatsServerBuilder {
    routers: Vec<Router>,
    config: ServeConfig,
    nats: NatsConfig,
}

impl NatsServerBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn router(mut self, router: Router) -> Self {
        self.routers.push(router);
        self
    }

    pub fn queue_group(mut self, group: impl Into<String>) -> Self {
        self.config.queue_group = Some(group.into());
        self
    }

    pub fn concurrency(mut self, max_inflight: usize) -> Self {
        self.config.concurrency = max_inflight.max(1);
        self
    }

    pub fn drain_timeout(mut self, timeout: Duration) -> Self {
        self.config.drain_timeout = timeout;
        self
    }

    pub fn nats_config(mut self, config: NatsConfig) -> Self {
        self.nats = config;
        self
    }

    pub fn servers(mut self, servers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.nats = self.nats.servers(servers);
        self
    }

    /// Opens a dedicated connection and starts serving.
    pub async fn serve(self) -> Result<NatsServer> {
        let nats = Nats::connect(&self.nats).await?;
        NatsServer::start(nats, true, self.routers, self.config).await
    }

    /// Serves over an existing connection, typically the one a [`super::NatsClient`] uses.
    pub async fn serve_with(self, nats: Nats) -> Result<NatsServer> {
        NatsServer::start(nats, false, self.routers, self.config).await
    }
}

impl NatsServer {
    pub fn builder() -> NatsServerBuilder {
        NatsServerBuilder::new()
    }

    /// Serves a single router on a dedicated connection with default settings.
    pub async fn bind(
        router: Router,
        servers: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self> {
        NatsServerBuilder::new()
            .router(router)
            .servers(servers)
            .serve()
            .await
    }

    async fn start(
        nats: Nats,
        owned: bool,
        routers: Vec<Router>,
        config: ServeConfig,
    ) -> Result<Self> {
        let (stop, _) = watch::channel(false);
        let retained = Arc::new(AtomicUsize::new(0));
        let routers: Vec<Arc<Router>> = routers.into_iter().map(Arc::new).collect();
        let mut pumps = Vec::with_capacity(routers.len());
        for router in &routers {
            let subject = router.request_topic().to_owned();
            let subscriber = match &config.queue_group {
                Some(group) => {
                    nats.client()
                        .queue_subscribe(subject.clone(), group.clone())
                        .await
                }
                None => nats.client().subscribe(subject.clone()).await,
            }
            .map_err(super::subscribe_error)?;
            tracing::info!(
                event = "nats.router.listening",
                service = router.service_name(),
                subject = %subject,
                queue_group = config.queue_group.as_deref().unwrap_or(""),
            );
            pumps.push(tokio::spawn(pump(
                nats.client().clone(),
                Arc::clone(router),
                subscriber,
                stop.subscribe(),
                config.concurrency,
                config.drain_timeout,
                Arc::clone(&retained),
            )));
        }
        Ok(Self {
            nats,
            owned,
            routers,
            pumps,
            stop,
            config,
            retained,
        })
    }

    pub fn routers(&self) -> &[Arc<Router>] {
        &self.routers
    }

    pub fn nats(&self) -> &Nats {
        &self.nats
    }

    /// Diagnostic count of accepted handler tasks the pumps have spawned but not
    /// yet reaped. Completed tasks are reaped while the pump keeps serving, so
    /// this stays near zero under steady traffic and is zero after a graceful
    /// shutdown.
    pub fn retained_tasks(&self) -> usize {
        self.retained.load(Ordering::Relaxed)
    }

    /// Stops accepting requests, waits for handlers to finish, then closes an owned connection.
    pub async fn shutdown(self) -> Result<()> {
        // Reject new work before the pumps stop, so anything already accepted is drained.
        for router in &self.routers {
            router.inflight().begin_shutdown();
        }
        let _ = self.stop.send(true);
        for pump in self.pumps {
            // Each pump joins its accepted handlers before returning.
            let _ = pump.await;
        }
        for router in &self.routers {
            if !router.inflight().drain(self.config.drain_timeout).await {
                tracing::warn!(
                    event = "nats.router.drain_timeout",
                    service = router.service_name(),
                    remaining = router.inflight().count(),
                );
            }
        }
        if self.owned {
            self.nats.client().drain().await.map_err(super::other_error)
        } else {
            self.nats.client().flush().await.map_err(super::other_error)
        }
    }

    /// Serves until `signal` resolves, then shuts down gracefully.
    pub async fn run_until<F: Future>(self, signal: F) -> Result<()> {
        signal.await;
        self.shutdown().await
    }
}

async fn pump(
    client: async_nats::Client,
    router: Arc<Router>,
    mut subscriber: async_nats::Subscriber,
    mut stop: watch::Receiver<bool>,
    concurrency: usize,
    drain_timeout: Duration,
    retained: Arc<AtomicUsize>,
) {
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut accepted = tokio::task::JoinSet::new();
    loop {
        let message = tokio::select! {
            biased;
            _ = stop.changed() => break,
            // Reap completed handlers while serving; with reaping only at
            // shutdown, finished tasks would stay in the set forever.
            Some(joined) = accepted.join_next() => {
                retained.fetch_sub(1, Ordering::Relaxed);
                if let Err(err) = joined {
                    tracing::warn!(event = "nats.handler_join_failed", error = %err);
                }
                continue;
            }
            next = subscriber.next() => match next {
                Some(message) => message,
                None => break,
            },
        };
        let permit = tokio::select! {
            biased;
            _ = stop.changed() => break,
            permit = Arc::clone(&semaphore).acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break,
            },
        };
        // Once shutdown began, stop accepting new work; pending messages are dropped.
        if router.inflight().is_shutting_down() {
            continue;
        }
        let router = Arc::clone(&router);
        let client = client.clone();
        retained.fetch_add(1, Ordering::Relaxed);
        accepted.spawn(async move {
            let _permit = permit;
            // Registered at spawn time, before any handler work, so a shutdown
            // racing this task still waits for the reply to be published.
            let _guard = router.inflight().enter();
            let reply = message.reply.clone();
            let outbound = router.handle(from_message(message)).await;
            let Some(reply) = reply else {
                return;
            };
            let sent = match to_header_map(&outbound.headers) {
                Some(headers) => {
                    client
                        .publish_with_headers(reply, headers, outbound.payload)
                        .await
                }
                None => client.publish(reply, outbound.payload).await,
            };
            if let Err(err) = sent {
                tracing::warn!(event = "nats.reply_failed", error = %err, "could not publish reply");
            }
        });
    }
    // Unsubscription and handler drain share one deadline.
    let deadline = tokio::time::Instant::now() + drain_timeout;
    let _ = tokio::time::timeout_at(deadline, subscriber.unsubscribe()).await;
    loop {
        match tokio::time::timeout_at(deadline, accepted.join_next()).await {
            Ok(Some(result)) => {
                retained.fetch_sub(1, Ordering::Relaxed);
                if let Err(err) = result {
                    tracing::warn!(event = "nats.handler_join_failed", error = %err);
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    accepted.abort_all();
    // Aborted tasks remain in the set until joined, so `accepted.len()` is
    // exactly this pump's spawned-but-unreaped residual; each was counted at
    // spawn and never decremented. `fetch_sub` balances this pump alone without
    // zeroing a sibling pump's share of the shared counter.
    retained.fetch_sub(accepted.len(), Ordering::Relaxed);
}
