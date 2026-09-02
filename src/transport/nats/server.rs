use std::fmt;
use std::future::Future;
use std::sync::Arc;
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
            )));
        }
        Ok(Self {
            nats,
            owned,
            routers,
            pumps,
            stop,
            config,
        })
    }

    pub fn routers(&self) -> &[Arc<Router>] {
        &self.routers
    }

    pub fn nats(&self) -> &Nats {
        &self.nats
    }

    /// Stops accepting requests, waits for handlers to finish, then closes an owned connection.
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.stop.send(true);
        for pump in self.pumps {
            let _ = pump.await;
        }
        for router in &self.routers {
            router.inflight().begin_shutdown();
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
) {
    let semaphore = Arc::new(Semaphore::new(concurrency));
    loop {
        let message = tokio::select! {
            biased;
            _ = stop.changed() => break,
            next = subscriber.next() => match next {
                Some(message) => message,
                None => break,
            },
        };
        let Ok(permit) = Arc::clone(&semaphore).acquire_owned().await else {
            break;
        };
        let router = Arc::clone(&router);
        let client = client.clone();
        tokio::spawn(async move {
            let _permit = permit;
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
    let _ = subscriber.unsubscribe().await;
}
