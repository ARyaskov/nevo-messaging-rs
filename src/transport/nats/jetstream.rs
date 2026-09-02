//! JetStream helpers: durable streams, consumers and pull subscriptions with real acks.
//! Values travel bare (no envelope), matching the TypeScript `JetStreamHelper`.

use std::fmt;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use async_nats::jetstream::consumer::{self, pull};
use async_nats::jetstream::{self, stream};
use bytes::Bytes;
use futures::Stream;
use futures::stream::BoxStream;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::codec::Codec;
use crate::error::{Error, ErrorCode, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Retention {
    #[default]
    Limits,
    Interest,
    WorkQueue,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct StreamSpec {
    pub name: String,
    pub subjects: Vec<String>,
    pub retention: Retention,
    pub max_age: Option<Duration>,
    pub max_bytes: Option<i64>,
    pub duplicate_window: Option<Duration>,
}

impl StreamSpec {
    pub fn new(
        name: impl Into<String>,
        subjects: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            subjects: subjects.into_iter().map(Into::into).collect(),
            retention: Retention::Limits,
            max_age: None,
            max_bytes: None,
            duplicate_window: None,
        }
    }

    pub fn retention(mut self, retention: Retention) -> Self {
        self.retention = retention;
        self
    }

    pub fn max_age(mut self, max_age: Duration) -> Self {
        self.max_age = Some(max_age);
        self
    }

    pub fn max_bytes(mut self, max_bytes: i64) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }

    pub fn duplicate_window(mut self, window: Duration) -> Self {
        self.duplicate_window = Some(window);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeliverPolicy {
    #[default]
    All,
    Last,
    New,
    ByStartSequence(u64),
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ConsumerSpec {
    pub stream: String,
    pub durable: String,
    pub filter_subject: Option<String>,
    pub ack_wait: Option<Duration>,
    pub max_deliver: Option<i64>,
    pub deliver_policy: DeliverPolicy,
}

impl ConsumerSpec {
    pub fn new(stream: impl Into<String>, durable: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            durable: durable.into(),
            filter_subject: None,
            ack_wait: None,
            max_deliver: None,
            deliver_policy: DeliverPolicy::All,
        }
    }

    pub fn filter_subject(mut self, subject: impl Into<String>) -> Self {
        self.filter_subject = Some(subject.into());
        self
    }

    pub fn ack_wait(mut self, wait: Duration) -> Self {
        self.ack_wait = Some(wait);
        self
    }

    pub fn max_deliver(mut self, max: i64) -> Self {
        self.max_deliver = Some(max);
        self
    }

    pub fn deliver_policy(mut self, policy: DeliverPolicy) -> Self {
        self.deliver_policy = policy;
        self
    }
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct PublishOptions {
    pub msg_id: Option<String>,
    pub expected_stream: Option<String>,
    pub expected_last_sequence: Option<u64>,
}

impl PublishOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Deduplication id honoured inside the stream's duplicate window.
    pub fn msg_id(mut self, id: impl Into<String>) -> Self {
        self.msg_id = Some(id.into());
        self
    }

    pub fn expected_stream(mut self, stream: impl Into<String>) -> Self {
        self.expected_stream = Some(stream.into());
        self
    }

    pub fn expected_last_sequence(mut self, sequence: u64) -> Self {
        self.expected_last_sequence = Some(sequence);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubAck {
    pub stream: String,
    pub sequence: u64,
    pub duplicate: bool,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PullOptions {
    pub batch: usize,
    pub expires: Duration,
    pub idle_heartbeat: Duration,
}

impl Default for PullOptions {
    fn default() -> Self {
        Self {
            batch: 32,
            expires: Duration::from_secs(5),
            idle_heartbeat: Duration::from_secs(1),
        }
    }
}

impl PullOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn batch(mut self, batch: usize) -> Self {
        self.batch = batch.max(1);
        self
    }

    pub fn expires(mut self, expires: Duration) -> Self {
        self.expires = expires;
        self
    }

    pub fn idle_heartbeat(mut self, heartbeat: Duration) -> Self {
        self.idle_heartbeat = heartbeat;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JsInfo {
    pub stream_sequence: u64,
    pub delivered: i64,
    pub pending: u64,
}

/// A JetStream delivery with explicit acknowledgement.
pub struct JsMessage {
    inner: jetstream::Message,
    codec: Codec,
}

impl fmt::Debug for JsMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsMessage")
            .field("subject", &self.inner.subject)
            .field("len", &self.inner.payload.len())
            .finish()
    }
}

impl JsMessage {
    pub fn subject(&self) -> &str {
        self.inner.subject.as_str()
    }

    pub fn payload(&self) -> &Bytes {
        &self.inner.payload
    }

    pub fn decode<T: DeserializeOwned>(&self) -> Result<T> {
        let codec = Codec::detect(&self.inner.payload).unwrap_or(self.codec);
        codec.decode(&self.inner.payload)
    }

    pub fn info(&self) -> Option<JsInfo> {
        self.inner.info().ok().map(|info| JsInfo {
            stream_sequence: info.stream_sequence,
            delivered: info.delivered,
            pending: info.pending,
        })
    }

    pub async fn ack(&self) -> Result<()> {
        self.inner.ack().await.map_err(ack_error)
    }

    pub async fn nak(&self, delay: Option<Duration>) -> Result<()> {
        self.inner
            .ack_with(jetstream::AckKind::Nak(delay))
            .await
            .map_err(ack_error)
    }

    pub async fn term(&self) -> Result<()> {
        self.inner
            .ack_with(jetstream::AckKind::Term)
            .await
            .map_err(ack_error)
    }

    pub async fn progress(&self) -> Result<()> {
        self.inner
            .ack_with(jetstream::AckKind::Progress)
            .await
            .map_err(ack_error)
    }

    pub fn into_inner(self) -> jetstream::Message {
        self.inner
    }
}

fn ack_error(err: async_nats::Error) -> Error {
    Error::new(ErrorCode::AckFailed, err.to_string())
}

fn js_error(err: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(ErrorCode::Internal, format!("JetStream error: {err}")).with_source(err)
}

/// Messages from a pull consumer; dropping it stops pulling.
pub struct JsSubscription {
    inner:
        BoxStream<'static, std::result::Result<jetstream::Message, consumer::pull::MessagesError>>,
    codec: Codec,
}

impl fmt::Debug for JsSubscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsSubscription").finish()
    }
}

impl Stream for JsSubscription {
    type Item = Result<JsMessage>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(message))) => Poll::Ready(Some(Ok(JsMessage {
                inner: message,
                codec: self.codec,
            }))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(js_error(err)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Stream/consumer management and publishing over a JetStream context.
#[derive(Clone)]
pub struct JetStream {
    context: jetstream::Context,
    codec: Codec,
}

impl fmt::Debug for JetStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JetStream")
            .field("codec", &self.codec)
            .finish()
    }
}

impl JetStream {
    pub fn new(client: async_nats::Client, codec: Codec) -> Self {
        Self {
            context: jetstream::new(client),
            codec,
        }
    }

    pub fn context(&self) -> &jetstream::Context {
        &self.context
    }

    pub async fn ensure_stream(&self, spec: &StreamSpec) -> Result<()> {
        let config = stream::Config {
            name: spec.name.clone(),
            subjects: spec.subjects.clone(),
            retention: match spec.retention {
                Retention::Limits => stream::RetentionPolicy::Limits,
                Retention::Interest => stream::RetentionPolicy::Interest,
                Retention::WorkQueue => stream::RetentionPolicy::WorkQueue,
            },
            max_age: spec.max_age.unwrap_or_default(),
            max_bytes: spec.max_bytes.unwrap_or(-1),
            duplicate_window: spec.duplicate_window.unwrap_or_default(),
            ..Default::default()
        };
        self.context
            .create_or_update_stream(config)
            .await
            .map(|_| ())
            .map_err(js_error)
    }

    pub async fn publish<T: Serialize + ?Sized>(
        &self,
        subject: &str,
        value: &T,
        opts: PublishOptions,
    ) -> Result<PubAck> {
        let payload = self.codec.encode(value)?;
        let mut publish = jetstream::message::PublishMessage::build().payload(payload);
        if let Some(id) = &opts.msg_id {
            publish = publish.message_id(id);
        }
        if let Some(stream) = &opts.expected_stream {
            publish = publish.expected_stream(stream);
        }
        if let Some(sequence) = opts.expected_last_sequence {
            publish = publish.expected_last_sequence(sequence);
        }
        let ack = self
            .context
            .send_publish(subject.to_owned(), publish)
            .await
            .map_err(js_error)?
            .await
            .map_err(js_error)?;
        Ok(PubAck {
            stream: ack.stream,
            sequence: ack.sequence,
            duplicate: ack.duplicate,
        })
    }

    fn consumer_config(spec: &ConsumerSpec) -> pull::Config {
        pull::Config {
            durable_name: Some(spec.durable.clone()),
            filter_subject: spec.filter_subject.clone().unwrap_or_default(),
            ack_wait: spec.ack_wait.unwrap_or(Duration::from_secs(30)),
            max_deliver: spec.max_deliver.unwrap_or(-1),
            deliver_policy: match spec.deliver_policy {
                DeliverPolicy::All => consumer::DeliverPolicy::All,
                DeliverPolicy::Last => consumer::DeliverPolicy::Last,
                DeliverPolicy::New => consumer::DeliverPolicy::New,
                DeliverPolicy::ByStartSequence(start_sequence) => {
                    consumer::DeliverPolicy::ByStartSequence { start_sequence }
                }
            },
            ..Default::default()
        }
    }

    pub async fn ensure_consumer(&self, spec: &ConsumerSpec) -> Result<()> {
        let stream = self
            .context
            .get_stream(&spec.stream)
            .await
            .map_err(js_error)?;
        stream
            .create_consumer(Self::consumer_config(spec))
            .await
            .map(|_| ())
            .map_err(js_error)
    }

    pub async fn pull_subscribe(
        &self,
        spec: &ConsumerSpec,
        opts: PullOptions,
    ) -> Result<JsSubscription> {
        let stream = self
            .context
            .get_stream(&spec.stream)
            .await
            .map_err(js_error)?;
        let consumer: consumer::Consumer<pull::Config> = stream
            .create_consumer(Self::consumer_config(spec))
            .await
            .map_err(js_error)?;
        let messages = consumer
            .stream()
            .max_messages_per_batch(opts.batch)
            .expires(opts.expires)
            .heartbeat(opts.idle_heartbeat)
            .messages()
            .await
            .map_err(js_error)?;
        Ok(JsSubscription {
            inner: Box::pin(messages),
            codec: self.codec,
        })
    }
}
