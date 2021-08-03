use rusoto_core::{credential::ProvideAwsCredentials, DispatchSignedRequest, Region};
use rusoto_sqs::{DeleteMessageRequest, Message, ReceiveMessageRequest, Sqs};

use async_trait::async_trait;
use derive_builder::Builder;
use log::{debug, error, info};
use rusoto_sqs::SqsClient;

use act_zero::runtimes::tokio::Timer;
use act_zero::timer::Tick;
use act_zero::*;

use super::{Config, ConfigBuilder, SQSListener};

#[derive(Builder)]
#[builder(build_fn(name = "build_private"))]
#[builder(pattern = "owned")]
pub struct SQSListenerClient<F: Fn(&Message) + Send + Sync + 'static> {
    #[builder(default = "Addr::detached()")]
    pub(crate) pid: Addr<SQSListenerClient<F>>,
    pub(crate) client: SqsClient,

    #[builder(default = "ConfigBuilder::default().build()")]
    pub(crate) config: Config,

    #[builder(default = "Timer::default()")]
    pub(crate) timer: Timer,
    pub(crate) listener: SQSListener<F>,
}

impl<F: Fn(&Message) + Send + Sync + 'static> SQSListenerClientBuilder<F> {
    /// Create a new listener the default AWS client and queue_url
    pub fn new(region: Region) -> Self {
        Self::new_with_client(SqsClient::new(region))
    }

    /// Create a new listener with custom credentials, request dispatcher, region and queue_url
    pub fn new_with<P, D>(request_dispatcher: D, credentials_provider: P, region: Region) -> Self
    where
        P: ProvideAwsCredentials + Send + Sync + 'static,
        D: DispatchSignedRequest + Send + Sync + 'static,
    {
        Self::new_with_client(SqsClient::new_with(
            request_dispatcher,
            credentials_provider,
            region,
        ))
    }

    /// Create new listener with a client and queue_url
    pub fn new_with_client(client: SqsClient) -> Self {
        Self {
            client: Some(client),
            ..Default::default()
        }
    }
}

impl<F: Fn(&Message) + Send + Sync + 'static> SQSListenerClient<F> {
    pub(crate) async fn ack_message(&self, message: Message) -> ActorResult<()> {
        if message.receipt_handle.is_none() {
            return Produces::ok(());
        }

        let _ignore = self
            .client
            .delete_message(DeleteMessageRequest {
                queue_url: self.listener.queue_url.clone(),
                receipt_handle: message.receipt_handle.clone().unwrap(),
            })
            .await;

        Produces::ok(())
    }
}

#[async_trait]
impl<F: Fn(&Message) + Send + Sync + 'static> Actor for SQSListenerClient<F> {
    async fn started(&mut self, pid: Addr<Self>) -> ActorResult<()> {
        info!("SQSListenerClient started...");

        // Start the timer
        self.timer
            .set_timeout_for_strong(pid.clone(), self.config.check_interval);

        self.pid = pid;

        Produces::ok(())
    }

    async fn error(&mut self, error: ActorError) -> bool {
        error!("SQSListenerClient Error: {:?}", error);

        // do not stop on actor error
        false
    }
}

#[async_trait]
impl<F: Fn(&Message) + Send + Sync + 'static> Tick for SQSListenerClient<F> {
    async fn tick(&mut self) -> ActorResult<()> {
        if self.timer.tick() {
            self.timer
                .set_timeout_for_strong(self.pid.clone(), self.config.check_interval);

            let _ = self.get_and_handle_messages().await;
        }
        Produces::ok(())
    }
}

impl<F: Fn(&Message) + Send + Sync + 'static> SQSListenerClient<F> {
    async fn get_and_handle_messages(&self) -> eyre::Result<()> {
        debug!("get and handle messages called");
        let handler = &self.listener.handler;

        let messages = self
            .client
            .receive_message(ReceiveMessageRequest {
                queue_url: self.listener.queue_url.clone(),
                ..Default::default()
            })
            .await?
            .messages
            .ok_or_else(|| eyre::eyre!("Unable to get message"))?;

        for message in messages {
            // ignore result from handler
            let _ = handler(&message);

            // if auto ack is set ack message
            if self.config.auto_ack {
                send!(self.pid.ack_message(message.clone()))
            }
        }

        Ok(())
    }
}
