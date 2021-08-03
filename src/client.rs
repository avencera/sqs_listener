#![doc(hidden)]
/// Implementation details for SQSListenerClient, don't use directly.
/// Instead use [SQSListenerClient](super::SQSListenerClient) and [SQSListenerClientBuilder](super::SQSListenerClientBuilder)
use rusoto_sqs::{DeleteMessageRequest, Message, ReceiveMessageRequest, Sqs};

use async_trait::async_trait;
use derive_builder::Builder;
use log::{debug, error, info};
use rusoto_sqs::SqsClient;

use act_zero::runtimes::tokio::Timer;
use act_zero::timer::Tick;
use act_zero::*;

use super::{Config, ConfigBuilder, Error, SQSListener};

#[derive(Builder)]
#[builder(pattern = "owned")]
#[doc(hidden)]
#[builder(build_fn(private, name = "build_private"))]
pub struct SQSListenerClient<F: Fn(&Message) + Send + Sync + 'static> {
    #[builder(default = "Addr::detached()", setter(skip))]
    pub(crate) pid: Addr<SQSListenerClient<F>>,

    pub(crate) client: SqsClient,

    #[builder(default = "ConfigBuilder::default().build()")]
    pub(crate) config: Config,

    #[builder(default = "Timer::default()", setter(skip))]
    pub(crate) timer: Timer,

    /// Add a listener to the [SQSListenerClient]
    pub(crate) listener: SQSListener<F>,
}

impl<F: Fn(&Message) + Send + Sync> SQSListenerClientBuilder<F> {
    // implementation detail
    pub(crate) fn priv_build(self) -> Result<SQSListenerClient<F>, SQSListenerClientBuilderError> {
        self.build_private()
    }

    // implementation, needs to be in this module because we are using Default with private fields
    pub(crate) fn priv_new_with_client(client: SqsClient) -> Self {
        Self {
            client: Some(client),
            ..Default::default()
        }
    }
}

impl<F: Fn(&Message) + Send + Sync> SQSListenerClient<F> {
    pub(crate) async fn ack_message(&self, message: Message) -> ActorResult<Result<(), Error>> {
        if message.receipt_handle.is_none() {
            return Produces::ok(Err(Error::NoMessageHandle));
        }

        let ignore = self
            .client
            .delete_message(DeleteMessageRequest {
                queue_url: self.listener.queue_url.clone(),
                receipt_handle: message.receipt_handle.clone().unwrap(),
            })
            .await;

        match ignore {
            Ok(_) => Produces::ok(Ok(())),
            Err(error) => Produces::ok(Err(Error::AckMessage(error))),
        }
    }
}

#[async_trait]
impl<F: Fn(&Message) + Send + Sync> Actor for SQSListenerClient<F> {
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
impl<F: Fn(&Message) + Send + Sync> Tick for SQSListenerClient<F> {
    async fn tick(&mut self) -> ActorResult<()> {
        if self.timer.tick() {
            self.timer
                .set_timeout_for_strong(self.pid.clone(), self.config.check_interval);

            match self.get_and_handle_messages().await {
                Ok(()) => {}
                Err(error) => error!("Error when handling message: {:?}", error),
            }
        }
        Produces::ok(())
    }
}

impl<F: Fn(&Message) + Send + Sync> SQSListenerClient<F> {
    async fn get_and_handle_messages(&self) -> Result<(), Error> {
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
            .ok_or(Error::UnknownReceiveMessages)?;

        for message in messages {
            // ignore result
            handler(&message);

            // if auto ack is set ack message
            if self.config.auto_ack {
                send!(self.pid.ack_message(message.clone()))
            }
        }

        Ok(())
    }
}
