use std::time::Duration;

use log::error;
use rusoto_core::RusotoError;
use rusoto_sqs::{Sqs, SqsClient};

use act_zero::runtimes::tokio::Timer;
use act_zero::timer::Tick;
use act_zero::*;

use async_trait::async_trait;
use derive_builder::Builder;

pub use rusoto_core::credential::ProvideAwsCredentials;
pub use rusoto_core::request::DispatchSignedRequest;
pub use rusoto_core::Region;
pub use rusoto_sqs::{
    DeleteMessageRequest, Message, ReceiveMessageError, ReceiveMessageRequest, ReceiveMessageResult,
};

#[derive(Debug, Clone)]
pub struct HandlerError;

pub struct SQSListenerClient<T: Send + Sync + 'static> {
    pid: Addr<Self>,
    client: SqsClient,
    config: Config,

    timer: Timer,
    listeners: Vec<SQSListener<T>>,
}

type HandlerFn<T> = fn(&Message, &Context<T>) -> Result<(), HandlerError>;

pub struct SQSListener<T: Send + Sync + 'static> {
    context: Context<T>,
    queue_url: String,
    handler: HandlerFn<T>,
}

struct Context<T> {
    inner: T,
}

impl<T> Context<T> {
    fn get(self) -> T {
        self.inner
    }
}

impl<T: Send + Sync + 'static> SQSListener<T> {
    fn new(queue_url: String, handler: HandlerFn<T>, context: Context<T>) -> Self {
        Self {
            context,
            queue_url,
            handler,
        }
    }
}

#[derive(Clone, Builder)]
pub struct Config {
    #[builder(default = "Duration::from_secs(10_u64)")]
    /// How often to check for new messages, defaults to 10 seconds
    check_interval: Duration,

    #[builder(default = "true")]
    /// Determines if messages should be automatically acknowledges. Defaults to true, if
    /// disabled you must manually ack the message by calling `message.ack()`
    auto_ack: bool,
}

#[async_trait]
impl<T: Send + Sync + 'static> Actor for SQSListenerClient<T> {
    async fn started(&mut self, pid: Addr<Self>) -> ActorResult<()> {
        let pid_clone = pid.clone();
        // send!(pid_clone.listen(pid));

        // Start the timer
        self.timer
            .set_timeout_for_strong(pid_clone.clone(), self.config.check_interval);

        self.pid = pid_clone;

        Produces::ok(())
    }

    async fn error(&mut self, error: ActorError) -> bool {
        error!("SQSListener Actor Error: {:?}", error);

        // do not stop on actor error
        false
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> Tick for SQSListenerClient<T> {
    async fn tick(&mut self) -> ActorResult<()> {
        if self.timer.tick() {
            self.timer
                .set_timeout_for_strong(self.pid.clone(), self.config.check_interval);

            // let _ = self.send_heartbeat().await;
        }
        Produces::ok(())
    }
}

impl<T: Send + Sync + 'static> SQSListenerClient<T> {
    /// returns the SqsClient
    pub fn client(self) -> SqsClient {
        self.client
    }

    /// Create a new listener the default AWS client and queue_url
    pub fn new(region: Region, config: Config) -> Self {
        Self::new_with_client(SqsClient::new(region), config)
    }

    /// Create a new listener with custom credentials, request dispatcher, region and queue_url
    pub fn new_with<P, D>(
        request_dispatcher: D,
        credentials_provider: P,
        region: Region,
        config: Config,
    ) -> Self
    where
        P: ProvideAwsCredentials + Send + Sync + 'static,
        D: DispatchSignedRequest + Send + Sync + 'static,
    {
        Self::new_with_client(
            SqsClient::new_with(request_dispatcher, credentials_provider, region),
            config,
        )
    }

    /// Create new listener with a client and queue_url
    pub fn new_with_client(client: SqsClient, config: Config) -> Self {
        Self {
            timer: Timer::default(),
            pid: Addr::detached(),
            config,
            client,
            listeners: vec![],
        }
    }

    /// Adds a listener
    pub fn add_listener(mut self, listener: SQSListener<T>) -> Self {
        self.listeners.push(listener);
        self
    }

    async fn get_and_handle_messages(&self) -> eyre::Result<()> {
        for listener in &self.listeners {
            let messages = self
                .client
                .receive_message(ReceiveMessageRequest {
                    queue_url: listener.queue_url.clone(),
                    ..Default::default()
                })
                .await?
                .messages
                .ok_or_else(|| eyre::eyre!("Unable to get message"))?;

            for message in messages {
                let handler = listener.handler;
                let _ = handler(&message, &listener.context);
            }
        }

        Ok(())
    }

    // fn ack_message(&self, message: &Message) {
    //     if message.receipt_handle.is_none() {
    //         return;
    //     }

    //     let _ignore = self
    //         .sqs_client
    //         .delete_message(DeleteMessageRequest {
    //             queue_url: self.queue_url.clone(),
    //             receipt_handle: message.receipt_handle.clone().unwrap(),
    //         })
    //         .sync();
    // }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
