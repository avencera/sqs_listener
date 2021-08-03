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

pub struct SQSListenerClient<F: Fn(&Message) + Send + Sync + 'static> {
    pid: Addr<Self>,
    client: SqsClient,
    config: Config,

    timer: Timer,
    listener: Option<SQSListener<F>>,
}

pub struct SQSListener<F: Fn(&Message)> {
    pub queue_url: String,
    pub handler: F,
}

impl<F: Fn(&Message)> SQSListener<F> {
    fn new(queue_url: String, handler: F) -> Self {
        Self { queue_url, handler }
    }
}

#[derive(Clone, Builder)]
#[builder(build_fn(name = "build_private", private))]
pub struct Config {
    #[builder(default = "Duration::from_secs(10_u64)")]
    /// How often to check for new messages, defaults to 10 seconds
    check_interval: Duration,

    #[builder(default = "true")]
    /// Determines if messages should be automatically acknowledges. Defaults to true, if
    /// disabled you must manually ack the message by calling `message.ack()`
    auto_ack: bool,
}

impl ConfigBuilder {
    pub fn build(self) -> Config {
        self.build_private()
            .expect("will always work because all fields have defaults")
    }
}

#[async_trait]
impl<F: Fn(&Message) + Send + Sync + 'static> Actor for SQSListenerClient<F> {
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
            listener: None,
        }
    }

    /// Adds a listener
    pub fn set_listener(mut self, listener: SQSListener<F>) -> Self {
        self.listener = Some(listener);
        self
    }

    async fn get_and_handle_messages(&self) -> eyre::Result<()> {
        let handler = &self.listener.as_ref().unwrap().handler;

        // let messages = self
        //     .client
        //     .receive_message(ReceiveMessageRequest {
        //         queue_url: self.listener.queue_url.clone(),
        //         ..Default::default()
        //     })
        //     .await?
        //     .messages
        //     .ok_or_else(|| eyre::eyre!("Unable to get message"))?;

        // for message in messages {
        //     let handler = self.listener.handler;
        //     let _ = handler(&message, &self.listener.context);
        // }

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
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn creates_with_closure() {
        let hashmap: HashMap<String, String> = HashMap::new();

        let listener = SQSListener {
            queue_url: "".to_string(),
            handler: move |message| {
                println!("HashMap: {:#?}", hashmap);
                println!("{:#?}", message)
            },
        };

        let _client = SQSListenerClient::new(Region::UsEast1, ConfigBuilder::default().build())
            .set_listener(listener);
    }
}
