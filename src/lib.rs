mod client;

use std::time::Duration;

use act_zero::runtimes::tokio::spawn_actor;
use act_zero::*;

use derive_builder::Builder;

pub use rusoto_core::credential::ProvideAwsCredentials;
pub use rusoto_core::request::DispatchSignedRequest;
pub use rusoto_core::Region;
pub use rusoto_sqs::Message;

pub type SQSListenerClientBuilder<F> = client::SQSListenerClientBuilder<F>;
pub type SQSListenerClientBuilderError = client::SQSListenerClientBuilderError;

impl<F: Fn(&Message) + Send + Sync + 'static> SQSListenerClientBuilder<F> {
    pub fn build(
        self: SQSListenerClientBuilder<F>,
    ) -> Result<SQSListenerClient<F>, SQSListenerClientBuilderError> {
        let inner: client::SQSListenerClient<F> = self.build_private()?;

        Ok(SQSListenerClient {
            inner,
            addr: Addr::detached(),
        })
    }
}

#[derive(Debug)]
pub struct SQSListener<F: Fn(&Message)> {
    /// Url for the SQS queue that you want to listen to
    queue_url: String,

    /// Function to call when a new message is received
    handler: F,
}

impl<F: Fn(&Message)> SQSListener<F> {
    pub fn new(queue_url: String, handler: F) -> Self {
        Self { queue_url, handler }
    }
}

pub struct SQSListenerClient<F: Fn(&Message) + Sync + Send + 'static> {
    addr: Addr<client::SQSListenerClient<F>>,
    inner: client::SQSListenerClient<F>,
}

impl<F: Fn(&Message) + Sync + Send + 'static> SQSListenerClient<F> {
    /// Starts the service, this will run forever until your application exits.
    pub async fn start(mut self) {
        self.addr = spawn_actor(self.inner);
        self.addr.termination().await
    }

    pub async fn ack_message(self) {}
}

#[derive(Clone, Builder, Debug)]
#[builder(pattern = "owned")]
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn creates_with_closure() {
        let hashmap: HashMap<String, String> = HashMap::new();

        let listener = SQSListener::new("".to_string(), move |message| {
            println!("HashMap: {:#?}", hashmap);
            println!("{:#?}", message)
        });

        let client = SQSListenerClientBuilder::new(Region::UsEast1)
            .listener(listener)
            .build();

        assert!(client.is_ok())
    }

    #[test]
    fn creates_with_config() {
        let hashmap: HashMap<String, String> = HashMap::new();

        let listener = SQSListener::new("".to_string(), move |message| {
            println!("HashMap: {:#?}", hashmap);
            println!("{:#?}", message)
        });

        let config = ConfigBuilder::default()
            .check_interval(Duration::from_millis(1000))
            .auto_ack(false)
            .build();

        let client = SQSListenerClientBuilder::new(Region::UsEast1)
            .listener(listener)
            .config(config)
            .build();

        assert!(client.is_ok())
    }
}
