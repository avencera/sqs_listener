use std::collections::HashMap;

use act_zero::runtimes::tokio::spawn_actor;
use act_zero::*;
use sqs_listener::{ConfigBuilder, Region, SQSListener, SQSListenerClient};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    env_logger::init();
    color_eyre::install()?;

    let hashmap: HashMap<String, String> = HashMap::new();

    let listener = SQSListener {
        queue_url: "".to_string(),
        handler: move |message| {
            println!("HashMap: {:#?}", hashmap);
            println!("{:#?}", message)
        },
    };

    let client = SQSListenerClient::new(Region::UsEast1, ConfigBuilder::default().build().unwrap())
        .set_listener(listener);

    let addr = spawn_actor(client);

    addr.termination().await;

    Ok(())
}
