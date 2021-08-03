use sqs_listener::{Region, SQSListener, SQSListenerClientBuilder};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    env_logger::init();
    color_eyre::install()?;

    let listener = SQSListener::new("".to_string(), |message| {
        println!("Message received {:#?}", message)
    });

    let client = SQSListenerClientBuilder::new(Region::UsEast1)
        .listener(listener)
        .build()?;

    let _ = client.start().await;

    Ok(())
}
