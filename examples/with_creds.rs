use std::env;

use sqs_listener::{
    credential::StaticProvider, request::HttpClient, Region, SQSListener, SQSListenerClientBuilder,
};

#[tokio::main]
async fn main() -> eyre::Result<()> {
    env_logger::init();
    color_eyre::install()?;

    let aws_access_key_id =
        env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID env variable needs to be present");

    let aws_secret_access_key = env::var("AWS_SECRET_ACCESS_KEY")
        .expect("AWS_SECRET_ACCESS_KEY env variable needs to be present");

    let client = SQSListenerClientBuilder::new_with(
        HttpClient::new().expect("failed to create request dispatcher"),
        StaticProvider::new_minimal(aws_access_key_id, aws_secret_access_key),
        Region::UsEast1,
    )
    .listener(SQSListener::new("".to_string(), |message| {
        println!("Message received {:#?}", message)
    }))
    .build()?;

    let _ = client.start().await;

    Ok(())
}
