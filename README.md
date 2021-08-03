# sqs_listener

![Build Status](https://github.com/avencera/sqs_listener/workflows/Rust/badge.svg)
[![Crates.io](https://img.shields.io/crates/v/sqs_listener.svg)](https://crates.io/crates/sqs_listener)
[![Documentation](https://docs.rs/sqs_listener/badge.svg)](https://docs.rs/sqs_listener)
[![Rust 1.52+](https://img.shields.io/badge/rust-1.52+-orange.svg)](https://www.rust-lang.org)

## Getting Started

Available on crates: [crates.io/sqs_listener](https://crates.io/crates/sqs_listener)

Documentation available at: [docs.rs/sqs_listener](https://docs.rs/sqs_listener/)

```toml
sqs_listener = "0.2.0"
```

### Simple Example

Simple example: [/examples/simple.rs](/examples/simple.rs)

```rust
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
```

### Start a listener using AWS creds

Example with creds: [/examples/with_creds.rs](/examples/with_creds.rs)

```rust
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

    let listener = SQSListener::new("".to_string(), |message| {
        println!("Message received {:#?}", message)
    });

    let client = SQSListenerClientBuilder::new_with(
        HttpClient::new().expect("failed to create request dispatcher"),
        StaticProvider::new_minimal(aws_access_key_id, aws_secret_access_key),
        Region::UsEast1,
    )
    .listener(listener)
    .build()?;

    let _ = client.start().await;

    Ok(())
}
```
