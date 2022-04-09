//! Simply produces some console output

use std::time::Duration;

use anyhow::Error;
use condow_core::{
    condow_client::{InMemoryClient, NoLocation},
    config::Config,
};
use tokio::runtime::Builder as RuntimeBuilder;
use tracing::{info_span, Level, Span};
use tracing_subscriber::{fmt::format::FmtSpan, FmtSubscriber};

fn main() -> Result<(), Error> {
    let fmt_subscriber = FmtSubscriber::builder()
        .with_level(true)
        .with_max_level(Level::DEBUG)
        .with_span_events(FmtSpan::FULL)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(fmt_subscriber)?;

    let runtime = RuntimeBuilder::new_multi_thread()
        .enable_time()
        .build()
        .unwrap();

    let current_span = Span::current();
    let outer_span = info_span!(parent: &current_span, "outer");
    let guard = outer_span.enter();

    runtime.block_on(run())?;

    drop(guard);

    runtime.shutdown_timeout(Duration::from_secs(10));

    Ok(())
}

async fn run() -> Result<(), Error> {
    let blob = (0u8..100).collect::<Vec<_>>();

    let config = Config::default()
        .buffers_full_delay_ms(0)
        .part_size_bytes(10)
        .max_concurrency(1);

    let condow = InMemoryClient::<NoLocation>::new(blob)
        .condow(config)
        .unwrap();

    let stream = condow.download(NoLocation, ..).await?;
    let _downloaded = stream.into_vec().await?;

    println!("Download finished");

    Ok(())
}
