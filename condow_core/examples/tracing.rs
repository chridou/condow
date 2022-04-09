//! Simply produces some console output

use std::time::Duration;

use anyhow::Error;
use condow_core::{
    condow_client::{InMemoryClient, NoLocation},
    config::Config,
};
use tokio::runtime::Builder as RuntimeBuilder;
use tracing::{info_span, Instrument, Level};
use tracing_subscriber::{fmt::format::FmtSpan, FmtSubscriber};

fn main() -> Result<(), Error> {
    let fmt_subscriber = FmtSubscriber::builder()
        .with_level(true)
        .with_max_level(Level::TRACE)
        .with_span_events(FmtSpan::FULL) // Logs (spams) lifecycle events of spans
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(fmt_subscriber)?;

    let runtime = RuntimeBuilder::new_multi_thread()
        .enable_time()
        .build()
        .unwrap();

    // Create an outer span which might be something created application side...
    let outer_span = info_span!("outer");

    runtime.block_on(run().instrument(outer_span))?;

    runtime.shutdown_timeout(Duration::from_secs(10));

    Ok(())
}

async fn run() -> Result<(), Error> {
    let blob = (0u8..100).collect::<Vec<_>>();

    let config = Config::default()
        .buffers_full_delay_ms(0)
        .part_size_bytes(10)
        .max_concurrency(2);

    let condow = InMemoryClient::<NoLocation>::new(blob)
        .condow(config)
        .unwrap();

    let stream = condow.download(NoLocation, ..).await?;
    let _downloaded = stream.into_vec().await?;

    println!("Download finished (not from tracing...)");

    Ok(())
}
