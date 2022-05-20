//! Simply produces some console output and a stack for a flamegraph
//!
//! Create flamegraph (copied from [tracing-flame](https://crates.io/crates/tracing-flame)):
//!
//! ```
//! cargo install inferno
//!
//! # flamegraph
//! cat tracing_flame.folded | inferno-flamegraph > tracing-flamegraph.svg
//!
//! # flamechart
//! cat tracing_flame.folded | inferno-flamegraph --flamechart > tracing-flamechart.svg
//! ```

use std::time::Duration;

use anyhow::Error;
use condow_core::{
    condow_client::failing_client_simulator::FailingClientSimulatorBuilder,
    config::Config,
    errors::{CondowError, CondowErrorKind},
};
use tokio::runtime::Builder as RuntimeBuilder;
use tracing::{info_span, Instrument};
use tracing_flame::FlameLayer;
use tracing_subscriber::{
    fmt::{format::FmtSpan, Layer},
    prelude::*,
    registry::Registry,
};

fn main() -> Result<(), Error> {
    let fmt_layer = Layer::default()
        .with_level(true)
        .with_target(false)
        //.with_span_events(FmtSpan::FULL) // Spams the log with lifecycle events of spans
        .with_span_events(FmtSpan::NEW)
        .with_line_number(false);

    let (flame_layer, _flame_guard) = FlameLayer::with_file("./tracing_flame.folded").unwrap();
    let flame_layer = flame_layer
        .with_threads_collapsed(true)
        .with_module_path(false)
        .with_file_and_line(false);

    let subscriber = Registry::default().with(fmt_layer).with(flame_layer);

    tracing::subscriber::set_global_default(subscriber)?;

    let runtime = RuntimeBuilder::new_multi_thread()
        .enable_time()
        .build()
        .unwrap();

    // Create an outer span which might be something created application side...
    let outer_span = info_span!("outer", "request_id" = 42);

    runtime.block_on(run().instrument(outer_span))?;

    runtime.shutdown_timeout(Duration::from_secs(10));

    Ok(())
}

async fn run() -> Result<(), Error> {
    let blob = (0u32..1_000).map(|x| x as u8).collect::<Vec<_>>();

    let config = Config::default()
        .max_buffers_full_delay_ms(0)
        .part_size_bytes(13)
        .max_concurrency(4);

    let condow = FailingClientSimulatorBuilder::default()
        .blob(blob)
        .chunk_size(7)
        .responses()
        .success()
        .failure(CondowErrorKind::Io)
        .success()
        .success_with_stream_failure(3)
        .success()
        .failures([CondowErrorKind::Io, CondowErrorKind::Remote])
        .success_with_stream_failure(6)
        .failure(CondowError::new_remote("this did not work"))
        .success_with_stream_failure(2)
        .finish()
        .condow(config)
        .unwrap();

    let stream = condow.blob().range(200..300).download().await?;
    let _downloaded = stream.into_vec().await?;

    println!("Download finished (not from tracing...)");

    Ok(())
}
