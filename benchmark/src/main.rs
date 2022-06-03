use std::io;

use tokio::task::spawn_blocking;

mod benchmarks;
mod client;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let results = benchmarks::run(103).await.unwrap();

    let handle = spawn_blocking(move || {
        let mut wtr = csv::Writer::from_writer(io::stdout());

        for stat in results.stats() {
            wtr.serialize(stat).unwrap();
        }

        wtr.flush().unwrap();
    });

    handle.await.unwrap();
}
