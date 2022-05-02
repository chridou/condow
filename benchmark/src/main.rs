use std::io;

mod benchmarks;
mod client;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let results = benchmarks::run().await.unwrap();

    let mut wtr = csv::Writer::from_writer(io::stdout());

    for stat in results.stats() {
        wtr.serialize(stat).unwrap();
    }

    wtr.flush().unwrap();
}
