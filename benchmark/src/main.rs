use std::{time::{Duration, Instant}, collections::BTreeMap};

use client::BenchmarkClient;
use condow_core::config::Mebi;

mod client;
mod benchmarks;

#[tokio::main]
async fn main() {
    let results = benchmarks::run().await.unwrap();

    println!("{results:#?}")
}

fn create_client() -> BenchmarkClient {
    BenchmarkClient::new(Mebi(16).value(), 7489) // Prime number
}

#[derive(Debug)]
pub struct Measurements {
    name: String,
    measurements: Vec<Duration>,
}

impl Measurements {
    pub fn new<T: Into<String>>(name: T) -> Self {
        Self {
            name: name.into(),
            measurements: Vec::with_capacity(128),
        }
    }

    pub fn measurement(&mut self, start: Instant, end: Instant) {
        self.measurements.push(end-start);
    }
}

#[derive(Debug)]
pub struct Benchmarks {
    measurements: BTreeMap<String, Measurements>
}

impl Benchmarks {
    pub fn new() -> Self {
        Benchmarks { measurements: BTreeMap::new() }
    }

    pub fn add_measurements(&mut self, measurements: Measurements) {
        self.measurements.insert(measurements.name.clone(), measurements);
    }
}
