#![deny(warnings)]

use std::str::from_utf8;

use anyhow::Error as AnyError;

use condow_core::config::Config;
use condow_http::HttpClient;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), AnyError> {
    let config = Config::default().buffer_size(1).part_size_bytes(1000);
    let condow = HttpClient::condow(config)?;

    let url = "https://gist.githubusercontent.com/nyurik/88730133a8d00ead67ac8520640e1fc1/raw/29459acf2aef3281489d0eb1b09f491831cf2324/denormalize_osm_data.md";
    let res = condow
        .blob()
        .at(String::from(url))
        .download_into_vec()
        .await
        .unwrap();
    println!("{:?}", from_utf8(res.as_ref()));
    Ok(())
}
