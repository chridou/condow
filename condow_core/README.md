# Condow

Condow is a CONcurrent DOWnloader which downloads files
by splitting the download into parts and downloading them 
concurrently.

Some services/technologies/backends can have their download
speed improved, if files are downloaded concurrently by 
"opening multiple connections". An example for this is AWS S3.

This crate provides the core functionality only. To actually
use it, use one of the implementation crates:

* `condow_rusoto`: AWS S3

All that is required to add more "services" is to implement
the `CondowClient` trait.

## License

condow is distributed under the terms of both the MIT license and the Apache License (Version 2.0).

See LICENSE-APACHE and LICENSE-MIT for details.

License: Apache-2.0/MIT