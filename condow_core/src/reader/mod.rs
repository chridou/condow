/// Implementations of async readers for downloads
///
/// Mostly for interfacing with other libraries.
pub use bytes_async_reader::*;
pub use random_access_reader::*;

mod bytes_async_reader;
mod random_access_reader;