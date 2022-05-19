# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## unreleased

### FIXED

- Events for download started and finished on Probes. Same for parts.

### CHANGED

- Updated documenation on parallelism behaviour

### ADDED

- config parameter `EnsureActivePull` to allow the stream to always be pulled by a dedicated task. This also gives some panic protection.
- config parameter `LogDownloadMessagesAsDebug` to configure log level of outer doenload events
- config parameters `MinPartsForConcurrentDownload`, `MinBytesForConcurrentDownload` and `SequentialDownloadMode` to configure sequential downloads

## 0.17.1 - 2022-05-12

### CHANGED

- `Probe` is handled with static dispatch internally
- Deprecated `Probe::chunk_completed` (time parameter was inaccurate and had a negative performance impact). Use new method `chunk_received` instead.
- improved performance on sequential downloads

## 0.17.0 - 2022-05-05

### CHANGED

- **BREAKING** `BuffersFullDelayMs` became `MaxBuffersFullDelayMs`.

### FIXED

- performance issues

## 0.16.0 - 2022-05-01

### CHANGED

- Download requests can change the configuration for the requested download

### REMOVED

- **BREAKING** `GetSizeMode`

## 0.15.0 - 2022-04-29

### CHANGED

- ***BREAKING*** Switched global instrumentation back to static dispatch

## 0.14.4 - 2022-04-29

### ADDED

- documentation for global instrumentation

## 0.14.3 - 2022-04-29

### ADDED

- documentation for per request probing

## 0.14.2 - 2022-04-28

### CHANGED

- In `OrderedChunkStream` replaced `dyn Stream` with concrete type `UnboundedReceiver`

## 0.14.1 - 2022-04-28

### ADDED

- documentation and tests

## 0.14.0 - 2022-04-28

### CHANGED

- `PartStream` has been replaced by an `OrderedChunkStream` to achieve a lower first byte to client latency

## 0.13.0 - 2022-04-27

### CHANGED

- **BREAKING** Downloading is done via a builder style request object
- **BREAKING** `Reporter` is used dynamically and now called `Probe`
- **BREAKING** Trait `Downloads` uses request API and has location as associated type
- **BREAKING** `RandomAccessReader` has type parameter removed
- **BREAKING** Renamed `NoLocation` to `IgnoreLocation`

### ADDED

- support of `tracing crate`
- trait `DownloadsUntyped` which takes a `&str` as a location

### REMOVED

- **BREAKING** Old logging mechanism via reporter 
- **BREAKING** `CompositeReporter`

## [0.12.4] - 2022-02-08

### ADDED

- `Reporter` can track failed parts
- `Logger` logs failed parts

## [0.12.3] - 2022-02-07

### ADDED

- log time of failed download

## [0.12.2] - 2022-02-07

### FIXED

- logging levels
## [0.12.1] - 2022-02-07

### FIXED

- return a stream error when panicking while downloading
- return a stream error when panicking while retrying

### ADDED

- add a function to `Reporter` trait to track panics
- documentation
- `FailingClientSimulator` can panic while streaming
- Display for `BytesHint`
- Logging via the `Reporter` trait

### CHANGED

- `FailingClientSimulator` does stream errors based on the requested range

### REMOVED

- location method from `Reporter` trait. Use constructor to set a location.

## [0.12.0] - 2022-01-19

### ADDED

- `RetryConfig` to configure a optional retries
- Changed the behaviour so that all requests can do retries. Also broken streams will be retried with the remaining data only queried.
- `Reporter` trait is notified on retries and resumed streams
- `CompositeReporter` propagates retry and broken stream to children
- more documentation

### CHANGED

- **BREAKING**: Config does no longer implement the `Eq` trait (there is now an f64 in there)
- **BREAKING**: Added a new field for retry config to the Config struct
- **BREAKING**: Config is now non-exhaustive
- **BREAKING**: Configuration from env on `Config` can now return `None` if no values were found in the env. This was necessary to manage nested configurations.
- **BREAKING**:  `InMemoryClient` can also handle static BLOBs. Ctor funs changed.
- `NoLocation` type available without test config

### Removed

- `StaticBlobCient`: `InMemoryClient` can also handle static BLOBs



## [0.11.0] -  2021/12/11

### Fixed

- Async reader did not return an error when offset was set before byte 0.

### Added

- `value` const methods added to units `Mebi`, `Gibi`, etc for allowing them to be used to initialize constants
- Documentation for async reader iteself and on the `Downloads` trait.

### Changes

- Breaking: In meory clients are generic over the location. Default type is `NoLocation`
- Updated README.md

## [0.10.2] - 2021/10/22

### Added

* `InMemoryClient` and `StaticBlobClient` for testing with in memory blobs

## [0.10.1] - 2021/10/20

### Added

* `Display` for `DownloadRange`

## [0.10.0] - 2021/10/19

### Added

* `AsyncRead` for stream of bytes
* `Asyncread` + `AsyncSeek` for downloaders (as seperate struct)

### CHANGED

* Use `u64` in range or IO based intefaces

### Removed

* `MultiRangeDownloader`

## [0.9.3] - 2021/09/21

### ADDED

* `ChunkStream` can be turned into a `PartStream` directly
* `TryFrom<ChukStream>` for `PartStream`

## [0.9.2] - 2021/09/21
### CHANGED

* make fields of `RangeChunk` public

## [0.9.1] - 2021/09/21

### ADDED

* Range adapter for offset and length

### CHANGED

* Ranges in multi downloads must be `Clone`
## [0.9.0] - 2021/09/21

### ADDED

* Download multiple ranges at once

## [0.8.1] - 2021/08/24

### FIXED

* `DownloadSession` did not always report on downloading

## [0.8.0] - 2021/08/24

### FIXED

* Wrong documentation on `Chunk::is_last()`

### CHANGED

* `get_size returns` `u64`

## [0.7.0] - 2021/08/23

### CHANGED

* `SimpleReporter` cn skip first chunk of a part when measuring timings

## [0.6.1] - 2021/08/22

### FIXED

* Report part download time as microseconds in `SimpleReport`

## [0.6.0] - 2021/08/22

### ADDED

* `CompositeReporter`

### CHANGED

* `CondowError`
* Report range and location

### Removed

* `GetSizeError`

## [0.5.0] - 2021/08/21

### ADDED

* Download session

### CHANGED

* Reporter interface

### CHANGED

* `SimpleReport` measures throughput with `f64`
## [0.4.1] - 2021/08/20

* Added a field `is_finished` to `SimpleReport`

## [0.4.0] - 2021/08/20

### CHANGED

* Rename `Outcome` to `StreamWithReport`
* Download location requires `Display`
* Reworked APIs

### ADDED 

* Trait `Downloads`
* Conversions for `InclusiveRange`

### REMOVED

* struct `ExclusiveOpenRange`
## [0.3.0] - 2021/08/20

### FIXED

* Fixed bug with reentrant lock in `SompleReport`

### CHANGED

* Downloader API extended and changed

### CHANGES

* Major changes in API of `condow_core`
## [0.2.1] - 2021/08/19

refactoring

## [0.2.0] - 2021/08/19

### CHANGED

* breaking API changes

### ADDED

* Request instrumentation

## [0.1.4] - 2021/08/18

### ADDED

* get_size method for downloader

## [0.1.4] - 2021/08/18

### FIXED

* removed leftover dbg!() usage

## [0.1.3] - 2021/08/18

### ADDED

* Part got a len function, too
## [0.1.2] - 2021/08/18

### ADDED

* tests and doc for byte units
* methods to fill buffers from `PartStream`
* tests for `PartStream`

### CHANGED

* deprecated "fill_buffer" in favour of "write_buffer"

## [0.1.1] - 2021/08/17

### FIXED

* parsing of `PartSizeBytes`
## [0.1.0] - 2021/08/17

### ADDED

* Initial release