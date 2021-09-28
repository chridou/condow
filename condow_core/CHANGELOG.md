# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.10.0] - 2021/09/27

### Added

* `AsyncRead` for stream of bytes
* `Asyncread` + `AsyncSeek` for downloaders (as seperate struct)

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