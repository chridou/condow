# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - unreleased

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