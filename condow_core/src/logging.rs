//! Reporters which do logging

use std::{fmt, sync::Arc};

use super::{Reporter, ReporterFactory};

/// A logger logging on events send to a [Reporter]
///
/// `Logger` implements [Reporter]. Therefore reporting
/// must be enabled. This can be done by directly passing
/// an instance to a download method that accepts a reporter
/// or by setting the [ReporterFactory] of one of the downloaders.
/// To ensure logging for all downloads using a [DownloadSession]
/// with the [LoggerFactory] as a [ReporterFactory] is recommended.
///
/// [DownloadSession]: crate::DownloadSession
#[derive(Clone)]
pub struct Logger {
    location: Option<Arc<String>>,
    inner: Inner,
}

impl Logger {
    /// Create a new instance by giving callbacks for various log levels
    ///
    /// Setting `None` disables logging on that level.
    ///
    /// The callbacks are called on the same thread as the downloading operation and
    /// therefore should be quick and non blocking.
    pub fn create(
        on_debug: Option<Arc<dyn Fn(&str, fmt::Arguments) + Send + Sync + 'static>>,
        on_info: Option<Arc<dyn Fn(&str, fmt::Arguments) + Send + Sync + 'static>>,
        on_warn: Option<Arc<dyn Fn(&str, fmt::Arguments) + Send + Sync + 'static>>,
        on_error: Option<Arc<dyn Fn(&str, fmt::Arguments) + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            location: None,
            inner: Inner {
                on_debug,
                on_info,
                on_warn,
                on_error,
            },
        }
    }

    /// Log a debug message
    pub fn debug(&self, msg: fmt::Arguments) {
        self.inner
            .on_debug
            .as_deref()
            .into_iter()
            .for_each(|f| f(self.location(), msg));
    }

    /// Log an info message
    pub fn info(&self, msg: fmt::Arguments) {
        self.inner
            .on_info
            .as_deref()
            .into_iter()
            .for_each(|f| f(self.location(), msg));
    }

    /// Log a warning message
    pub fn warn(&self, msg: fmt::Arguments) {
        self.inner
            .on_warn
            .as_deref()
            .into_iter()
            .for_each(|f| f(self.location(), msg));
    }

    /// Log an error message
    pub fn error(&self, msg: fmt::Arguments) {
        self.inner
            .on_error
            .as_deref()
            .into_iter()
            .for_each(|f| f(self.location(), msg));
    }

    /// The location of the download this reporter is referring to
    pub fn location(&self) -> &str {
        match self.location.as_ref() {
            Some(l) => &l,
            None => "<unknown>",
        }
    }
}

impl Reporter for Logger {
    fn effective_range(&self, _range: crate::InclusiveRange) {}

    fn download_started(&self) {
        self.info(format_args!("Download started"));
    }

    fn download_completed(&self, _time: std::time::Duration) {
        self.info(format_args!("Download completed"));
    }

    fn download_failed(&self, time: Option<std::time::Duration>) {
        if let Some(time) = time {
            self.error(format_args!("Download failed after {:?}", time));
        } else {
            self.error(format_args!("Download failed"));
        }
    }

    fn retry_attempt(
        &self,
        _location: &dyn fmt::Display,
        error: &crate::errors::CondowError,
        next_in: std::time::Duration,
    ) {
        self.warn(format_args!("retry in {:?} on error '{}'", next_in, error));
    }

    fn stream_resume_attempt(
        &self,
        _location: &dyn fmt::Display,
        error: &crate::errors::IoError,
        orig_range: crate::InclusiveRange,
        remaining_range: crate::InclusiveRange,
    ) {
        self.warn(format_args!(
            "stream resume attempt for remaining \
        range {} on range {} on error '{}'",
            error, orig_range, remaining_range
        ));
    }

    fn panic_detected(&self, msg: &str) {
        self.warn(format_args!("panic detected '{}'", msg));
    }

    fn queue_full(&self) {}

    fn chunk_completed(
        &self,
        _part_index: u64,
        _chunk_index: usize,
        _n_bytes: usize,
        _time: std::time::Duration,
    ) {
    }

    fn part_started(&self, part_index: u64, range: crate::InclusiveRange) {
        self.debug(format_args!(
            "Download of part {} ({}) started",
            part_index, range
        ));
    }

    fn part_completed(
        &self,
        part_index: u64,
        n_chunks: usize,
        n_bytes: u64,
        time: std::time::Duration,
    ) {
        self.debug(format_args!(
            "Download of part {} ({} bytes, {} chunks, time: {:?}) finished",
            part_index, n_bytes, n_chunks, time
        ));
    }
}

/// A builder for a [LoggerFactory]
#[derive(Default)]
pub struct LoggerFactoryBuilder {
    on_debug: Option<Arc<dyn Fn(&str, fmt::Arguments) + Send + Sync + 'static>>,
    on_info: Option<Arc<dyn Fn(&str, fmt::Arguments) + Send + Sync + 'static>>,
    on_warn: Option<Arc<dyn Fn(&str, fmt::Arguments) + Send + Sync + 'static>>,
    on_error: Option<Arc<dyn Fn(&str, fmt::Arguments) + Send + Sync + 'static>>,
}

impl LoggerFactoryBuilder {
    /// Set a shared dynamic callback for logging debug messages
    pub fn on_debug_dyn(
        mut self,
        on_debug: Arc<dyn Fn(&str, fmt::Arguments) + Send + Sync + 'static>,
    ) -> Self {
        self.on_debug = Some(on_debug);
        self
    }

    /// Set a callback for logging debug messages
    pub fn on_debug<F>(self, on_debug: F) -> Self
    where
        F: Fn(&str, fmt::Arguments) + Send + Sync + 'static,
    {
        self.on_debug_dyn(Arc::new(on_debug))
    }

    /// Set a shared dynamic callback for logging info messages
    pub fn on_info_dyn(
        mut self,
        on_info: Arc<dyn Fn(&str, fmt::Arguments) + Send + Sync + 'static>,
    ) -> Self {
        self.on_info = Some(on_info);
        self
    }

    /// Set a callback for logging info messages
    pub fn on_info<F>(self, on_info: F) -> Self
    where
        F: Fn(&str, fmt::Arguments) + Send + Sync + 'static,
    {
        self.on_info_dyn(Arc::new(on_info))
    }

    /// Set a shared dynamic callback for logging warning messages
    pub fn on_warn_dyn(
        mut self,
        on_warn: Arc<dyn Fn(&str, fmt::Arguments) + Send + Sync + 'static>,
    ) -> Self {
        self.on_warn = Some(on_warn);
        self
    }

    /// Set a callback for logging warning messages
    pub fn on_warn<F>(self, on_warn: F) -> Self
    where
        F: Fn(&str, fmt::Arguments) + Send + Sync + 'static,
    {
        self.on_warn_dyn(Arc::new(on_warn))
    }

    /// Set a shared dynamic callback for logging error messages
    pub fn on_error_dyn(
        mut self,
        on_error: Arc<dyn Fn(&str, fmt::Arguments) + Send + Sync + 'static>,
    ) -> Self {
        self.on_error = Some(on_error);
        self
    }

    /// Set a callback for logging error messages
    pub fn on_error<F>(self, on_error: F) -> Self
    where
        F: Fn(&str, fmt::Arguments) + Send + Sync + 'static,
    {
        self.on_error_dyn(Arc::new(on_error))
    }

    /// Returns a [StdOutConfigurator] to configure a default logging format
    /// printed on stdout.
    pub fn std_out(self) -> StdOutConfigurator {
        StdOutConfigurator { builder: self }
    }

    /// Returns a [StdErrConfigurator] to configure a default logging format
    /// printed on stderr.
    pub fn std_err(self) -> StdErrConfigurator {
        StdErrConfigurator { builder: self }
    }

    /// Create the logging factory
    pub fn finish(self) -> LoggerFactory {
        LoggerFactory {
            inner: Inner {
                on_debug: self.on_debug,
                on_info: self.on_info,
                on_warn: self.on_warn,
                on_error: self.on_error,
            },
        }
    }
}

/// Configure preformatted logging to stdout
pub struct StdOutConfigurator {
    builder: LoggerFactoryBuilder,
}

impl StdOutConfigurator {
    /// Enable debug logging on stdout
    pub fn debug(mut self) -> Self {
        self.builder = self.builder.on_debug(|loc, msg| {
            println!("{:5}|{}|{}", "DEBUG", loc, msg);
        });
        self
    }

    /// Enable info logging on stdout
    pub fn info(mut self) -> Self {
        self.builder = self.builder.on_info(|loc, msg| {
            println!("{:5}|{}|{}", "INFO", loc, msg);
        });
        self
    }

    /// Enable warning logging on stdout
    pub fn warn(mut self) -> Self {
        self.builder = self.builder.on_warn(|loc, msg| {
            println!("{:5}|{}|{}", "WARN", loc, msg);
        });
        self
    }

    /// Enable error logging on stdout
    pub fn error(mut self) -> Self {
        self.builder = self.builder.on_error(|loc, msg| {
            println!("{:5}|{}|{}", "ERROR", loc, msg);
        });
        self
    }

    /// Enable warn and error logging on stdout
    pub fn warn_error(self) -> Self {
        self.warn().error()
    }

    /// Enable info, warn and error logging on stdout
    pub fn info_warn_error(self) -> Self {
        self.info().warn().error()
    }

    /// Enable debug, info, warn and error logging on stdout
    pub fn all(self) -> Self {
        self.debug().info().warn().error()
    }

    /// Get back to the [LoggerFactoryBuilder]
    pub fn done(self) -> LoggerFactoryBuilder {
        self.builder
    }

    /// Build the [LoggerFactory]
    pub fn finish(self) -> LoggerFactory {
        self.builder.finish()
    }
}

/// Configure preformatted logging to stderr
pub struct StdErrConfigurator {
    builder: LoggerFactoryBuilder,
}

impl StdErrConfigurator {
    /// Enable debug logging on stderr
    pub fn debug(mut self) -> Self {
        self.builder = self.builder.on_debug(|loc, msg| {
            eprintln!("{:5}|{}|{}", "DEBUG", loc, msg);
        });
        self
    }

    /// Enable info logging on stderr
    pub fn info(mut self) -> Self {
        self.builder = self.builder.on_info(|loc, msg| {
            eprintln!("{:5}|{}|{}", "INFO", loc, msg);
        });
        self
    }

    /// Enable warning logging on stderr
    pub fn warn(mut self) -> Self {
        self.builder = self.builder.on_warn(|loc, msg| {
            eprintln!("{:5}|{}|{}", "WARN", loc, msg);
        });
        self
    }

    /// Enable error logging on stderr
    pub fn error(mut self) -> Self {
        self.builder = self.builder.on_error(|loc, msg| {
            eprintln!("{:5}|{}|{}", "ERROR", loc, msg);
        });
        self
    }

    /// Enable warn and error logging on stderr
    pub fn warn_error(self) -> Self {
        self.warn().error()
    }

    /// Enable info, warn and error logging on stderr
    pub fn info_warn_error(self) -> Self {
        self.info().warn().error()
    }

    /// Enable debug, info, warn and error logging on stderr
    pub fn all(self) -> Self {
        self.debug().info().warn().error()
    }

    /// Get back to the [LoggerFactoryBuilder]
    pub fn done(self) -> LoggerFactoryBuilder {
        self.builder
    }

    /// Build the [LoggerFactory]
    pub fn finish(self) -> LoggerFactory {
        self.builder.finish()
    }
}

/// A factory for [Logger]s which can be used to create
/// [Logger]s for downloads.
///
/// `LoggerFactory` implements [ReporterFactory].
/// To ensure logging for all downloads using a [DownloadSession]
/// with the [LoggerFactory] as a [ReporterFactory] is recommended.
///
/// [DownloadSession]: crate::DownloadSession
#[derive(Clone)]
pub struct LoggerFactory {
    inner: Inner,
}

impl ReporterFactory for LoggerFactory {
    type ReporterType = Logger;

    fn make(&self, location: &dyn fmt::Display) -> Self::ReporterType {
        Logger {
            location: Some(Arc::new(location.to_string())),
            inner: self.inner.clone(),
        }
    }
}

#[derive(Clone)]
struct Inner {
    on_debug: Option<Arc<dyn Fn(&str, fmt::Arguments) + Send + Sync + 'static>>,
    on_info: Option<Arc<dyn Fn(&str, fmt::Arguments) + Send + Sync + 'static>>,
    on_warn: Option<Arc<dyn Fn(&str, fmt::Arguments) + Send + Sync + 'static>>,
    on_error: Option<Arc<dyn Fn(&str, fmt::Arguments) + Send + Sync + 'static>>,
}
