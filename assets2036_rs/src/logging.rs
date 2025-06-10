use crate::Event;
use log::{Level, Metadata, Record};
use serde_json::json;
use std::sync::Arc;

/// A logging handler that forwards log records to an ASSETS2036 event.
///
/// This struct implements the `log::Log` trait and is intended to be used
/// with the `log` crate facade. When registered as a logger, it takes log records,
/// formats them, and then triggers a specified `log_entry` event on an asset's
/// `_endpoint` submodel (typically the AssetManager's endpoint).
#[derive(Debug)]
pub struct RustLoggingHandler {
    /// The `Arc<Event>` representing the `_endpoint/log_entry` event.
    /// Formatted log messages will be sent by triggering this event.
    pub log_event: Arc<Event>,
}

impl log::Log for RustLoggingHandler {
    /// Checks if a log record with the given metadata would be enabled.
    ///
    /// # Parameters
    /// * `metadata`: The metadata for the log record, including level and target.
    ///
    /// # Returns
    /// Currently always returns `true`, meaning all log levels passed by the `log` facade
    /// will be processed by the `log` method. This could be extended to filter
    /// by `metadata.level()` or `metadata.target()`.
    fn enabled(&self, _metadata: &Metadata) -> bool {
        // Potentially filter by level here if desired
        // For example: metadata.level() <= log::max_level()
        // Or, metadata.level() <= Level::Info to only log Info and above via this handler
        true
    }

    /// Processes a log record.
    ///
    /// This method formats the log record into a string including its level,
    /// target (module path), line number, and message. It then constructs a JSON payload
    /// with "level" and "message" fields and asynchronously triggers the `log_event`
    /// (assumed to be `_endpoint/log_entry`).
    ///
    /// If triggering the event fails (e.g., due to a communication error), the error
    /// along with the original log message is printed to `stderr` using `eprintln!`.
    /// This is to avoid potential infinite loops if the error logging itself used this handler.
    ///
    /// # Parameters
    /// * `record`: The log record to process, provided by the `log` facade.
    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let message = format!(
            "[{target} {line}] {args}",
            target = record.target(),
            line = record.line().unwrap_or(0),
            args = record.args()
        );

        let params = json!({
            "level": record.level().to_string(),
            "message": message
        });

        let event_clone = Arc::clone(&self.log_event);

        tokio::spawn(async move {
            match event_clone.trigger(params).await {
                Ok(_) => { /* Successfully triggered log event */ }
                Err(e) => {
                    eprintln!(
                        "Error triggering log_entry event for AssetManager: {:?}. Original log: [{}] {}",
                        e, record.level(), record.args()
                    );
                }
            }
        });
    }

    /// Flushes any buffered log records.
    ///
    /// This implementation is a no-op, as log messages are sent asynchronously
    /// via MQTT events without intermediate buffering within this handler.
    fn flush(&self) {
        // No-op
    }
}
