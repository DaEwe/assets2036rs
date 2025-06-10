use crate::Event; // Assuming Event is directly accessible, might need adjustment based on actual module structure
use log::{Level, Metadata, Record};
use serde_json::json;
use std::sync::Arc;

#[derive(Debug)]
pub struct RustLoggingHandler {
    // This will be Arc<crate::Event> after the refactor.
    // For now, to make it compilable before refactoring SubModel,
    // we might need a placeholder or accept that this part won't fully work until SubModel is refactored.
    // Let's proceed as if it's Arc<Event> as per the plan.
    pub log_event: Arc<Event>,
}

impl log::Log for RustLoggingHandler {
    fn enabled(&self, metadata: &Metadata) -> bool {
        // Potentially filter by level here if desired
        // For example: metadata.level() <= log::max_level()
        // Or, metadata.level() <= Level::Info to only log Info and above via this handler
        true // Enable for all levels for now
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        // Format the log message
        // Using module_path and line for more detailed logs, similar to common logging formats.
        let message = format!(
            "[{target} {line}] {args}",
            target = record.target(), // often module_path
            line = record.line().unwrap_or(0),
            args = record.args()
        );

        let params = json!({
            "level": record.level().to_string(),
            "message": message
        });

        let event_clone = Arc::clone(&self.log_event);

        // Spawn a Tokio task to send the log event asynchronously
        // This avoids blocking the logging call site.
        tokio::spawn(async move {
            match event_clone.trigger(params).await {
                Ok(_) => { /* Successfully triggered log event */ }
                Err(e) => {
                    // Log errors from triggering to stderr, as we can't use the logger itself here
                    // to avoid potential infinite loops.
                    eprintln!(
                        "Error triggering log_entry event for AssetManager: {:?}. Original log: [{}] {}",
                        e, record.level(), record.args()
                    );
                }
            }
        });
    }

    fn flush(&self) {
        // No-op, as our logging is fire-and-forget via MQTT events.
    }
}
