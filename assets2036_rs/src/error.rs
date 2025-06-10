use thiserror::Error;

/// The primary error type for the `assets2036_rs` library.
///
/// This enum encompasses various categories of errors that can occur during
/// asset interaction, communication, data processing, and other operations.
#[derive(Error, Debug)]
pub enum Error {
    /// Represents an error during JSON serialization or deserialization.
    /// Wraps a `serde_json::Error`.
    #[error("JSON processing error: {0}")]
    JsonError(#[from] serde_json::Error),

    /// Indicates that a provided parameter was invalid for a given context.
    #[error("Invalid parameter for {context}: {reason}")]
    InvalidParameter {
        /// The context or operation where the parameter validation failed.
        context: String,
        /// The specific reason why the parameter was considered invalid.
        reason: String,
    },

    /// Indicates an attempt to write to a property that is defined as read-only.
    #[error("Property is not writable")]
    NotWritable,

    /// Represents an error related to submodel definitions, such as parsing or validation issues.
    /// Contains a string describing the specific definition error.
    #[error("Submodel definition error: {0}")]
    SubmodelDefinitionError(String),

    /// Indicates an error occurred while trying to acquire a lock (e.g., `RwLock`),
    /// often due to lock poisoning from a previous panic.
    /// Contains details about the locking failure.
    #[error("Locking error (read/write). Details: {0}")]
    LockError(String),

    /// A general communication error not covered by more specific variants.
    /// Contains a string describing the communication issue.
    #[error("Communication error: {0}")]
    CommunicationError(String),

    /// Indicates that a remote operation call timed out.
    #[error("Operation '{operation}' timed out after {timeout_ms} ms")]
    OperationTimeoutError {
        /// The name or topic of the operation that timed out.
        operation: String,
        /// The timeout duration in milliseconds.
        timeout_ms: u64,
    },

    /// Indicates that an attempt to subscribe to an MQTT topic failed.
    #[error("Subscription to topic '{topic}' failed: {details}")]
    SubscriptionFailed {
        /// The topic for which the subscription failed.
        topic: String,
        /// Details about the subscription failure.
        details: String,
    },

    /// Indicates that an attempt to publish a message to an MQTT topic failed.
    #[error("Publish to topic '{topic}' failed: {details}")]
    PublishFailed {
        /// The topic to which publishing failed.
        topic: String,
        /// Details about the publishing failure.
        details: String,
    },

    /// Represents an error encountered while trying to connect to the communication broker (e.g., MQTT broker).
    #[error("Connection to {host}:{port} failed: {details}")]
    ConnectionError {
        /// The host address of the broker.
        host: String,
        /// The port number of the broker.
        port: u16,
        /// Details about the connection failure.
        details: String,
    },

    /// Indicates that a discovery query (e.g., for assets or submodels) did not yield any data within the expected timeout.
    /// Contains a string identifying the query or target.
    #[error("Discovery query for '{0}' failed to collect any data within timeout")]
    DiscoveryNoData(String),

    /// Indicates an error encountered while parsing a URL string.
    /// Contains a string describing the parsing error.
    #[error("URL parse error: {0}")]
    UrlParseError(String),

    /// Represents an error that occurred while trying to read a file from the filesystem (e.g., for a `file://` URL).
    #[error("File read error for path '{path}': {details}")]
    FileReadError {
        /// The path of the file that could not be read.
        path: String,
        /// Details about the file read error, typically from `std::io::Error`.
        details: String
    },

    /// Represents an error that occurred during an HTTP request (e.g., for an `http://` or `https://` URL).
    #[error("HTTP request error for URL '{url}': {details}")]
    HttpRequestError {
        /// The URL for which the HTTP request failed.
        url: String,
        /// Details about the HTTP request error, often from the underlying HTTP client (e.g., `reqwest`).
        details: String
    },

    /// Indicates that a remote asset did not report as "online" within a specified timeout period.
    #[error("Asset {namespace}/{name} did not come online within {timeout_secs} seconds")]
    AssetNotOnlineError {
        /// The name of the asset.
        name: String,
        /// The namespace of the asset.
        namespace: String,
        /// The timeout duration in seconds that was exceeded.
        timeout_secs: u64,
    },

    /// Indicates that a requested asset could not be found (e.g., during proxy creation or query).
    #[error("Asset not found: {namespace}/{name}")]
    AssetNotFoundError {
        /// The namespace of the asset that was not found.
        namespace: String,
        /// The name of the asset that was not found.
        name: String
    },

    /// Indicates that a submodel's `_meta` information was missing or contained an invalid submodel definition.
    /// This typically occurs during asset proxy creation when processing discovered submodels.
    #[error("Submodel definition missing or invalid in _meta for {submodel_name} of asset {asset_name}")]
    InvalidMetaSubmodelDefinition {
        /// The name of the submodel whose `_meta` data was problematic.
        submodel_name: String,
        /// The name of the asset to which the submodel belongs.
        asset_name: String,
    },

    /// Represents an error specific to asset relationship operations (e.g., setting `belongs_to`).
    /// Contains a string describing the relation error.
    #[error("Asset relation error: {0}")]
    RelationError(String),

    /// A catch-all for other types of errors not covered by more specific variants.
    /// Usage of this variant should be minimized in favor of more specific errors.
    /// Contains a string describing the error.
    #[error("Other error: {0}")]
    Other(String),
}
