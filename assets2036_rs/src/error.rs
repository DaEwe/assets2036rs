use thiserror::Error;
#[derive(Error, Debug)]
pub enum Error {
    #[error("JSON processing error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Invalid parameter for {context}: {reason}")]
    InvalidParameter { context: String, reason: String },
    #[error("Property is not writable")]
    NotWritable,
    #[error("Submodel definition error: {0}")]
    SubmodelDefinitionError(String), // Can be enhanced if specific parsing errors are caught
    #[error("Locking error (read/write). Details: {0}")]
    LockError(String), // Added details, as poisoning doesn't give a source directly for #[from]
    #[error("Communication error: {0}")]
    CommunicationError(String),
    #[error("Operation '{operation}' timed out after {timeout_ms} ms")]
    OperationTimeoutError { operation: String, timeout_ms: u64 },
    #[error("Subscription to topic '{topic}' failed: {details}")]
    SubscriptionFailed { topic: String, details: String },
    #[error("Publish to topic '{topic}' failed: {details}")]
    PublishFailed { topic: String, details: String },
    #[error("Connection to {host}:{port} failed: {details}")]
    ConnectionError {
        host: String,
        port: u16,
        details: String,
    },
    #[error("URL parse error: {0}")]
    UrlParseError(String),
    #[error("File read error for path '{path}': {details}")]
    FileReadError { path: String, details: String },
    #[error("HTTP request error for URL '{url}': {details}")]
    HttpRequestError { url: String, details: String },
    #[error("Discovery query for '{0}' failed to collect any data within timeout")]
    DiscoveryNoData(String),
    #[error("Asset relation error: {0}")]
    RelationError(String),
    #[error("Asset {namespace}/{name} did not come online within {timeout_secs} seconds")]
    AssetNotOnlineError {
        name: String,
        namespace: String,
        timeout_secs: u64,
    },
    #[error("Asset not found: {namespace}/{name}")]
    AssetNotFoundError { namespace: String, name: String },
    #[error(
        "Submodel definition missing or invalid in _meta for {submodel_name} of asset {asset_name}"
    )]
    InvalidMetaSubmodelDefinition {
        submodel_name: String,
        asset_name: String,
    },
    #[error("Other error: {0}")] // Kept for unavoidable generic errors, but usage should be minimized
    Other(String),
}
