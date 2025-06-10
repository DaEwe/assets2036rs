use thiserror::Error;
#[derive(Error, Debug)]
pub enum Error {
    #[error("JSON processing error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),
    #[error("Property is not writable")]
    NotWritable,
    #[error("Submodel definition error: {0}")]
    SubmodelDefinitionError(String),
    #[error("Locking error (read/write)")]
    LockError,
    #[error("Other error: {0}")]
    Other(String),
    #[error("Discovery query for '{0}' failed to collect any data within timeout")]
    DiscoveryNoData(String),
    #[error("URL loading from {0} not yet implemented")]
    UrlLoadingNotImplemented(String),
    #[error("Asset not found: {namespace}/{name}")]
    AssetNotFoundError { namespace: String, name: String },
    #[error(
        "Submodel definition missing or invalid in _meta for {submodel_name} of asset {asset_name}"
    )]
    InvalidMetaSubmodelDefinition {
        submodel_name: String,
        asset_name: String,
    },
}
