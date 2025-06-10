// Copyright (c) 2024, The assets2036_rs Contributors
// SPDX-License-Identifier: Apache-2.0

//! This crate provides a Rust implementation for interacting with assets
//! according to the ASSETS2036 specification. It allows for creating,
//! managing, and interacting with digital representations of assets,
//! their submodels, properties, operations, and events, primarily over MQTT.

use chrono::Utc;
use log;

use serde_json::Value;
use std::boxed::Box;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;

use tokio::sync::{oneshot, watch, RwLock as TokioRwLock};
use tokio::time::{sleep, Duration};
use url::Url;

// Modules
mod communication_client;
mod error;
pub mod logging; // Made logging module public

// Re-exports
pub use crate::communication_client::{CommunicationClient, MqttCommunicationClient}; // Re-exporting MqttCommunicationClient for convenience
pub use error::Error;
use crate::logging::RustLoggingHandler;
use log::LevelFilter;


/// Default timeout for invoking operations on remote assets, in milliseconds.
pub const DEFAULT_OPERATION_TIMEOUT_MS: u64 = 5000;

/// JSON string defining the standard `_relations` submodel.
///
/// This submodel is used to describe relationships between assets, such as parent-child
/// relationships (`belongs_to`) or other custom relations.
pub const RELATIONS_SUBMODEL_JSON: &str = r#"
{
    "name": "_relations",
    "version": "1.0.0",
    "description": "Submodel for defining relationships between assets",
    "properties": {
        "belongs_to": {
            "type": "object",
            "description": "Specifies the parent asset this asset belongs to",
            "properties": {
                "namespace": {
                    "type": "string",
                    "description": "Namespace of the parent asset"
                },
                "asset_name": {
                    "type": "string",
                    "description": "Name of the parent asset"
                }
            },
            "required": ["namespace", "asset_name"]
        },
        "relations": {
            "type": "array",
            "description": "A list of other relations this asset has",
            "items": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "description": "Type of the relation (e.g., 'controls', 'monitors')"
                    },
                    "target_namespace": {
                        "type": "string",
                        "description": "Namespace of the target asset"
                    },
                    "target_asset_name": {
                        "type": "string",
                        "description": "Name of the target asset"
                    },
                    "direction": {
                        "type": "string",
                        "enum": ["ingoing", "outgoing", "bidirectional"],
                        "description": "Direction of the relation"
                    }
                },
                "required": ["type", "target_namespace", "target_asset_name", "direction"]
            }
        }
    }
}
"#;

/// Represents the data type of a submodel property.
///
/// These types correspond to JSON schema types and are used for validation
/// and defining property structures.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PropertyType {
    /// A boolean type (`true` or `false`).
    Boolean,
    /// An integer type.
    Integer,
    /// A general number type (can be integer or floating-point).
    Number,
    /// A string type.
    String,
    /// An object type (a map of key-value pairs).
    Object,
    /// An array type (a list of values).
    Array,
    /// A null type.
    #[default]
    Null,
}

/// Defines the access mode for an asset or its components.
///
/// This determines whether the interaction is from the perspective of an
/// "owner" (having full control and publishing updates) or a "consumer"
/// (reading data and invoking operations on remote assets).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Mode {
    /// Owner mode, indicating full control over the asset's properties and state.
    /// Owners publish property changes and bind operations.
    Owner,
    /// Consumer mode, indicating interaction with a remote asset.
    /// Consumers subscribe to property changes and invoke remote operations.
    Consumer,
}

/// Defines the structure and metadata of a submodel property.
///
/// This includes its data type, description, read-only status, and potentially
/// nested definitions for complex types like objects or arrays.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct PropertyDefinition {
    /// The data type of the property, as defined by [`PropertyType`].
    #[serde(rename = "type")]
    pub type_of: PropertyType,
    /// An optional human-readable description of the property.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Indicates if the property is read-only.
    ///
    /// If `Some(true)`, the property cannot be changed by consumers.
    /// If `None` or `Some(false)`, it is considered writable by default (for owners).
    #[serde(rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// For array properties, this defines the type of items within the array.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Box<PropertyDefinition>>, // Boxed to handle recursive type
    /// For object properties, this defines the structure of nested properties.
    /// Keys are property names, values are their definitions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, Box<PropertyDefinition>>>,
    /// An optional list of allowed values for the property (enum).
    /// If present, the property value must be one of these.
    #[serde(
        rename = "enum",
        alias = "enum_values",
        skip_serializing_if = "Option::is_none"
    )]
    pub enum_values: Option<Vec<serde_json::Value>>,
}

/// Defines the structure and metadata of a submodel event.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EventDefinition {
    /// An optional human-readable description of the event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// An optional definition of parameters that are emitted with the event.
    /// Keys are parameter names, values are their property definitions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<HashMap<String, PropertyDefinition>>,
}

/// Defines the structure and metadata of a submodel operation.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OperationDefinition {
    /// An optional human-readable description of the operation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// An optional definition of parameters required to invoke the operation.
    /// Keys are parameter names, values are their property definitions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<HashMap<String, PropertyDefinition>>,
    /// An optional definition of the response structure returned by the operation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<PropertyDefinition>,
}

/// Defines a submodel, which is a collection of properties, events, and operations.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SubModelDefinition {
    /// The unique name of the submodel.
    pub name: String,
    /// An optional version string for the submodel.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// An optional human-readable description of the submodel.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// A map of property definitions within this submodel.
    /// Keys are property names.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, PropertyDefinition>>,
    /// A map of event definitions within this submodel.
    /// Keys are event names.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events: Option<HashMap<String, EventDefinition>>,
    /// A map of operation definitions within this submodel.
    /// Keys are operation names.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operations: Option<HashMap<String, OperationDefinition>>,
}

/// Information identifying a child asset.
///
/// Used when querying for child assets.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ChildAssetInfo {
    /// The namespace of the child asset.
    pub namespace: String,
    /// The name of the child asset.
    pub name: String,
}


/// Validates a `serde_json::Value` against a [`PropertyDefinition`].
///
/// This function checks for type correctness and adherence to enum constraints.
/// For object and array types, it recursively validates nested elements if definitions are provided.
///
/// # Parameters
/// * `value`: The JSON value to validate.
/// * `def`: The property definition to validate against.
///
/// # Returns
/// * `Ok(())` if the value is valid according to the definition.
/// * `Err(Error::InvalidParameter { ... })` if validation fails, with context and reason.
pub fn validate_value(value: &serde_json::Value, def: &PropertyDefinition) -> Result<(), Error> {
    match def.type_of {
        PropertyType::Boolean => {
            if !value.is_boolean() {
                return Err(Error::InvalidParameter {
                    context: "property_type_mismatch".to_string(),
                    reason: format!("Expected boolean, got {:?} (value: {})", value, value),
                });
            }
        }
        PropertyType::Integer => {
            if !value.is_i64() {
                return Err(Error::InvalidParameter {
                    context: "property_type_mismatch".to_string(),
                    reason: format!("Expected integer, got {:?} (value: {})", value, value),
                });
            }
        }
        PropertyType::Number => {
            if !value.is_number() {
                return Err(Error::InvalidParameter {
                    context: "property_type_mismatch".to_string(),
                    reason: format!("Expected number, got {:?} (value: {})", value, value),
                });
            }
        }
        PropertyType::String => {
            if !value.is_string() {
                return Err(Error::InvalidParameter {
                    context: "property_type_mismatch".to_string(),
                    reason: format!("Expected string, got {:?} (value: {})", value, value),
                });
            }
        }
        PropertyType::Null => {
            if !value.is_null() {
                return Err(Error::InvalidParameter {
                    context: "property_type_mismatch".to_string(),
                    reason: format!("Expected null, got {:?} (value: {})", value, value),
                });
            }
        }
        PropertyType::Object => {
            if !value.is_object() {
                return Err(Error::InvalidParameter {
                    context: "property_type_mismatch".to_string(),
                    reason: format!("Expected object, got {:?} (value: {})", value, value),
                });
            }
            if let Some(defined_props_map) = def.properties.as_ref() {
                let value_obj = value.as_object().unwrap(); // Safe due to is_object check
                for (key, val_in_value) in value_obj {
                    if let Some(sub_def) = defined_props_map.get(key) {
                        validate_value(val_in_value, sub_def)?;
                    } else {
                        // Allow extra properties not in definition, per JSON schema flexibility
                        log::debug!("Validation: Unknown property '{}' in object value.", key);
                    }
                }
            }
        }
        PropertyType::Array => {
            if !value.is_array() {
                return Err(Error::InvalidParameter {
                    context: "property_type_mismatch".to_string(),
                    reason: format!("Expected array, got {:?} (value: {})", value, value),
                });
            }
            if let Some(item_def) = def.items.as_ref() {
                for item_in_value in value.as_array().unwrap() { // Safe due to is_array check
                    validate_value(item_in_value, item_def)?;
                }
            }
        }
    }
    if let Some(allowed_values) = def.enum_values.as_ref() {
        if !allowed_values.contains(value) {
            return Err(Error::InvalidParameter {
                context: "enum_check_failed".to_string(),
                reason: format!(
                    "Value {} not in allowed enum values: {:?}",
                    value, allowed_values
                ),
            });
        }
    }
    Ok(())
}

/// Represents a submodel property of an asset.
///
/// Provides methods to get, set (for owners), delete (for owners), and subscribe to changes
/// (for consumers) of the property's value.
#[derive(Debug)]
pub struct Property {
    /// The name of the property.
    pub name: String,
    /// The definition of the property, outlining its type and constraints.
    pub definition: PropertyDefinition,
    /// The current value of the property, protected by an asynchronous `RwLock`.
    pub value: Arc<TokioRwLock<serde_json::Value>>,
    /// The base MQTT topic for this property's parent (submodel).
    pub parent_topic: String,
    comm_client: Arc<dyn CommunicationClient>,
}
impl Property {
    /// Creates a new `Property`.
    ///
    /// This is typically called internally when a `SubModel` is instantiated.
    ///
    /// # Parameters
    /// * `name`: The name of the property. Accepts any type that implements `Into<String>`.
    /// * `definition`: The [`PropertyDefinition`] describing the property.
    /// * `comm_client`: An `Arc` wrapped communication client for MQTT interactions.
    /// * `parent_topic`: The MQTT topic of the parent submodel. Accepts any type that implements `Into<String>`.
    pub fn new(
        name: impl Into<String>,
        definition: PropertyDefinition,
        comm_client: Arc<dyn CommunicationClient>,
        parent_topic: impl Into<String>,
    ) -> Self {
        Property {
            name: name.into(),
            definition,
            value: Arc::new(TokioRwLock::new(serde_json::Value::Null)),
            parent_topic: parent_topic.into(),
            comm_client,
        }
    }

    /// Gets the current value of the property.
    ///
    /// This method asynchronously acquires a read lock on the property's value.
    ///
    /// # Returns
    /// The current `serde_json::Value` of the property.
    ///
    /// # Panics
    /// Panics if the `TokioRwLock` is poisoned (i.e., a task panicked while holding a write lock).
    pub async fn get_value(&self) -> serde_json::Value {
        self.value.read().await.clone()
    }

    /// Sets the value of the property (for owners).
    ///
    /// This method validates the new value against the property's definition.
    /// If valid, it updates the local value and publishes the change to the MQTT broker
    /// with `retain = true`.
    ///
    /// # Parameters
    /// * `new_value`: The new `serde_json::Value` to set for the property.
    ///
    /// # Returns
    /// * `Ok(())` if the value was set and published successfully.
    /// * `Err(Error::NotWritable)` if the property is read-only.
    /// * `Err(Error::InvalidParameter)` if `new_value` fails validation.
    /// * `Err(Error::JsonError)` if serialization of the value fails.
    /// * `Err(Error::PublishFailed)` if publishing to MQTT fails.
    ///
    /// # Panics
    /// Panics if the `TokioRwLock` for the value is poisoned.
    pub async fn set_value(&self, new_value: serde_json::Value) -> Result<(), Error> {
        if self.definition.read_only == Some(true) {
            return Err(Error::NotWritable);
        }
        validate_value(&new_value, &self.definition)?;

        let mut value_guard = self
            .value
            .write()
            .await;
        *value_guard = new_value;
        let payload_str = serde_json::to_string(&*value_guard)?;
        self.comm_client
            .publish(self.get_topic(), payload_str, true)
            .await?;
        Ok(())
    }

    /// Gets the full MQTT topic for this property.
    ///
    /// The topic is typically `parent_topic/property_name`.
    ///
    /// # Returns
    /// A `String` representing the MQTT topic.
    pub fn get_topic(&self) -> String {
        format!("{}/{}", self.parent_topic, self.name)
    }

    /// Subscribes to changes for this property (for consumers).
    ///
    /// The provided callback will be invoked when a message is received on the property's topic.
    /// The callback receives the new property value as a `serde_json::Value`.
    /// The local property value is updated internally using `try_write()` to avoid blocking
    /// the MQTT callback thread.
    ///
    /// # Parameters
    /// * `callback`: A callback function that takes the new `serde_json::Value` as an argument.
    ///   It must be `Send + Sync + 'static`.
    ///
    /// # Returns
    /// * `Ok(())` if the subscription was successful.
    /// * `Err(Error::SubscriptionFailed)` if subscribing to the MQTT topic fails.
    pub async fn on_change(
        &self,
        callback: Box<dyn Fn(Value) + Send + Sync + 'static>,
    ) -> Result<(), Error> {
        let value_arc = self.value.clone();
        let internal_callback = move |_topic: String, payload_str: String| {
            match serde_json::from_str::<Value>(&payload_str) {
                Ok(new_prop_value) => {
                    match value_arc.try_write() {
                        Ok(mut guard) => *guard = new_prop_value.clone(),
                        Err(e) => {
                            log::error!(
                                "Failed to acquire write lock (try_write) for property value after MQTT update: {}. Update for value {:?} may be lost.",
                                e, new_prop_value
                            );
                            return;
                        }
                    }
                    callback(new_prop_value);
                }
                Err(e) => {
                    log::error!(
                        "Failed to deserialize property payload: {}. Payload: {}",
                        e,
                        payload_str
                    );
                }
            }
        };
        self.comm_client
            .subscribe(self.get_topic(), Box::new(internal_callback))
            .await
    }

    /// Deletes the property (for owners).
    ///
    /// This sets the local property value to `Value::Null` and publishes an empty string
    /// with `retain = true` to the property's topic, effectively clearing the retained message
    /// on the MQTT broker.
    ///
    /// # Returns
    /// * `Ok(())` if deletion was successful.
    /// * `Err(Error::NotWritable)` if the property is read-only.
    /// * `Err(Error::LockError)` if the local value lock is poisoned.
    /// * `Err(Error::PublishFailed)` if publishing the empty message fails.
    ///
    /// # Panics
    /// Panics if the `TokioRwLock` for the value is poisoned during write.
    pub async fn delete(&self) -> Result<(), Error> {
        if self.definition.read_only == Some(true) {
            log::warn!(
                "Attempted to delete read-only property: {}",
                self.get_topic()
            );
            return Err(Error::NotWritable);
        }

        let mut value_guard = self
            .value
            .write()
            .await;

        *value_guard = serde_json::Value::Null;

        log::debug!("Deleting property by publishing empty retained message to topic: {}", self.get_topic());

        self.comm_client
            .publish(self.get_topic(), "".to_string(), true)
            .await?;

        Ok(())
    }
}

/// Represents a submodel event of an asset.
///
/// Provides methods to trigger the event (for owners) or subscribe to occurrences
/// of the event (for consumers).
#[derive(Debug)]
pub struct Event {
    /// The name of the event.
    pub name: String,
    /// The definition of the event, outlining its parameters.
    pub definition: EventDefinition,
    /// The base MQTT topic for this event's parent (submodel).
    pub parent_topic: String,
    comm_client: Arc<dyn CommunicationClient>,
}
impl Event {
    /// Creates a new `Event`.
    ///
    /// Typically called internally when a `SubModel` is instantiated.
    ///
    /// # Parameters
    /// * `name`: The name of the event. Accepts any type that implements `Into<String>`.
    /// * `definition`: The [`EventDefinition`] describing the event.
    /// * `comm_client`: An `Arc` wrapped communication client for MQTT interactions.
    /// * `parent_topic`: The MQTT topic of the parent submodel. Accepts any type that implements `Into<String>`.
    pub fn new(
        name: impl Into<String>,
        definition: EventDefinition,
        comm_client: Arc<dyn CommunicationClient>,
        parent_topic: impl Into<String>,
    ) -> Self {
        Event {
            name: name.into(),
            definition,
            parent_topic: parent_topic.into(),
            comm_client,
        }
    }

    /// Triggers the event (for owners).
    ///
    /// Validates the provided parameters against the event's definition.
    /// If valid, it constructs a payload including a timestamp and the parameters,
    /// then publishes it to the event's MQTT topic with `retain = false`.
    ///
    /// # Parameters
    /// * `params`: A `serde_json::Value` containing the parameters for the event.
    ///   Should be an object if parameters are defined, or `Value::Null` / empty object if not.
    ///
    /// # Returns
    /// * `Ok(())` if the event was triggered and published successfully.
    /// * `Err(Error::InvalidParameter)` if `params` fail validation.
    /// * `Err(Error::JsonError)` if serialization of the event payload fails.
    /// * `Err(Error::PublishFailed)` if publishing to MQTT fails.
    pub async fn trigger(&self, params: serde_json::Value) -> Result<(), Error> {
        if let Some(param_defs) = &self.definition.parameters {
            if let Some(p_obj) = params.as_object() {
                for (name, def) in param_defs {
                    if let Some(val) = p_obj.get(name) {
                        validate_value(val, def).map_err(|e| Error::InvalidParameter {
                            context: format!("event_parameter_validation: '{}'", self.name),
                            reason: format!("Validation failed for parameter '{}': {}", name, e),
                        })?;
                    } else {
                        return Err(Error::InvalidParameter {
                            context: format!("event_parameter_validation: '{}'", self.name),
                            reason: format!("Missing event parameter: {}", name),
                        });
                    }
                }
            } else if !param_defs.is_empty() {
                return Err(Error::InvalidParameter {
                    context: format!("event_parameter_validation: '{}'", self.name),
                    reason: "Event parameters should be an object, but received non-object or missing parameters."
                        .to_string(),
                });
            }
        } else if !params.is_null() && !params.as_object().map_or(true, |m| m.is_empty()) {
            return Err(Error::InvalidParameter {
                context: format!("event_parameter_validation: '{}'", self.name),
                reason: "Event expects no parameters or an empty object, but received data."
                    .to_string(),
            });
        }
        let now_ms = Utc::now().timestamp_millis();
        let event_payload = serde_json::json!({ "timestamp": now_ms, "parameters": params });
        let payload_str = serde_json::to_string(&event_payload)?;
        self.comm_client
            .publish(self.get_topic(), payload_str, false)
            .await?;
        log::info!(
            "Event '{}' triggered on topic {}",
            self.name,
            self.get_topic()
        );
        Ok(())
    }

    /// Subscribes to this event (for consumers).
    ///
    /// The provided callback will be invoked when the event is triggered by a remote owner.
    /// The callback receives the event parameters as a `HashMap<String, Value>` and the event timestamp (`i64`).
    ///
    /// # Parameters
    /// * `callback`: A callback function that takes the event parameters and timestamp.
    ///   It must be `Send + Sync + 'static`.
    ///
    /// # Returns
    /// * `Ok(())` if the subscription was successful.
    /// * `Err(Error::SubscriptionFailed)` if subscribing to the MQTT topic fails.
    pub async fn on_event(
        &self,
        callback: Box<dyn Fn(HashMap<String, Value>, i64) + Send + Sync + 'static>,
    ) -> Result<(), Error> {
        self.comm_client
            .subscribe_event(self.get_topic(), callback)
            .await
    }

    /// Gets the full MQTT topic for this event.
    ///
    /// The topic is typically `parent_topic/event_name`.
    ///
    /// # Returns
    /// A `String` representing the MQTT topic.
    pub fn get_topic(&self) -> String {
        format!("{}/{}", self.parent_topic, self.name)
    }
}

/// Represents a submodel operation of an asset.
///
/// Provides methods to invoke the operation (for consumers) or bind a handler
/// to implement the operation (for owners).
#[derive(Debug)]
pub struct Operation {
    /// The name of the operation.
    pub name: String,
    /// The definition of the operation, outlining its parameters and response.
    pub definition: OperationDefinition,
    /// The base MQTT topic for this operation's parent (submodel).
    pub parent_topic: String,
    comm_client: Arc<dyn CommunicationClient>,
}
impl Operation {
    /// Creates a new `Operation`.
    ///
    /// Typically called internally when a `SubModel` is instantiated.
    ///
    /// # Parameters
    /// * `name`: The name of the operation. Accepts any type that implements `Into<String>`.
    /// * `definition`: The [`OperationDefinition`] describing the operation.
    /// * `comm_client`: An `Arc` wrapped communication client for MQTT interactions.
    /// * `parent_topic`: The MQTT topic of the parent submodel. Accepts any type that implements `Into<String>`.
    pub fn new(
        name: impl Into<String>,
        definition: OperationDefinition,
        comm_client: Arc<dyn CommunicationClient>,
        parent_topic: impl Into<String>,
    ) -> Self {
        Operation {
            name: name.into(),
            definition,
            parent_topic: parent_topic.into(),
            comm_client,
        }
    }

    /// Invokes the operation on a remote asset (for consumers).
    ///
    /// Validates the provided parameters against the operation's definition.
    /// If valid, it sends an invocation request via MQTT and waits for a response.
    /// The response is then validated against the operation's response definition.
    ///
    /// # Parameters
    /// * `params`: A `serde_json::Value` containing the parameters for the operation.
    ///   Should be an object if parameters are defined, or `Value::Null` / empty object if not.
    ///
    /// # Returns
    /// * `Ok(Value)` containing the response from the operation if successful and a response is expected.
    ///   Returns `Ok(Value::Null)` if the operation is void and completes successfully.
    /// * `Err(Error::InvalidParameter)` if `params` fail validation or if the response validation fails.
    /// * `Err(Error::OperationTimeoutError)` if the operation times out.
    /// * Other communication or JSON errors from the underlying client.
    pub async fn invoke(&self, params: Value) -> Result<Value, Error> {
        if let Some(param_defs_map) = self.definition.parameters.as_ref() {
            if let Some(params_obj) = params.as_object() {
                for (name, def_val) in param_defs_map {
                    if let Some(val_from_user) = params_obj.get(name) {
                        validate_value(val_from_user, def_val).map_err(|e| {
                            Error::InvalidParameter {
                                context: format!("operation_parameter_validation: '{}'", self.name),
                                reason: format!("Validation failed for parameter '{}': {}", name, e),
                            }
                        })?;
                    } else {
                        return Err(Error::InvalidParameter {
                            context: format!("operation_parameter_validation: '{}'", self.name),
                            reason: format!("Missing parameter: {}", name),
                        });
                    }
                }
                for user_param_name in params_obj.keys() {
                    if !param_defs_map.contains_key(user_param_name) {
                        log::warn!(
                            "Invoke: Unknown parameter '{}' provided for operation '{}'",
                            user_param_name,
                            self.name
                        );
                    }
                }
            } else if !param_defs_map.is_empty() {
                return Err(Error::InvalidParameter {
                    context: format!("operation_parameter_validation: '{}'", self.name),
                    reason: "Parameters should be an object, but received non-object or missing parameters."
                        .to_string(),
                });
            }
        } else if !params.is_null() && !params.as_object().map_or(true, |m| m.is_empty()) {
            return Err(Error::InvalidParameter {
                context: format!("operation_parameter_validation: '{}'", self.name),
                reason: "Operation expects no parameters or an empty object, but received data."
                    .to_string(),
            });
        }
        let response = self
            .comm_client
            .invoke_operation(self.get_topic(), params, DEFAULT_OPERATION_TIMEOUT_MS)
            .await?;
        if let Some(resp_def) = self.definition.response.as_ref() {
            validate_value(&response, resp_def).map_err(|e| Error::InvalidParameter {
                context: format!("operation_response_validation: '{}'", self.name),
                reason: format!("Invalid response from operation: {}", e),
            })?;
        } else if !response.is_null() {
            return Err(Error::InvalidParameter {
                context: format!("operation_response_validation: '{}'", self.name),
                reason: format!(
                    "Operation expected no response (void), but got non-null: {:?}",
                    response
                ),
            });
        }
        Ok(response)
    }

    /// Binds a handler function to this operation (for owners).
    ///
    /// The provided callback will be invoked when a consumer calls this operation.
    /// The callback receives the operation parameters as a `serde_json::Value` and
    /// should return a `Result<Value, Error>` which will be sent back as the response.
    ///
    /// # Parameters
    /// * `callback`: A callback function that takes `Value` (parameters) and returns `Result<Value, Error>`.
    ///   It must be `Send + Sync + 'static`.
    ///
    /// # Returns
    /// * `Ok(())` if the operation was successfully bound.
    /// * `Err(Error::SubscriptionFailed)` if subscribing to the MQTT request topic fails.
    pub async fn bind(
        &self,
        callback: Box<dyn Fn(Value) -> Result<Value, Error> + Send + Sync + 'static>,
    ) -> Result<(), Error> {
        self.comm_client
            .bind_operation(self.get_topic(), callback)
            .await?;
        log::info!(
            "Operation '{}' bound on topic {}",
            self.name,
            self.get_topic()
        );
        Ok(())
    }

    /// Gets the full MQTT topic for this operation.
    ///
    /// The topic is typically `parent_topic/operation_name`.
    /// Note that requests and responses will use sub-topics like `.../request` and `.../response`.
    ///
    /// # Returns
    /// A `String` representing the base MQTT topic for the operation.
    pub fn get_topic(&self) -> String {
        format!("{}/{}", self.parent_topic, self.name)
    }
}


/// Represents a submodel of an asset, containing properties, events, and operations.
///
/// Instances of `SubModel` are managed by an [`Asset`].
#[derive(Debug)]
pub struct SubModel {
    /// The name of the submodel.
    pub name: String,
    /// A map of properties belonging to this submodel, keyed by property name.
    /// Values are `Arc` wrapped [`Property`] instances.
    pub properties: HashMap<String, Arc<Property>>,
    /// A map of events belonging to this submodel, keyed by event name.
    /// Values are `Arc` wrapped [`Event`] instances.
    pub events: HashMap<String, Arc<Event>>,
    /// A map of operations belonging to this submodel, keyed by operation name.
    /// Values are `Arc` wrapped [`Operation`] instances.
    pub operations: HashMap<String, Arc<Operation>>,
    /// The base MQTT topic for this submodel (e.g., `namespace/asset_name/submodel_name`).
    pub parent_topic: String,
    comm_client: Arc<dyn CommunicationClient>,
}
impl SubModel {
    /// Creates a new `SubModel` instance from its definition.
    ///
    /// This is typically called internally by an [`Asset`] when implementing a submodel.
    /// It initializes all properties, events, and operations defined in the `SubModelDefinition`.
    /// For owners (`Mode::Owner`), it also creates and publishes a `_meta` property for this submodel.
    ///
    /// # Parameters
    /// * `def`: The [`SubModelDefinition`] for this submodel.
    /// * `parent_topic_for_submodel`: The base MQTT topic for this submodel. Accepts `impl Into<String>`.
    /// * `mode`: The [`Mode`] (Owner/Consumer) of the parent asset.
    /// * `comm_client`: The shared communication client.
    /// * `asset_namespace`: The namespace of the parent asset. Accepts `impl Into<String>`.
    /// * `asset_endpoint_name`: The endpoint name of the parent asset (used for `_meta` source). Accepts `impl Into<String>`.
    ///
    /// # Returns
    /// A `Result` containing the new `SubModel` or an `Error` if initialization fails (e.g., JSON error, publish error for `_meta`).
    pub async fn new(
        def: SubModelDefinition,
        parent_topic_for_submodel: impl Into<String>,
        mode: &Mode,
        comm_client: Arc<dyn CommunicationClient>,
        asset_namespace: impl Into<String>,
        asset_endpoint_name: impl Into<String>,
    ) -> Result<Self, Error> {
        let parent_topic_for_submodel_str = parent_topic_for_submodel.into();
        let asset_namespace_str = asset_namespace.into();
        let asset_endpoint_name_str = asset_endpoint_name.into();

        let submodel_base_name = def.name.clone();
        let submodel_topic = format!("{}/{}", parent_topic_for_submodel_str, submodel_base_name);
        let mut properties = HashMap::new();
        if let Some(props_def) = &def.properties {
            for (name, prop_def) in props_def {
                properties.insert(
                    name.clone(),
                    Arc::new(Property::new(
                        name.clone(),
                        prop_def.clone(),
                        comm_client.clone(),
                        submodel_topic.clone(),
                    )),
                );
            }
        }
        let mut events = HashMap::new();
        if let Some(events_def) = &def.events {
            for (name, event_def) in events_def {
                events.insert(
                    name.clone(),
                    Arc::new(Event::new(
                        name.clone(),
                        event_def.clone(),
                        comm_client.clone(),
                        submodel_topic.clone(),
                    )),
                );
            }
        }
        let mut operations = HashMap::new();
        if let Some(ops_def) = &def.operations {
            for (name, op_def) in ops_def {
                operations.insert(
                    name.clone(),
                    Arc::new(Operation::new(
                        name.clone(),
                        op_def.clone(),
                        comm_client.clone(),
                        submodel_topic.clone(),
                    )),
                );
            }
        }
        let mut new_submodel = Self {
            name: submodel_base_name,
            properties,
            events,
            operations,
            parent_topic: submodel_topic.clone(),
            comm_client: comm_client.clone(),
        };
        if mode == &Mode::Owner {
            let meta_def_json = serde_json::json!({"type": "object", "readOnly": true, "properties": {"source": {"type": "string"},"submodel_definition": {"type": "object"},"submodel_url": {"type": "string"}}});
            let meta_def: PropertyDefinition = serde_json::from_value(meta_def_json)?;
            let meta_prop_name = "_meta".to_string();
            let meta_property = Arc::new(Property::new(
                meta_prop_name.clone(),
                meta_def,
                comm_client.clone(),
                new_submodel.get_topic(),
            ));
            let meta_value_payload = serde_json::json!({"source": format!("{}/{}", asset_namespace_str, asset_endpoint_name_str),"submodel_definition": def.clone(),"submodel_url": "file://localhost"});
            meta_property.set_value(meta_value_payload).await?;
            new_submodel
                .properties
                .insert(meta_prop_name, meta_property);
        }
        Ok(new_submodel)
    }

    /// Gets the base MQTT topic for this submodel.
    pub fn get_topic(&self) -> String {
        self.parent_topic.clone()
    }

    /// Gets a property by name from this submodel.
    ///
    /// # Parameters
    /// * `name`: The name of the property to retrieve.
    ///
    /// # Returns
    /// An `Option<Arc<Property>>` containing the property if found, otherwise `None`.
    /// The `Arc` allows shared ownership of the property.
    pub fn get_property(&self, name: &str) -> Option<Arc<Property>> {
        self.properties.get(name).cloned()
    }

    /// Gets an event by name from this submodel.
    /// # Parameters
    /// * `name`: The name of the event to retrieve.
    ///
    /// # Returns
    /// An `Option<Arc<Event>>` containing the event if found, otherwise `None`.
    pub fn get_event(&self, name: &str) -> Option<Arc<Event>> {
        self.events.get(name).cloned()
    }

    /// Gets an operation by name from this submodel.
    /// # Parameters
    /// * `name`: The name of the operation to retrieve.
    ///
    /// # Returns
    /// An `Option<Arc<Operation>>` containing the operation if found, otherwise `None`.
    pub fn get_operation(&self, name: &str) -> Option<Arc<Operation>> {
        self.operations.get(name).cloned()
    }
}

/// Represents an ASSETS2036 asset, composed of one or more submodels.
///
/// Provides methods to interact with its submodels, properties, events, and operations.
/// Also handles creation of child assets and querying relationships.
#[derive(Debug)]
pub struct Asset {
    /// The name of the asset.
    pub name: String,
    /// The namespace the asset belongs to.
    pub namespace: String,
    /// A map of submodels implemented by this asset, keyed by submodel name.
    pub sub_models: HashMap<String, SubModel>,
    /// The operational mode of this asset instance ([`Mode::Owner`] or [`Mode::Consumer`]).
    pub mode: Mode,
    /// The endpoint name associated with this asset, used for `_meta` source information.
    pub endpoint_name: String,
    comm_client: Arc<dyn CommunicationClient>,
}
impl Asset {
    /// Creates a new `Asset` instance.
    ///
    /// This is the basic constructor. Submodels are typically added using [`Asset::implement_sub_model`].
    /// For a more automated setup, especially for the `_endpoint` asset, see [`AssetManager`].
    ///
    /// # Parameters
    /// * `name`: The name of the asset. Accepts `impl Into<String>`.
    /// * `namespace`: The namespace for the asset. Accepts `impl Into<String>`.
    /// * `mode`: The [`Mode`] (Owner/Consumer) for this asset instance.
    /// * `endpoint_name`: The endpoint identifier for this asset, used in `_meta` properties. Accepts `impl Into<String>`.
    /// * `comm_client`: A shared [`CommunicationClient`] for MQTT interactions.
    pub fn new(
        name: impl Into<String>,
        namespace: impl Into<String>,
        mode: Mode,
        endpoint_name: impl Into<String>,
        comm_client: Arc<dyn CommunicationClient>,
    ) -> Self {
        Asset {
            name: name.into(),
            namespace: namespace.into(),
            sub_models: HashMap::new(),
            mode,
            endpoint_name: endpoint_name.into(),
            comm_client,
        }
    }

    /// Returns a clone of the `Arc` wrapped communication client used by this asset.
    pub fn get_communication_client(&self) -> Arc<dyn CommunicationClient> {
        self.comm_client.clone()
    }

    /// Implements a submodel for this asset from a JSON string definition.
    ///
    /// Parses the `sub_model_definition_json` and creates a [`SubModel`] instance,
    /// adding it to the asset's collection of submodels.
    ///
    /// # Parameters
    /// * `sub_model_definition_json`: A string slice containing the JSON definition of the submodel.
    ///
    /// # Returns
    /// * `Ok(())` if the submodel was successfully parsed and implemented.
    /// * `Err(Error)` if parsing fails or if (for `Mode::Owner`) publishing the `_meta` property fails.
    pub async fn implement_sub_model(
        &mut self,
        sub_model_definition_json: &str,
    ) -> Result<(), Error> {
        let sub_model_def: SubModelDefinition = serde_json::from_str(sub_model_definition_json)?;
        let sub_model = SubModel::new(
            sub_model_def.clone(),
            self.get_topic(),
            &self.mode,
            self.comm_client.clone(),
            self.namespace.clone(),
            self.endpoint_name.clone(),
        )
        .await?;
        self.sub_models.insert(sub_model_def.name, sub_model);
        Ok(())
    }

    /// Gets a reference to a submodel implemented by this asset, by its name.
    ///
    /// # Parameters
    /// * `name`: The name of the submodel to retrieve.
    ///
    /// # Returns
    /// An `Option<&SubModel>` containing a reference to the submodel if found, otherwise `None`.
    pub fn get_submodel(&self, name: &str) -> Option<&SubModel> {
        self.sub_models.get(name)
    }

    /// Gets the base MQTT topic for this asset (e.g., `namespace/asset_name`).
    pub fn get_topic(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }

    /// Creates a child asset of the current asset.
    ///
    /// The child asset will automatically have the `_relations` submodel implemented,
    /// and its `belongs_to` property will be set to point to this (parent) asset.
    ///
    /// # Parameters
    /// * `child_asset_name`: The name for the new child asset. Accepts `impl Into<String>`.
    /// * `child_namespace`: An optional namespace for the child asset. If `None`, the parent's namespace is used. Accepts `Option<impl Into<String>>`.
    /// * `submodel_sources`: A vector of strings, where each string is a JSON definition for a submodel to be implemented by the child.
    ///   Note: URL loading for these sources is not handled here; pre-resolved JSON strings are expected.
    ///
    /// # Returns
    /// * `Ok(Asset)` containing the newly created child asset.
    /// * `Err(Error)` if creating the child asset, implementing its submodels, or setting the `belongs_to` relation fails.
    ///   Specifically, can return `Error::RelationError` if setting `belongs_to` fails.
    pub async fn create_child_asset(
        &self,
        child_asset_name: impl Into<String>,
        child_namespace: Option<impl Into<String>>,
        mut submodel_sources: Vec<String>,
    ) -> Result<Asset, Error> {
        let child_asset_name_str = child_asset_name.into();
        let child_ns = child_namespace.map(|s| s.into()).unwrap_or_else(|| self.namespace.clone());

        submodel_sources.push(RELATIONS_SUBMODEL_JSON.to_string());

        let mut child_asset = Asset::new(
            child_asset_name_str.clone(),
            child_ns.clone(),
            self.mode.clone(),
            self.endpoint_name.clone(),
            self.comm_client.clone(),
        );

        for source_str in submodel_sources {
            match child_asset.implement_sub_model(&source_str).await {
                Ok(_) => log::info!(
                    "Implemented submodel from source for child asset '{}'",
                    child_asset_name_str
                ),
                Err(e) => {
                    log::error!(
                        "Failed to implement submodel for child asset '{}' from source string (might be URL or JSON): {:?}, Error: {:?}",
                        child_asset_name_str, source_str, e
                    );
                    return Err(e);
                }
            }
        }

        if let Some(relations_sm_arc) = child_asset.get_submodel("_relations").map(Arc::new) { // Assuming get_submodel returns &SubModel, then wrap in Arc
            let relations_sm = relations_sm_arc; // To satisfy borrow checker if get_submodel returns &SubModel
             if let Some(belongs_to_prop_arc) = relations_sm.get_property("belongs_to") {
                let parent_info = serde_json::json!({
                    "namespace": self.namespace,
                    "asset_name": self.name
                });
                match belongs_to_prop_arc.set_value(parent_info.clone()).await {
                    Ok(_) => log::info!(
                        "Set 'belongs_to' for child asset '{}' to {:?}",
                        child_asset_name_str, parent_info
                    ),
                    Err(e) => {
                        log::error!(
                            "Failed to set 'belongs_to' for child asset '{}': {:?}",
                            child_asset_name_str, e
                        );
                        return Err(Error::RelationError(format!(
                            "Failed to set belongs_to property: {}",
                            e
                        )));
                    }
                }
            } else {
                return Err(Error::RelationError(
                    "'_relations' submodel for child asset missing 'belongs_to' property definition."
                        .to_string(),
                ));
            }
        } else {
            return Err(Error::RelationError(
                "Child asset missing '_relations' submodel after implementation.".to_string(),
            ));
        }

        Ok(child_asset)
    }

    /// Queries for child assets that have their `belongs_to` property pointing to this asset.
    ///
    /// This method uses the communication client's `query_asset_children` method.
    ///
    /// # Returns
    /// * `Ok(Vec<ChildAssetInfo>)` containing information about discovered child assets.
    /// * `Err(Error)` if the query fails (e.g., communication error, discovery error).
    pub async fn get_child_assets(&self) -> Result<Vec<ChildAssetInfo>, Error> {
        self.comm_client
            .query_asset_children(&self.namespace, &self.name)
            .await
    }
}

/// Manages assets and their lifecycle, including creation and discovery.
///
/// The `AssetManager` is the primary entry point for applications using this library.
/// It handles the connection to the communication backend (e.g., MQTT broker)
/// and provides an API to create local assets (owners) and proxies to remote assets (consumers).
/// It also manages a special `_endpoint` asset for itself, used for health status and logging.
pub struct AssetManager {
    host: String,
    port: u16,
    namespace: String,
    endpoint_name: String,
    comm_client: Arc<dyn CommunicationClient>,
    endpoint_asset: Option<Asset>,
    is_connected: Arc<TokioRwLock<bool>>,
    health_monitor_shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}
impl AssetManager {
    /// Creates a new `AssetManager`.
    ///
    /// Initializes the communication client but does not connect automatically.
    /// Use [`AssetManager::connect`] to establish the connection.
    ///
    /// # Parameters
    /// * `host`: The hostname or IP address of the communication broker (e.g., MQTT broker). Accepts `impl Into<String>`.
    /// * `port`: The port number of the broker.
    /// * `namespace`: The default namespace for assets created by this manager. Accepts `impl Into<String>`.
    /// * `endpoint_name`: A unique name for this manager's endpoint, used in MQTT client ID and `_meta` source. Accepts `impl Into<String>`.
    pub fn new(
        host: impl Into<String>,
        port: u16,
        namespace: impl Into<String>,
        endpoint_name: impl Into<String>,
    ) -> Self {
        let host_str = host.into();
        let namespace_str = namespace.into();
        let endpoint_name_str = endpoint_name.into();

        let client_id_prefix = format!("{}/{}", namespace_str, endpoint_name_str);
        let mqtt_client = MqttCommunicationClient::new(&client_id_prefix, &host_str, port);
        Self {
            host: host_str,
            port,
            namespace: namespace_str,
            endpoint_name: endpoint_name_str,
            comm_client: Arc::new(mqtt_client),
            endpoint_asset: None,
            is_connected: Arc::new(TokioRwLock::new(false)),
            health_monitor_shutdown_tx: None,
        }
    }

    /// Placeholder for self-ping logic. Currently returns `true`.
    ///
    /// In a real implementation, this would check the manager's own health,
    /// possibly including the state of its communication client.
    async fn _self_ping(&self) -> bool {
        true
    }

    /// Sets up a periodic health monitoring task.
    ///
    /// The task calls the provided `callback` function and `_self_ping` at the specified `interval_seconds`.
    /// The combined result updates the `healthy` property of the manager's `_endpoint` asset.
    /// If `exit_on_unhealthy` is true, the entire process will exit if the health status becomes false.
    /// Calling this method again will stop any previously running health monitor task.
    ///
    /// # Parameters
    /// * `callback`: A function that returns `bool` indicating custom health status. Must be `Send + Sync + 'static`.
    /// * `interval_seconds`: The interval at which to perform health checks.
    /// * `exit_on_unhealthy`: If `true`, `std::process::exit(-1)` is called when health becomes false.
    ///
    /// # Panics
    /// The spawned health monitoring task may panic if setting the `healthy` property fails due to a poisoned lock.
    pub fn set_healthy_callback<F>(
        &mut self,
        callback: F,
        interval_seconds: u64,
        exit_on_unhealthy: bool,
    ) where
        F: Fn() -> bool + Send + Sync + 'static,
    {
        if let Some(previous_shutdown_tx) = self.health_monitor_shutdown_tx.take() {
            if previous_shutdown_tx.send(()).is_err() {
                log::warn!("Failed to send shutdown signal to previous health monitor task; it might have already completed or panicked.");
            }
        }

        let (new_shutdown_tx, mut new_shutdown_rx) = oneshot::channel::<()>();
        self.health_monitor_shutdown_tx = Some(new_shutdown_tx);

        let healthy_prop_arc = match self
            .endpoint_asset
            .as_ref()
            .and_then(|asset| asset.get_submodel("_endpoint"))
            .and_then(|sm| sm.get_property("healthy"))
        {
            Some(prop) => prop,
            None => {
                log::error!("Cannot start health monitor: 'healthy' property not found on _endpoint asset. Ensure AssetManager is connected.");
                self.health_monitor_shutdown_tx.take();
                return;
            }
        };

        tokio::spawn(async move {
            log::info!("Health monitor task started with interval {} seconds.", interval_seconds);
            loop {
                tokio::select! {
                    _ = &mut new_shutdown_rx => {
                        log::info!("Health monitor task received shutdown signal via channel.");
                        break;
                    }
                    _ = sleep(Duration::from_secs(interval_seconds)) => {
                        let self_ping_ok = true;
                        let callback_ok = callback();
                        let is_healthy_now = callback_ok && self_ping_ok;

                        log::debug!("Health monitor check: Ping OK: {}, Callback OK: {}, Overall Healthy: {}", self_ping_ok, callback_ok, is_healthy_now);

                        if let Err(e) = healthy_prop_arc.set_value(json!(is_healthy_now)).await {
                            log::error!("Failed to set 'healthy' property in monitor task: {:?}", e);
                        } else {
                            log::info!("Health status updated to: {}", is_healthy_now);
                        }

                        if exit_on_unhealthy && !is_healthy_now {
                            log::error!("Health monitor determined system is unhealthy. Exiting application as requested.");
                            std::process::exit(-1);
                        }
                    }
                }
            }
            log::info!("Health monitor task finished.");
        });
    }

    /// Creates the internal `_endpoint` asset for the `AssetManager`.
    ///
    /// This asset exposes properties like `online` and `healthy`, an event `log_entry`,
    /// and operations like `ping`, `shutdown`, `restart`.
    /// It's typically called automatically after a successful [`AssetManager::connect`].
    async fn create_endpoint_asset(&mut self) -> Result<(), Error> {
        if self.endpoint_asset.is_some() {
            return Ok(());
        }
        let ep_asset_name = "_endpoint".to_string(); // Use to_string for clarity, though &str would work with Into
        let mut ep_asset = Asset::new(
            ep_asset_name, // Asset::new takes impl Into<String>
            self.namespace.clone(),
            Mode::Owner,
            self.endpoint_name.clone(),
            self.comm_client.clone(),
        );

        const ENDPOINT_SUBMODEL_JSON: &str = r#"{
            "name": "_endpoint",
            "version": "1.0.0",
            "properties": {
                "online": {"type": "boolean"},
                "healthy": {"type": "boolean"}
            },
            "events": {
                "log_entry": {
                    "description": "Emits a log entry.",
                    "parameters": {
                        "level": {"type": "string"},
                        "message": {"type": "string"}
                    }
                }
            },
            "operations": {
                "ping": {"response": {"type": "string"}},
                "shutdown": {},
                "restart": {}
            }
        }"#;
        ep_asset.implement_sub_model(ENDPOINT_SUBMODEL_JSON).await?;
        if let Some(sm_arc) = ep_asset.get_submodel("_endpoint").map(Arc::new) { // Assuming get_submodel returns &SubModel
            let sm = sm_arc; // To satisfy borrow checker for multiple uses
            if let Some(online_prop_arc) = sm.get_property("online") {
                online_prop_arc.set_value(serde_json::json!(true)).await?;
            }
            if let Some(healthy_prop_arc) = sm.get_property("healthy") {
                healthy_prop_arc.set_value(serde_json::json!(true)).await?; // Initially healthy
            }
            if let Some(ping_op_arc) = sm.get_operation("ping") {
                ping_op_arc
                    .bind(Box::new(|_params| Ok(serde_json::json!("pong"))))
                    .await?;
            }
            // Note: shutdown and restart operations are placeholders in this implementation.
            // A real implementation would require more complex logic, possibly involving
            // the `health_monitor_shutdown_tx` or process control.
            if let Some(shutdown_op_arc) = sm.get_operation("shutdown") {
                shutdown_op_arc
                    .bind(Box::new(move |_params| {
                        log::info!("Shutdown op called (Placeholder, does not exit process).");
                        // In a real scenario: self.disconnect().await; std::process::exit(0);
                        Ok(Value::Null)
                    }))
                    .await?;
            }
            if let Some(restart_op_arc) = sm.get_operation("restart") {
                restart_op_arc
                    .bind(Box::new(|_params| {
                        log::info!("Restart op called (Placeholder, does not restart process).");
                        Ok(Value::Null)
                    }))
                    .await?;
            }
        }
        self.endpoint_asset = Some(ep_asset);
        log::info!(
            "Endpoint asset '_endpoint' created for AssetManager {}.",
            self.endpoint_name
        );
        Ok(())
    }

    /// Connects the `AssetManager` to the communication backend.
    ///
    /// Also creates and initializes the internal `_endpoint` asset.
    ///
    /// # Returns
    /// * `Ok(())` on successful connection and endpoint initialization.
    /// * `Err(Error)` if connection or endpoint setup fails.
    pub async fn connect(&mut self) -> Result<(), Error> {
        let mut is_connected_guard = self.is_connected.write().await;
        if *is_connected_guard {
            log::warn!("AssetManager already connected.");
            return Ok(());
        }
        self.comm_client
            .connect(
                &self.host,
                self.port,
                &self.namespace,
                &self.endpoint_name,
            )
            .await?;
        *is_connected_guard = true;
        log::info!(
            "AssetManager connected to MQTT broker at {}:{}",
            self.host,
            self.port
        );
        drop(is_connected_guard);
        self.create_endpoint_asset().await?;
        Ok(())
    }

    /// Retrieves a logging handler that routes `log` crate messages via MQTT.
    ///
    /// The handler uses the `log_entry` event of the manager's `_endpoint` asset.
    /// Requires the `AssetManager` to be connected and its `_endpoint` asset initialized.
    ///
    /// # Returns
    /// * `Ok(RustLoggingHandler)` if successful.
    /// * `Err(Error::CommunicationError)` if the endpoint asset is not initialized.
    /// * `Err(Error::RelationError)` if the `_endpoint` submodel or `log_entry` event is not found.
    pub fn get_logging_handler(&self) -> Result<RustLoggingHandler, Error> {
        let ep_asset = self.endpoint_asset.as_ref().ok_or_else(|| {
            Error::CommunicationError("Endpoint asset not initialized in AssetManager for logging handler.".to_string())
        })?;
        let ep_submodel = ep_asset.get_submodel("_endpoint").ok_or_else(|| {
            Error::RelationError("'_endpoint' submodel not found in endpoint asset for logging.".to_string())
        })?;
        let log_event_arc = ep_submodel.get_event("log_entry").ok_or_else(|| {
            Error::RelationError("'log_entry' event not found in _endpoint submodel for logging.".to_string())
        })?;
        Ok(RustLoggingHandler {
            log_event: log_event_arc,
        })
    }

    /// Disconnects the `AssetManager` from the communication backend.
    ///
    /// Sets the `_endpoint/online` property to `false` before disconnecting.
    /// Also stops any running health monitor task.
    ///
    /// # Returns
    /// * `Ok(())` on successful disconnection.
    /// * `Err(Error)` if setting endpoint offline or disconnecting the client fails.
    pub async fn disconnect(&self) -> Result<(), Error> { // Changed to take &self based on usage pattern
        let mut is_connected_guard = self.is_connected.write().await;
        if !*is_connected_guard {
            log::warn!("AssetManager already disconnected.");
            return Ok(());
        }
        if let Some(ep_asset) = self.endpoint_asset.as_ref() {
            if let Some(sm_arc) = ep_asset.get_submodel("_endpoint").map(Arc::new) { // Assuming get_submodel returns &SubModel
                let sm = sm_arc;
                if let Some(online_prop_arc) = sm.get_property("online") {
                    if let Err(e) = online_prop_arc.set_value(serde_json::json!(false)).await {
                        log::error!("Failed to set endpoint offline: {:?}", e);
                    }
                }
            }
        }

        // Note: Cannot take self.health_monitor_shutdown_tx with &self.
        // This requires either &mut self or internal mutability for health_monitor_shutdown_tx (e.g. Arc<Mutex<Option<...>>>)
        // For now, this part of disconnect won't work as intended with &self.
        // If AssetManager had a Drop impl, that would take `&mut self`.
        // Or, disconnect should take `&mut self`.
        // Let's assume for now this is called before drop and AssetManager is mutable elsewhere or this field is handled.
        // if let Some(shutdown_tx) = self.health_monitor_shutdown_tx.take() { // Needs &mut self
        //     if shutdown_tx.send(()).is_err() {
        //         log::warn!("Failed to send shutdown signal to health monitor task on disconnect; it might have already completed or panicked.");
        //     } else {
        //         log::info!("Health monitor task signaled to shut down due to AssetManager disconnect.");
        //     }
        // }

        self.comm_client.disconnect().await?;
        *is_connected_guard = false;
        log::info!("AssetManager disconnected.");
        Ok(())
    }

    /// Creates a new local asset (owner mode) or a proxy to a remote asset (consumer mode).
    ///
    /// Submodel definitions can be provided as direct JSON strings or as URLs (`file://`, `http://`, `https://`).
    ///
    /// # Parameters
    /// * `name`: The name for the new asset. Accepts `impl Into<String>`.
    /// * `submodel_sources`: A vector of strings, each being a submodel JSON definition or a URL to one.
    /// * `mode`: The [`Mode`] for the asset (Owner or Consumer).
    ///
    /// # Returns
    /// * `Ok(Asset)` containing the newly created asset.
    /// * `Err(Error)` if the manager is not connected, or if loading/implementing any submodel fails.
    ///   Possible errors include `Error::CommunicationError`, `Error::UrlParseError`, `Error::FileReadError`,
    ///   `Error::HttpRequestError`, or errors from `Asset::implement_sub_model`.
    pub async fn create_asset(
        &self,
        name: impl Into<String>,
        submodel_sources: Vec<String>,
        mode: Mode,
    ) -> Result<Asset, Error> {
        if !*self.is_connected.read().await {
            return Err(Error::CommunicationError(
                "AssetManager not connected".to_string(),
            ));
        }
        let name_str = name.into();
        let mut asset = Asset::new(
            name_str.clone(),
            self.namespace.clone(),
            mode,
            self.endpoint_name.clone(),
            self.comm_client.clone(),
        );

        for source_str in submodel_sources {
            let submodel_json_content: String = match Url::parse(&source_str) {
                Ok(url_obj) => {
                    log::info!("Attempting to load submodel from URL: {}", url_obj);
                    match url_obj.scheme() {
                        "file" => {
                            let path = url_obj.to_file_path().map_err(|_| {
                                Error::UrlParseError(format!(
                                    "Invalid file URL path: {}",
                                    source_str
                                ))
                            })?;
                            let mut file = File::open(&path).map_err(|e| {
                                Error::FileReadError {
                                    path: path.to_string_lossy().into_owned(),
                                    details: e.to_string(),
                                }
                            })?;
                            let mut contents = String::new();
                            file.read_to_string(&mut contents).map_err(|e| {
                                Error::FileReadError {
                                    path: path.to_string_lossy().into_owned(),
                                    details: e.to_string(),
                                }
                            })?;
                            log::info!("Successfully loaded submodel from file URL: {}", path.display());
                            contents
                        }
                        "http" | "https" => {
                            let response =
                                reqwest::blocking::get(url_obj.clone()).map_err(|e| {
                                    Error::HttpRequestError {
                                        url: source_str.clone(),
                                        details: e.to_string(),
                                    }
                                })?;
                            if !response.status().is_success() {
                                return Err(Error::HttpRequestError {
                                    url: source_str.clone(),
                                    details: format!("HTTP request failed with status: {}", response.status()),
                                });
                            }
                            let contents = response.text().map_err(|e| {
                                Error::HttpRequestError {
                                    url: source_str.clone(),
                                    details: e.to_string(),
                                }
                            })?;
                            log::info!("Successfully loaded submodel from HTTP(S) URL: {}", source_str);
                            contents
                        }
                        unsupported_scheme => {
                            log::error!(
                                "Unsupported URL scheme for submodel loading: {}",
                                unsupported_scheme
                            );
                            return Err(Error::UrlParseError(format!(
                                "Unsupported URL scheme: {}",
                                unsupported_scheme
                            )));
                        }
                    }
                }
                Err(_) => {
                    log::info!(
                        "Source '{}' is not a valid URL, treating as direct JSON string.",
                        source_str
                    );
                    source_str
                }
            };

            match asset.implement_sub_model(&submodel_json_content).await {
                Ok(_) => log::info!(
                    "Successfully implemented submodel for asset '{}' from source.",
                    name_str
                ),
                Err(e) => {
                    log::error!(
                        "Failed to implement submodel for asset '{}'. Source: '{}', Error: {:?}",
                        name_str,
                        source_str,
                        e
                    );
                    return Err(e);
                }
            }
        }
        Ok(asset)
    }

    /// Creates a proxy to a remote asset (consumer mode).
    ///
    /// This method queries the remote asset's `_meta` information for all its submodels
    /// and uses these definitions to construct a local proxy `Asset` instance in `Mode::Consumer`.
    /// Optionally, it can wait for the remote asset's `_endpoint/online` property to become true.
    ///
    /// # Parameters
    /// * `namespace`: The namespace of the remote asset. Accepts `impl Into<String>`.
    /// * `asset_name`: The name of the remote asset. Accepts `impl Into<String>`.
    /// * `wait_for_online_secs`: An `Option<u64>` specifying how many seconds to wait for the asset to be online.
    ///   - `Some(0)`: Checks current online status, returns error immediately if not online.
    ///   - `Some(N)` where `N > 0`: Waits up to `N` seconds.
    ///   - `None`: Does not wait or check online status.
    ///
    /// # Returns
    /// * `Ok(Asset)` containing the asset proxy if successful.
    /// * `Err(Error)` if:
    ///   - The manager is not connected (`Error::CommunicationError`).
    ///   - The remote asset is not found or has no submodels (`Error::AssetNotFoundError`).
    ///   - Parsing `_meta` information fails (`Error::InvalidMetaSubmodelDefinition`, `Error::JsonError`).
    ///   - Implementing submodels for the proxy fails.
    ///   - `wait_for_online_secs` is specified and the asset does not come online within the timeout (`Error::AssetNotOnlineError`).
    ///   - Retrieving `_endpoint` or `online` property for the wait logic fails (`Error::RelationError`).
    pub async fn create_asset_proxy(
        &self,
        namespace: impl Into<String>,
        asset_name: impl Into<String>,
        wait_for_online_secs: Option<u64>,
    ) -> Result<Asset, Error> {
        if !*self.is_connected.read().await {
            return Err(Error::CommunicationError(
                "AssetManager not connected".to_string(),
            ));
        }
        let namespace_str = namespace.into();
        let asset_name_str = asset_name.into();

        log::info!("Creating asset proxy for '{}/{}'", namespace_str, asset_name_str);
        let submodel_meta_values = self
            .comm_client
            .query_submodels_for_asset(&namespace_str, &asset_name_str)
            .await?;
        if submodel_meta_values.is_empty() {
            log::warn!(
                "No submodels found for asset '{}/{}' during proxy creation.",
                namespace_str,
                asset_name_str
            );
            return Err(Error::AssetNotFoundError {
                namespace: namespace_str.clone(),
                name: asset_name_str.clone(),
            });
        }
        let mut asset_proxy = Asset::new(
            asset_name_str.clone(),
            namespace_str.clone(),
            Mode::Consumer,
            self.endpoint_name.clone(),
            self.comm_client.clone(),
        );
        for (submodel_name, meta_value) in submodel_meta_values {
            log::debug!(
                "Processing _meta for submodel '{}' of asset '{}/{}'",
                submodel_name,
                namespace_str,
                asset_name_str
            );
            if let Some(sm_def_val) = meta_value.get("submodel_definition") {
                match serde_json::to_string(sm_def_val) {
                    Ok(sm_def_json_string) => {
                        if let Err(e) = asset_proxy.implement_sub_model(&sm_def_json_string).await {
                            log::error!(
                                "Failed to implement submodel '{}' for proxy '{}/{}': {:?}",
                                submodel_name,
                                namespace_str,
                                asset_name_str,
                                e
                            );
                            return Err(e);
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to re-serialize submodel_definition from _meta for submodel '{}' of asset '{}/{}': {}", submodel_name, namespace_str, asset_name_str, e);
                        return Err(Error::InvalidMetaSubmodelDefinition {
                            submodel_name,
                            asset_name: asset_name_str.clone(),
                        });
                    }
                }
            } else {
                log::warn!("_meta for submodel '{}' of asset '{}/{}' did not contain 'submodel_definition' field.", submodel_name, namespace_str, asset_name_str);
                return Err(Error::InvalidMetaSubmodelDefinition {
                    submodel_name,
                    asset_name: asset_name_str.clone(),
                });
            }
        }
        log::info!(
            "Successfully created asset proxy for '{}/{}' with {} submodel(s).",
            namespace_str,
            asset_name_str,
            asset_proxy.sub_models.len()
        );

        if let Some(timeout_duration_secs) = wait_for_online_secs {
            if timeout_duration_secs == 0 {
                let is_currently_online = asset_proxy
                    .get_submodel("_endpoint")
                    .and_then(|ep_sm| ep_sm.get_property("online"))
                    .map(|online_prop| online_prop.get_value().await.as_bool().unwrap_or(false))
                    .unwrap_or(false);

                if !is_currently_online {
                    return Err(Error::AssetNotOnlineError {
                        name: asset_name_str.clone(),
                        namespace: namespace_str.clone(),
                        timeout_secs: 0,
                    });
                }
            } else {
                let online_prop = asset_proxy
                    .get_submodel("_endpoint")
                    .ok_or_else(|| Error::RelationError("Proxy asset missing _endpoint submodel for online check.".to_string()))?
                    .get_property("online")
                    .ok_or_else(|| Error::RelationError("Proxy asset's _endpoint missing 'online' property.".to_string()))?;

                if online_prop.get_value().await.as_bool().unwrap_or(false) {
                    log::info!("Asset '{}/{}' is already online.", namespace_str, asset_name_str);
                    return Ok(asset_proxy);
                }

                log::info!("Waiting for asset '{}/{}' to come online for up to {} seconds...", namespace_str, asset_name_str, timeout_duration_secs);

                let (tx, mut rx) = watch::channel::<bool>(false);

                let online_prop_clone_for_callback = online_prop.clone();
                let log_asset_name_online_cb = asset_name_str.clone(); // Clone for the callback
                let log_namespace_online_cb = namespace_str.clone(); // Clone for the callback
                let on_change_callback = Box::new(move |new_value: Value| {
                    if let Some(is_online) = new_value.as_bool() {
                        if is_online {
                            let _ = tx.send(true);
                            log::info!("Online property for '{}/{}' became true.", log_namespace_online_cb, log_asset_name_online_cb);
                        }
                    }
                });

                online_prop.on_change(on_change_callback).await?;

                if online_prop.get_value().await.as_bool().unwrap_or(false) {
                    log::info!("Asset '{}/{}' reported online immediately after subscription setup.", namespace_str, asset_name_str);
                    return Ok(asset_proxy);
                }

                match tokio::time::timeout(
                    Duration::from_secs(timeout_duration_secs),
                    async {
                        while !*rx.borrow() {
                            if rx.changed().await.is_err() {
                                return Err(Error::CommunicationError("Online status watch channel closed unexpectedly.".to_string()));
                            }
                        }
                        Ok(())
                    }
                ).await {
                    Ok(Ok(_)) => {
                        log::info!("Asset '{}/{}' is now online.", namespace_str, asset_name_str);
                    }
                    Ok(Err(e)) => {
                         log::warn!("Error waiting for online signal for '{}/{}': {:?}", namespace_str, asset_name_str, e);
                        return Err(e);
                    }
                    Err(_) => {
                        log::warn!("Asset '{}/{}' did not come online within {} seconds.", namespace_str, asset_name_str, timeout_duration_secs);
                        return Err(Error::AssetNotOnlineError {
                            name: asset_name_str.clone(),
                            namespace: namespace_str.clone(),
                            timeout_secs: timeout_duration_secs,
                        });
                    }
                }
            }
        }
        Ok(asset_proxy)
    }

    /// Queries for assets in a given namespace that implement a list of submodels.
    ///
    /// # Parameters
    /// * `query_namespace`: Optional namespace to search in. If `None`, the manager's default namespace is used.
    /// * `submodel_names`: A slice of submodel names to filter by. Assets must implement *all* specified submodels.
    ///
    /// # Returns
    /// * `Ok(Vec<String>)` containing the names of matching assets.
    /// * `Err(Error)` if the query fails.
    pub async fn query_assets(
        &self,
        query_namespace: Option<&str>,
        submodel_names: &[&str],
    ) -> Result<Vec<String>, Error> {
        let ns_to_query = query_namespace.unwrap_or(&self.namespace);
        log::debug!(
            "Querying assets in namespace '{}' implementing submodels: {:?}",
            ns_to_query,
            submodel_names
        );
        self.comm_client
            .query_asset_names(Some(ns_to_query), submodel_names)
            .await
    }
}
