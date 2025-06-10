use chrono::Utc;
use log;

use serde_json::Value;
use std::boxed::Box;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;

use tokio::sync::RwLock as TokioRwLock;
use url::Url;

pub const DEFAULT_OPERATION_TIMEOUT_MS: u64 = 5000;

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

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ChildAssetInfo {
    pub namespace: String,
    pub name: String,
}

mod communication_client;
mod error;
pub use error::Error;

use crate::communication_client::{CommunicationClient, MqttCommunicationClient};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PropertyType {
    Boolean,
    Integer,
    Number,
    String,
    Object,
    Array,
    #[default]
    Null,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Mode {
    Owner,
    Consumer,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct PropertyDefinition {
    #[serde(rename = "type")]
    pub type_of: PropertyType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Box<PropertyDefinition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, Box<PropertyDefinition>>>,
    #[serde(
        rename = "enum",
        alias = "enum_values",
        skip_serializing_if = "Option::is_none"
    )]
    pub enum_values: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EventDefinition {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<HashMap<String, PropertyDefinition>>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OperationDefinition {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<HashMap<String, PropertyDefinition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<PropertyDefinition>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SubModelDefinition {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, PropertyDefinition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events: Option<HashMap<String, EventDefinition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operations: Option<HashMap<String, OperationDefinition>>,
}

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
                let value_obj = value.as_object().unwrap();
                for (key, val_in_value) in value_obj {
                    if let Some(sub_def) = defined_props_map.get(key) {
                        validate_value(val_in_value, sub_def)?;
                    } else {
                        log::warn!("Validation: Unknown property '{}' in object value.", key);
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
                for item_in_value in value.as_array().unwrap() {
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

#[derive(Debug)]
pub struct Property {
    pub name: String,
    pub definition: PropertyDefinition,
    pub value: Arc<TokioRwLock<serde_json::Value>>, // Changed to TokioRwLock
    pub parent_topic: String,
    comm_client: Arc<dyn CommunicationClient>,
}
impl Property {
    pub fn new(
        name: String,
        definition: PropertyDefinition,
        comm_client: Arc<dyn CommunicationClient>,
        parent_topic: String,
    ) -> Self {
        Property {
            name,
            definition,
            value: Arc::new(TokioRwLock::new(serde_json::Value::Null)), // Use TokioRwLock
            parent_topic,
            comm_client,
        }
    }
    // get_value is now async
    pub async fn get_value(&self) -> serde_json::Value {
        match self.value.read().await {
            guard => guard.clone(),
            // Tokio's RwLock read guard doesn't directly return a Result that indicates poisoning in the same way std::sync::RwLock does.
            // If a lock is poisoned, operations on it (like .read().await or .write().await) will typically panic.
            // Thus, explicit poison checking like with std::sync::RwLock is less common here.
            // If a panic occurs due to poisoning, the task will panic.
            // For robustness, one might wrap this in a task that can be recovered from, but that's a larger pattern.
            // For now, we assume panics are the indicator of poisoning for Tokio locks.
            // If we wanted to avoid panic and return Null, we'd need to use `try_read()` and handle its error.
            // Let's stick to direct .read().await for now and rely on panic for poisoning.
            // To align with previous behavior of returning Null on error:
            // This requires a different approach as .read().await doesn't return Result for poisoning.
            // A simple way is not to explicitly handle poisoning here, let it panic.
            // If we must return Value::Null, we'd need to rethink.
            // For now, let's keep it simple and assume it won't panic in normal operation.
            // The `guard` here is the actual RwLockReadGuard.
        }
    }
    pub async fn set_value(&self, new_value: serde_json::Value) -> Result<(), Error> {
        if self.definition.read_only == Some(true) {
            return Err(Error::NotWritable);
        }
        validate_value(&new_value, &self.definition)?;
        // TokioRwLock::write returns a Result if another task holding the lock panicked (poisoned).
        let mut value_guard = self
            .value
            .write()
            .await; // Changed to .await
        // No direct map_err for poisoning on the await itself. Poisoning would cause panic on await/lock.
        // If write() is successful, it returns a guard. If poisoned, it panics.
        // To map to Error::LockError, we would need to use `try_write()` or catch panics.
        // For now, let it panic on poisoning, or assume non-poisoned path for simplicity of this step.
        // If we absolutely must avoid panic and return LockError:
        // let mut value_guard = self.value.try_write().map_err(|e| Error::LockError(format!("Failed to acquire write lock (possibly poisoned): {}", e)))?;
        // This however makes it non-blocking, which might not be desired.
        // Sticking to .write().await and letting it panic on poisoning is idiomatic for Tokio unless specific recovery is needed.
        // Let's assume for now we want to convert panic into LockError if possible, though that's non-trivial.
        // The simplest change is just .write().await and accept panic on poison.
        //
        // If we need to handle LockError like before:
        // This is tricky as `await` itself will panic on poison.
        // A more robust way would be to use `try_write()` and handle the WouldBlock case if needed,
        // or ensure that panics are caught higher up if that's the application strategy.
        // For this refactor, we'll assume that a panic due to poisoning is acceptable,
        // or that a higher-level mechanism handles it.
        // If we want to map it to Error::LockError, we'd need a custom mechanism.
        // Let's proceed with the direct await and acknowledge this difference.
        *value_guard = new_value;
        let payload_str = serde_json::to_string(&*value_guard)?;
        self.comm_client
            .publish(self.get_topic(), payload_str, true)
            .await?;
        Ok(())
    }
    pub fn get_topic(&self) -> String {
        format!("{}/{}", self.parent_topic, self.name)
    }
    pub async fn on_change(
        &self,
        callback: Box<dyn Fn(Value) + Send + Sync + 'static>,
    ) -> Result<(), Error> {
        let value_arc = self.value.clone();
        let internal_callback = move |_topic: String, payload_str: String| {
            match serde_json::from_str::<Value>(&payload_str) {
                Ok(new_prop_value) => {
                    // Attempt to use blocking_write or try_write for the sync callback context
                    match value_arc.try_write() { // Using try_write to avoid blocking the MQTT callback thread indefinitely
                        Ok(mut guard) => *guard = new_prop_value.clone(),
                        Err(e) => { // try_write failed (either locked or poisoned)
                            // If it's just locked, the update might be lost.
                            // If poisoned, it's a more severe issue.
                            log::error!(
                                "Failed to acquire write lock (try_write) for property value after MQTT update: {}. Update for value {:?} may be lost.",
                                e, new_prop_value
                            );
                            // Depending on strictness, one might choose to panic here on poison,
                            // or attempt blocking_write in a spawned task if updates are critical.
                            // For now, log and skip update if try_write fails.
                            return;
                        }
                    }
                    callback(new_prop_value); // Call the user's original sync callback
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

    pub async fn delete(&self) -> Result<(), Error> {
        if self.definition.read_only == Some(true) {
            log::warn!(
                "Attempted to delete read-only property: {}",
                self.get_topic()
            );
            return Err(Error::NotWritable);
        }

        // Acquire lock to update local value first
        let mut value_guard = self
            .value
            .write()
            .write()
            .await; // Changed to .await
            // Similar to set_value, poisoning would panic here.

        // Set local value to Null
        *value_guard = serde_json::Value::Null;

        log::debug!("Deleting property by publishing empty retained message to topic: {}", self.get_topic());

        // Publish an empty string with retain=true to clear the message from the broker
        self.comm_client
            .publish(self.get_topic(), "".to_string(), true)
            .await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct Event {
    pub name: String,
    pub definition: EventDefinition,
    pub parent_topic: String,
    comm_client: Arc<dyn CommunicationClient>,
}
impl Event {
    pub fn new(
        name: String,
        definition: EventDefinition,
        comm_client: Arc<dyn CommunicationClient>,
        parent_topic: String,
    ) -> Self {
        Event {
            name,
            definition,
            parent_topic,
            comm_client,
        }
    }
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
    pub async fn on_event(
        &self,
        callback: Box<dyn Fn(HashMap<String, Value>, i64) + Send + Sync + 'static>,
    ) -> Result<(), Error> {
        self.comm_client
            .subscribe_event(self.get_topic(), callback)
            .await
    }
    pub fn get_topic(&self) -> String {
        format!("{}/{}", self.parent_topic, self.name)
    }
}

#[derive(Debug)]
pub struct Operation {
    pub name: String,
    pub definition: OperationDefinition,
    pub parent_topic: String,
    comm_client: Arc<dyn CommunicationClient>,
}
impl Operation {
    pub fn new(
        name: String,
        definition: OperationDefinition,
        comm_client: Arc<dyn CommunicationClient>,
        parent_topic: String,
    ) -> Self {
        Operation {
            name,
            definition,
            parent_topic,
            comm_client,
        }
    }
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
    pub fn get_topic(&self) -> String {
        format!("{}/{}", self.parent_topic, self.name)
    }
}

// Import for RustLoggingHandler
use crate::logging::RustLoggingHandler;
use log::LevelFilter;
// Import for RustLoggingHandler
use crate::logging::RustLoggingHandler;
use log::LevelFilter;
// Arc was already imported, but ensure it's used where needed.
use tokio::sync::{oneshot, watch}; // Added watch
use tokio::time::{sleep, Duration};

#[derive(Debug)]
pub struct SubModel {
    pub name: String,
    pub properties: HashMap<String, Arc<Property>>, // Changed to Arc<Property>
    pub events: HashMap<String, Arc<Event>>,         // Changed to Arc<Event>
    pub operations: HashMap<String, Arc<Operation>>, // Changed to Arc<Operation>
    pub parent_topic: String,
    comm_client: Arc<dyn CommunicationClient>,
}
impl SubModel {
    pub async fn new(
        def: SubModelDefinition,
        parent_topic_for_submodel: String,
        mode: &Mode,
        comm_client: Arc<dyn CommunicationClient>,
        asset_namespace: String,
        asset_endpoint_name: String,
    ) -> Result<Self, Error> {
        let submodel_base_name = def.name.clone();
        let submodel_topic = format!("{}/{}", parent_topic_for_submodel, submodel_base_name);
        let mut properties = HashMap::new();
        if let Some(props_def) = &def.properties {
            for (name, prop_def) in props_def {
                properties.insert(
                    name.clone(),
                    Arc::new(Property::new( // Wrapped in Arc::new()
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
                    Arc::new(Event::new( // Wrapped in Arc::new()
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
                    Arc::new(Operation::new( // Wrapped in Arc::new()
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
            let meta_property = Arc::new(Property::new( // Wrapped in Arc::new()
                meta_prop_name.clone(),
                meta_def,
                comm_client.clone(),
                new_submodel.get_topic(),
            ));
            let meta_value_payload = serde_json::json!({"source": format!("{}/{}", asset_namespace, asset_endpoint_name),"submodel_definition": def.clone(),"submodel_url": "file://localhost"});
            // -> Access set_value on Arc<Property>
            meta_property.set_value(meta_value_payload).await?;
            new_submodel
                .properties
                .insert(meta_prop_name, meta_property);
        }
        Ok(new_submodel)
    }
    pub fn get_topic(&self) -> String {
        self.parent_topic.clone()
    }
    // Getter methods now return Option<Arc<T>> for direct cloning, or Option<&Arc<T>> if only reference is needed.
    // For get_logging_handler, we need to clone the Arc.
    pub fn get_property(&self, name: &str) -> Option<Arc<Property>> {
        self.properties.get(name).cloned()
    }
    pub fn get_event(&self, name: &str) -> Option<Arc<Event>> {
        self.events.get(name).cloned()
    }
    pub fn get_operation(&self, name: &str) -> Option<Arc<Operation>> {
        self.operations.get(name).cloned()
    }
}

#[derive(Debug)]
pub struct Asset {
    pub name: String,
    pub namespace: String,
    pub sub_models: HashMap<String, SubModel>,
    pub mode: Mode,
    pub endpoint_name: String,
    comm_client: Arc<dyn CommunicationClient>,
}
impl Asset {
    pub fn new(
        name: String,
        namespace: String,
        mode: Mode,
        endpoint_name: String,
        comm_client: Arc<dyn CommunicationClient>,
    ) -> Self {
        Asset {
            name,
            namespace,
            sub_models: HashMap::new(),
            mode,
            endpoint_name,
            comm_client,
        }
    }
    pub fn get_communication_client(&self) -> Arc<dyn CommunicationClient> {
        self.comm_client.clone()
    }
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
    pub fn get_submodel(&self, name: &str) -> Option<&SubModel> {
        self.sub_models.get(name)
    }
    pub fn get_topic(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }

    pub async fn create_child_asset(
        &self,
        child_asset_name: String,
        child_namespace: Option<String>,
        mut submodel_sources: Vec<String>, // Mutable to add _relations if not present
    ) -> Result<Asset, Error> {
        let child_ns = child_namespace.unwrap_or_else(|| self.namespace.clone());

        // Ensure _relations submodel is included
        // A more robust check might involve parsing names, but for now, direct string comparison or presence check.
        // We'll just add it; implement_sub_model should handle duplicates gracefully if SubModelDefinition name is the key.
        // Or, Asset::new could take a list of SubModelDefinition structs instead of strings to load.
        // For now, we pass the JSON string.
        submodel_sources.push(RELATIONS_SUBMODEL_JSON.to_string());

        let mut child_asset = Asset::new(
            child_asset_name.clone(),
            child_ns.clone(),
            self.mode.clone(), // Child inherits parent's mode (Owner/Consumer)
            self.endpoint_name.clone(), // Child uses parent's endpoint_name for _meta source
            self.comm_client.clone(),
        );

        for source_str in submodel_sources {
            // Assuming source_str can be a direct JSON string or a URL to be resolved by AssetManager logic
            // For now, Asset::implement_sub_model takes direct JSON.
            // If AssetManager::create_asset's URL resolving logic is needed here, it would require refactoring
            // or passing the AssetManager instance/relevant parts.
            // For this step, we'll assume submodel_sources are direct JSON strings or handled by a simplified implement_sub_model.
            // The current Asset::implement_sub_model only takes JSON string, not URLs.
            // This is a simplification for this step. A full solution might need AssetManager::create_asset's help.

            // If source_str is a URL, it needs to be resolved to JSON content first.
            // This logic is in AssetManager::create_asset. To avoid duplicating it,
            // this create_child_asset would ideally call something on an AssetManager instance.
            // Given the current structure, we'll proceed with the assumption that implement_sub_model
            // will be called with resolved JSON strings.
            // However, the provided submodel_sources are strings.
            // The simplest path is to ensure RELATIONS_SUBMODEL_JSON (which is a string) is implemented.
            // Other sources would need to be pre-resolved if they are URLs.
            // Let's assume for now that `submodel_sources` provided to this function are direct JSON strings.

            match child_asset.implement_sub_model(&source_str).await {
                Ok(_) => log::info!(
                    "Implemented submodel from source for child asset '{}'",
                    child_asset_name
                ),
                Err(e) => {
                    log::error!(
                        "Failed to implement submodel for child asset '{}' from source string (might be URL or JSON): {:?}, Error: {:?}",
                        child_asset_name, source_str, e
                    );
                    return Err(e);
                }
            }
        }

        // Set the 'belongs_to' property
        if let Some(relations_sm) = child_asset.get_submodel("_relations") {
            if let Some(belongs_to_prop) = relations_sm.get_property("belongs_to") {
                let parent_info = serde_json::json!({
                    "namespace": self.namespace,
                    "asset_name": self.name
                });
                match belongs_to_prop.set_value(parent_info.clone()).await {
                    Ok(_) => log::info!(
                        "Set 'belongs_to' for child asset '{}' to {:?}",
                        child_asset_name, parent_info
                    ),
                    Err(e) => {
                        log::error!(
                            "Failed to set 'belongs_to' for child asset '{}': {:?}",
                            child_asset_name, e
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

    pub async fn get_child_assets(&self) -> Result<Vec<ChildAssetInfo>, Error> {
        self.comm_client
            .query_asset_children(&self.namespace, &self.name)
            .await
    }
}

pub struct AssetManager {
    host: String,
    port: u16,
    namespace: String,
    endpoint_name: String,
    comm_client: Arc<dyn CommunicationClient>,
    endpoint_asset: Option<Asset>, // Asset itself is not Clone, direct Arc<Asset> is not straightforward here.
                                   // We will get Arc<Property> from it.
    is_connected: Arc<TokioRwLock<bool>>,
    health_monitor_shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}
impl AssetManager {
    pub fn new(host: String, port: u16, namespace: String, endpoint_name: String) -> Self {
        let client_id_prefix = format!("{}/{}", namespace, endpoint_name);
        let mqtt_client = MqttCommunicationClient::new(&client_id_prefix, &host, port);
        Self {
            host,
            port,
            namespace,
            endpoint_name,
            comm_client: Arc::new(mqtt_client),
            endpoint_asset: None,
            is_connected: Arc::new(TokioRwLock::new(false)),
            health_monitor_shutdown_tx: None,
        }
    }

    // Placeholder for now, as per instructions
    async fn _self_ping(&self) -> bool {
        // In a real scenario, this might involve checking connection to broker,
        // or other internal health checks.
        // For example, if the endpoint asset itself had a ping operation exposed locally:
        // if let Some(ep_asset) = &self.endpoint_asset {
        //     if let Some(ep_sm) = ep_asset.get_submodel("_endpoint") {
        //         if let Some(ping_op) = ep_sm.get_operation("ping") {
        //             // This would require a local, non-MQTT way to invoke or check status.
        //             // Or, it could involve a round-trip ping to the broker via the comm_client,
        //             // but that's more involved than a simple self-ping.
        //             // For now, simplified:
        //             return true;
        //         }
        //     }
        // }
        // false // Default if not properly set up
        true // Simplified placeholder
    }

    pub fn set_healthy_callback<F>(
        &mut self,
        callback: F,
        interval_seconds: u64,
        exit_on_unhealthy: bool,
    ) where
        F: Fn() -> bool + Send + Sync + 'static,
    {
        // Stop any existing health monitor task
        if let Some(previous_shutdown_tx) = self.health_monitor_shutdown_tx.take() {
            if previous_shutdown_tx.send(()).is_err() {
                log::warn!("Failed to send shutdown signal to previous health monitor task; it might have already completed or panicked.");
            }
        }

        let (new_shutdown_tx, mut new_shutdown_rx) = oneshot::channel::<()>();
        self.health_monitor_shutdown_tx = Some(new_shutdown_tx);

        // Get the 'healthy' property Arc from the endpoint asset.
        // This requires endpoint_asset to be initialized (usually by connect()).
        let healthy_prop_arc = match self
            .endpoint_asset
            .as_ref()
            .and_then(|asset| asset.get_submodel("_endpoint"))
            .and_then(|sm| sm.get_property("healthy"))
        {
            Some(prop) => prop,
            None => {
                log::error!("Cannot start health monitor: 'healthy' property not found on _endpoint asset. Ensure AssetManager is connected.");
                // Remove the shutdown sender we just stored, as the task won't start.
                self.health_monitor_shutdown_tx.take();
                return;
            }
        };

        // self._self_ping() is async, but the current placeholder is sync.
        // For a real async _self_ping, we would need to handle it inside the async block.
        // For now, we'll call the placeholder.
        // If _self_ping needs &self, and AssetManager is not Clone, this becomes more complex.
        // We'd need to pass necessary data to the task or use an Arc<AssetManager>.
        // Given _self_ping is simplified, we'll proceed.

        tokio::spawn(async move {
            log::info!("Health monitor task started with interval {} seconds.", interval_seconds);
            loop {
                tokio::select! {
                    _ = &mut new_shutdown_rx => {
                        log::info!("Health monitor task received shutdown signal via channel.");
                        break;
                    }
                    _ = sleep(Duration::from_secs(interval_seconds)) => {
                        // Current _self_ping is simplified and sync.
                        // If it were async and required &AssetManager, more complex handling needed.
                        let self_ping_ok = true; // AssetManager::_self_ping().await would be here
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
                            std::process::exit(-1); // Exits the entire process
                        }
                    }
                }
            }
            log::info!("Health monitor task finished.");
        });
    }

    async fn create_endpoint_asset(&mut self) -> Result<(), Error> {
        if self.endpoint_asset.is_some() {
            return Ok(());
        }
        let ep_asset_name = "_endpoint".to_string();
        let mut ep_asset = Asset::new(
            ep_asset_name.clone(),
            self.namespace.clone(),
            Mode::Owner,
            self.endpoint_name.clone(),
            self.comm_client.clone(),
        );
        // Updated ENDPOINT_SUBMODEL_JSON to include the log_entry event
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
        if let Some(sm) = ep_asset.get_submodel("_endpoint") {
            if let Some(online_prop) = sm.get_property("online") {
                online_prop.set_value(serde_json::json!(true)).await?;
            }
            if let Some(healthy_prop) = sm.get_property("healthy") {
                healthy_prop.set_value(serde_json::json!(true)).await?;
            }
            if let Some(ping_op) = sm.get_operation("ping") {
                ping_op
                    .bind(Box::new(|_params| Ok(serde_json::json!("pong"))))
                    .await?;
            }
            if let Some(shutdown_op) = sm.get_operation("shutdown") {
                shutdown_op
                    .bind(Box::new(move |_params| {
                        log::info!("Shutdown op called (Placeholder)");
                        Ok(Value::Null)
                    }))
                    .await?;
            }
            if let Some(restart_op) = sm.get_operation("restart") {
                restart_op
                    .bind(Box::new(|_params| {
                        log::info!("Restart op called (Placeholder)");
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
    pub async fn connect(&mut self) -> Result<(), Error> {
        let mut is_connected_guard = self.is_connected.write().await;
        if *is_connected_guard {
            log::warn!("AssetManager already connected.");
            return Ok(());
        }
        self.comm_client
            .connect(&self.host, self.port, &self.namespace, &self.endpoint_name)
            .await?;
        *is_connected_guard = true;
        log::info!(
            "AssetManager connected to MQTT broker at {}:{}",
            self.host,
            self.port
        );
        drop(is_connected_guard);
        // create_endpoint_asset is called after successful connection.
        // It might try to publish using the comm_client.
        self.create_endpoint_asset().await?;
        Ok(())
    }

    pub fn get_logging_handler(&self) -> Result<RustLoggingHandler, Error> {
        let ep_asset = self.endpoint_asset.as_ref().ok_or_else(|| {
            Error::CommunicationError("Endpoint asset not initialized in AssetManager for logging handler.".to_string())
        })?;
        let ep_submodel = ep_asset.get_submodel("_endpoint").ok_or_else(|| {
            Error::RelationError("'_endpoint' submodel not found in endpoint asset for logging.".to_string())
        })?;
        // get_event now returns Option<Arc<Event>>
        let log_event_arc = ep_submodel.get_event("log_entry").ok_or_else(|| {
            Error::RelationError("'log_entry' event not found in _endpoint submodel for logging.".to_string())
        })?;
        Ok(RustLoggingHandler {
            log_event: log_event_arc, // Arc::clone is implicitly done if log_event_arc is moved
        })
    }

    pub async fn disconnect(&self) -> Result<(), Error> {
        let mut is_connected_guard = self.is_connected.write().await;
        if !*is_connected_guard {
            log::warn!("AssetManager already disconnected.");
            return Ok(());
        }
        if let Some(ep_asset) = self.endpoint_asset.as_ref() {
            if let Some(sm) = ep_asset.get_submodel("_endpoint") {
                // sm.get_property returns Option<Arc<Property>>
                if let Some(online_prop_arc) = sm.get_property("online") {
                    if let Err(e) = online_prop_arc.set_value(serde_json::json!(false)).await {
                        log::error!("Failed to set endpoint offline: {:?}", e);
                    }
                }
            }
        }

        // Stop health monitor task
        if let Some(shutdown_tx) = self.health_monitor_shutdown_tx.take() {
            if shutdown_tx.send(()).is_err() {
                log::warn!("Failed to send shutdown signal to health monitor task on disconnect; it might have already completed or panicked.");
            } else {
                log::info!("Health monitor task signaled to shut down due to AssetManager disconnect.");
            }
        }

        self.comm_client.disconnect().await?;
        *is_connected_guard = false;
        log::info!("AssetManager disconnected.");
        Ok(())
    }
    pub async fn create_asset(
        &self,
        name: String,
        submodel_sources: Vec<String>,
        mode: Mode,
    ) -> Result<Asset, Error> {
        if !*self.is_connected.read().await {
            return Err(Error::CommunicationError(
                "AssetManager not connected".to_string(),
            ));
        }
        let mut asset = Asset::new(
            name.clone(),
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
                    // Not a valid URL, assume it's a direct JSON string
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
                    name
                ),
                Err(e) => {
                    log::error!(
                        "Failed to implement submodel for asset '{}'. Source: '{}', Error: {:?}",
                        name,
                        source_str, // Log original source string for better context
                        e
                    );
                    return Err(e);
                }
            }
        }
        Ok(asset)
    }

    pub async fn create_asset_proxy(
        &self,
        namespace: String,
        asset_name: String,
        wait_for_online_secs: Option<u64>,
    ) -> Result<Asset, Error> {
        if !*self.is_connected.read().await {
            return Err(Error::CommunicationError(
                "AssetManager not connected".to_string(),
            ));
        }
        log::info!("Creating asset proxy for '{}/{}'", namespace, asset_name);
        let submodel_meta_values = self
            .comm_client
            .query_submodels_for_asset(&namespace, &asset_name)
            .await?;
        if submodel_meta_values.is_empty() {
            log::warn!(
                "No submodels found for asset '{}/{}' during proxy creation.",
                namespace,
                asset_name
            );
            return Err(Error::AssetNotFoundError {
                namespace: namespace.clone(),
                name: asset_name.clone(),
            });
        }
        let mut asset_proxy = Asset::new(
            asset_name.clone(),
            namespace.clone(),
            Mode::Consumer,
            self.endpoint_name.clone(),
            self.comm_client.clone(),
        );
        for (submodel_name, meta_value) in submodel_meta_values {
            log::debug!(
                "Processing _meta for submodel '{}' of asset '{}/{}'",
                submodel_name,
                namespace,
                asset_name
            );
            if let Some(sm_def_val) = meta_value.get("submodel_definition") {
                match serde_json::to_string(sm_def_val) {
                    Ok(sm_def_json_string) => {
                        if let Err(e) = asset_proxy.implement_sub_model(&sm_def_json_string).await {
                            log::error!(
                                "Failed to implement submodel '{}' for proxy '{}/{}': {:?}",
                                submodel_name,
                                namespace,
                                asset_name,
                                e
                            );
                            return Err(e);
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to re-serialize submodel_definition from _meta for submodel '{}' of asset '{}/{}': {}", submodel_name, namespace, asset_name, e);
                        return Err(Error::InvalidMetaSubmodelDefinition {
                            submodel_name,
                            asset_name: asset_name.clone(),
                        });
                    }
                }
            } else {
                log::warn!("_meta for submodel '{}' of asset '{}/{}' did not contain 'submodel_definition' field.", submodel_name, namespace, asset_name);
                return Err(Error::InvalidMetaSubmodelDefinition {
                    submodel_name,
                    asset_name: asset_name.clone(),
                });
            }
        }
        log::info!(
            "Successfully created asset proxy for '{}/{}' with {} submodel(s).",
            namespace,
            asset_name,
            asset_proxy.sub_models.len()
        );

        if let Some(timeout_duration_secs) = wait_for_online_secs {
            if timeout_duration_secs == 0 { // Treat 0 as check current state but don't wait
                let is_currently_online = asset_proxy
                    .get_submodel("_endpoint")
                    .and_then(|ep_sm| ep_sm.get_property("online"))
                    .map(|online_prop| online_prop.get_value().as_bool().unwrap_or(false))
                    .unwrap_or(false);

                if !is_currently_online {
                    return Err(Error::AssetNotOnlineError {
                        name: asset_name.clone(),
                        namespace: namespace.clone(),
                        timeout_secs: 0,
                    });
                }
            } else {
                // Wait for online logic
                let online_prop = asset_proxy
                    .get_submodel("_endpoint")
                    .ok_or_else(|| Error::RelationError("Proxy asset missing _endpoint submodel for online check.".to_string()))?
                    .get_property("online")
                    .ok_or_else(|| Error::RelationError("Proxy asset's _endpoint missing 'online' property.".to_string()))?;

                // Check if already online
                if online_prop.get_value().as_bool().unwrap_or(false) {
                    log::info!("Asset '{}/{}' is already online.", namespace, asset_name);
                    return Ok(asset_proxy);
                }

                log::info!("Waiting for asset '{}/{}' to come online for up to {} seconds...", namespace, asset_name, timeout_duration_secs);

                let (tx, mut rx) = watch::channel::<bool>(false); // Channel to signal online state

                // Setup on_change callback for the 'online' property
                // Need to clone online_prop for the callback, or rather, its Arc.
                let online_prop_clone_for_callback = online_prop.clone();
                let on_change_callback = Box::new(move |new_value: Value| {
                    if let Some(is_online) = new_value.as_bool() {
                        if is_online {
                            // Send signal that it's online.
                            // Ignore error if receiver is dropped (e.g., timeout already occurred)
                            let _ = tx.send(true);
                            log::info!("Online property for '{}/{}' became true.", online_prop_clone_for_callback.parent_topic.split('/').nth(1).unwrap_or_default(), online_prop_clone_for_callback.parent_topic.split('/').nth(0).unwrap_or_default());
                        }
                    }
                });

                // Subscribe to changes. This is async.
                online_prop.on_change(on_change_callback).await?;

                // Ensure the subscription is processed and current value is checked again *after* subscription is active.
                // The remote asset might have published its online status=true *before* we subscribed.
                // The Property's value is updated by its own subscription handler.
                // A short sleep might allow the initial value (if true) to be processed by the subscription if it was just set up.
                // However, if it's already true, the watch channel won't change from its initial 'false'.
                // So, check again *after* subscribing.
                if online_prop.get_value().as_bool().unwrap_or(false) {
                    log::info!("Asset '{}/{}' reported online immediately after subscription setup.", namespace, asset_name);
                     // No need to unsubscribe explicitly here, as the callback will drop when Asset/SubModel/Property are dropped.
                    return Ok(asset_proxy);
                }


                // Wait for the signal from the watch channel, with a timeout
                match tokio::time::timeout(
                    Duration::from_secs(timeout_duration_secs),
                    async {
                        // Wait until the received value is true
                        while !*rx.borrow() {
                            if rx.changed().await.is_err() {
                                // Sender dropped, means something went wrong or callback won't fire.
                                return Err(Error::CommunicationError("Online status watch channel closed unexpectedly.".to_string()));
                            }
                        }
                        Ok(())
                    }
                ).await {
                    Ok(Ok(_)) => { // Signal received and it was true
                        log::info!("Asset '{}/{}' is now online.", namespace, asset_name);
                        // Successfully online
                    }
                    Ok(Err(e)) => { // Internal error from the async block (e.g. watch channel closed)
                         log::warn!("Error waiting for online signal for '{}/{}': {:?}", namespace, asset_name, e);
                        return Err(e);
                    }
                    Err(_) => { // Timeout elapsed
                        log::warn!("Asset '{}/{}' did not come online within {} seconds.", namespace, asset_name, timeout_duration_secs);
                        return Err(Error::AssetNotOnlineError {
                            name: asset_name.clone(),
                            namespace: namespace.clone(),
                            timeout_secs: timeout_duration_secs,
                        });
                    }
                }
                 // TODO: Consider unsubscribing from on_change if the asset_proxy is not dropped immediately after this.
                 // However, if Ok(asset_proxy) is returned, the subscription should live as long as asset_proxy.
            }
        }
        Ok(asset_proxy)
    }
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
