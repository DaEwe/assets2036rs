use serde::{Deserialize, Serialize};
use thiserror::Error;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock as TokioRwLock;
use std::time::Duration;
use async_trait::async_trait;
use rumqttc::{MqttOptions, QoS, Event as MqttEvent, Packet};
use serde_json::Value;
use std::fmt;
use uuid;
use log;
use chrono::Utc;
use std::boxed::Box; // Added for Box<PropertyDefinition>

pub const DEFAULT_OPERATION_TIMEOUT_MS: u64 = 5000;

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
    Owner, Consumer,
}

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
    #[error("Submodel definition missing or invalid in _meta for {submodel_name} of asset {asset_name}")]
    InvalidMetaSubmodelDefinition { submodel_name: String, asset_name: String },
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
    #[serde(rename = "enum", alias = "enum_values", skip_serializing_if = "Option::is_none")] // Added alias for enum_values
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

// Helper function for validation
fn validate_value(value: &serde_json::Value, def: &PropertyDefinition) -> Result<(), Error> {
    match def.type_of {
        PropertyType::Boolean => if !value.is_boolean() { return Err(Error::InvalidParameter(format!("Type mismatch: expected boolean, got {:?} (value: {})", value, value))); }
        PropertyType::Integer => if !value.is_i64() { return Err(Error::InvalidParameter(format!("Type mismatch: expected integer, got {:?} (value: {})", value, value))); }
        PropertyType::Number  => if !value.is_number() { return Err(Error::InvalidParameter(format!("Type mismatch: expected number, got {:?} (value: {})", value, value))); }
        PropertyType::String  => if !value.is_string() { return Err(Error::InvalidParameter(format!("Type mismatch: expected string, got {:?} (value: {})", value, value))); }
        PropertyType::Null    => if !value.is_null() { return Err(Error::InvalidParameter(format!("Type mismatch: expected null, got {:?} (value: {})", value, value))); }
        PropertyType::Object  => {
            if !value.is_object() { return Err(Error::InvalidParameter(format!("Type mismatch: expected object, got {:?} (value: {})", value, value))); }
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
        PropertyType::Array   => {
            if !value.is_array() { return Err(Error::InvalidParameter(format!("Type mismatch: expected array, got {:?} (value: {})", value, value))); }
            if let Some(item_def) = def.items.as_ref() {
                for item_in_value in value.as_array().unwrap() {
                    validate_value(item_in_value, item_def)?;
                }
            }
        }
    }
    if let Some(allowed_values) = def.enum_values.as_ref() {
        if !allowed_values.contains(value) {
            return Err(Error::InvalidParameter(format!("Value {} not in allowed enum values: {:?}", value, allowed_values)));
        }
    }
    Ok(())
}


// --- CommunicationClient Trait ---
#[async_trait]
pub trait CommunicationClient: Send + Sync + fmt::Debug + 'static {
    async fn connect(&self, host: &str, port: u16, namespace: &str, endpoint_name: &str) -> Result<(), Error>;
    async fn disconnect(&self) -> Result<(), Error>;
    async fn publish(&self, topic: String, payload: String, retain: bool) -> Result<(), Error>;
    async fn subscribe(&self, topic: String, callback: Box<dyn Fn(String, String) + Send + Sync + 'static>) -> Result<(), Error>;
    async fn unsubscribe(&self, topic: &str) -> Result<(), Error>;
    async fn subscribe_event(&self, topic: String, callback: Box<dyn Fn(HashMap<String, Value>, i64) + Send + Sync + 'static>) -> Result<(), Error>;
    async fn trigger_event(&self, topic: String, params: HashMap<String, Value>) -> Result<(), Error>;
    async fn invoke_operation(&self, operation_topic: String, params: Value, timeout_ms: u64) -> Result<Value, Error>;
    async fn bind_operation(&self, topic: String, callback: Box<dyn Fn(Value) -> Result<Value, Error> + Send + Sync + 'static>) -> Result<(), Error>;
    async fn query_asset_names(&self, namespace: Option<&str>, submodel_names: &[&str]) -> Result<Vec<String>, Error>;
    async fn query_submodels_for_asset(&self, namespace: &str, asset_name: &str) -> Result<HashMap<String, Value>, Error>;
}

// --- MqttCommunicationClient ---
pub struct MqttCommunicationClient {
    client_handle: Arc<TokioRwLock<Option<rumqttc::AsyncClient>>>,
    event_loop_task: Arc<TokioRwLock<Option<tokio::task::JoinHandle<()>>>>,
    mqtt_options: MqttOptions,
    client_response_base_topic: String,
    request_map: Arc<TokioRwLock<HashMap<String, tokio::sync::oneshot::Sender<Value>>>>,
    operation_bindings: Arc<TokioRwLock<HashMap<String, Box<dyn Fn(Value) -> Result<Value, Error> + Send + Sync + 'static>>>>,
    property_subscriptions: Arc<TokioRwLock<HashMap<String, Box<dyn Fn(String, String) + Send + Sync + 'static>>>>,
    event_subscriptions: Arc<TokioRwLock<HashMap<String, Box<dyn Fn(HashMap<String, Value>, i64) + Send + Sync + 'static>>>>,
}

impl fmt::Debug for MqttCommunicationClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttCommunicationClient")
         .field("client_present", &self.client_handle.blocking_read().is_some())
         .field("event_loop_task_present", &self.event_loop_task.blocking_read().is_some())
         .field("mqtt_options", &self.mqtt_options)
         .field("client_response_base_topic", &self.client_response_base_topic)
         .field("request_map_len", &self.request_map.blocking_read().len())
         .field("operation_bindings_len", &self.operation_bindings.blocking_read().len())
         .field("property_subscriptions_len", &self.property_subscriptions.blocking_read().len())
         .field("event_subscriptions_len", &self.event_subscriptions.blocking_read().len())
         .finish()
    }
}

impl MqttCommunicationClient {
    pub fn new(client_id_prefix: &str, host: &str, port: u16) -> Self {
        let client_id = format!("{}-{}", client_id_prefix, uuid::Uuid::new_v4());
        let mut mqtt_options = MqttOptions::new(client_id.clone(), host, port);
        mqtt_options.set_keep_alive(Duration::from_secs(5));
        let client_response_base_topic = format!("{}/responses", client_id);

        Self {
            client_handle: Arc::new(TokioRwLock::new(None)),
            event_loop_task: Arc::new(TokioRwLock::new(None)),
            mqtt_options,
            client_response_base_topic,
            request_map: Arc::new(TokioRwLock::new(HashMap::new())),
            operation_bindings: Arc::new(TokioRwLock::new(HashMap::new())),
            property_subscriptions: Arc::new(TokioRwLock::new(HashMap::new())),
            event_subscriptions: Arc::new(TokioRwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl CommunicationClient for MqttCommunicationClient {
    async fn connect(&self, host: &str, port: u16, _namespace: &str, _endpoint_name: &str) -> Result<(), Error> {
        let mut client_handle_guard = self.client_handle.write().await;
        let mut task_guard = self.event_loop_task.write().await;

        if client_handle_guard.is_some() || task_guard.is_some() {
            log::warn!("Already connected or event loop task running.");
            return Ok(());
        }

        let mut current_connect_options = MqttOptions::new(self.mqtt_options.client_id(), host, port);
        current_connect_options.set_keep_alive(self.mqtt_options.keep_alive());

        let (new_async_client, mut event_loop) = rumqttc::AsyncClient::new(current_connect_options, 100);
        *client_handle_guard = Some(new_async_client);

        log::info!("MQTT client created. Attempting to connect to {}:{}", host, port);

        let req_map = self.request_map.clone();
        let op_bindings = self.operation_bindings.clone();
        let prop_subs = self.property_subscriptions.clone();
        let event_subs = self.event_subscriptions.clone();

        let spawned_task = tokio::spawn(async move {
            log::info!("MQTT Event loop started.");
            loop {
                match event_loop.poll().await {
                    Ok(MqttEvent::Incoming(Packet::ConnAck(ack))) => { log::info!("MQTT Connected: {:?}", ack); }
                    Ok(MqttEvent::Incoming(Packet::Publish(publish_packet))) => {
                        let topic_clone = publish_packet.topic.clone();
                        log::debug!("Received MQTT Message: Topic: {}, Payload Len: {}", topic_clone, publish_packet.payload.len());

                        if let Some(sender) = req_map.write().await.remove(&topic_clone) {
                            match serde_json::from_slice::<Value>(&publish_packet.payload) {
                                Ok(val) => { if sender.send(val).is_err() { log::error!("Failed to send op response to internal channel for topic {}", topic_clone); } }
                                Err(e) => log::error!("Failed to deserialize MQTT payload for invoked op on topic {}: {}", topic_clone, e),
                            }
                            continue;
                        }
                        if op_bindings.read().await.contains_key(&topic_clone) {
                             log::info!("Placeholder: Received request for bound operation on topic {}", topic_clone);
                             continue;
                        }
                        if let Some(callback) = prop_subs.read().await.get(&topic_clone) {
                            match String::from_utf8(publish_packet.payload.to_vec()) {
                                Ok(payload_str) => callback(topic_clone, payload_str),
                                Err(e) => log::error!("MQTT payload for property on topic {} was not valid UTF-8: {}", topic_clone, e),
                            }
                            continue;
                        }
                        if let Some(event_callback) = event_subs.read().await.get(&topic_clone) {
                            match serde_json::from_slice::<Value>(&publish_packet.payload) {
                                Ok(full_event_payload) => {
                                    if let (Some(ts_val), Some(params_val)) = (full_event_payload.get("timestamp"), full_event_payload.get("parameters")) {
                                        if let (Some(ts), Some(params_obj)) = (ts_val.as_i64(), params_val.as_object()) {
                                            let params: HashMap<String, Value> = params_obj.iter().map(|(k,v)| (k.clone(), v.clone())).collect();
                                            event_callback(params, ts);
                                        } else { log::error!("Event payload for topic {} had incorrect types for ts/params.", topic_clone); }
                                    } else { log::error!("Event payload for topic {} missing 'timestamp' or 'parameters'.", topic_clone); }
                                }
                                Err(e) => log::error!("Failed to deserialize event payload for topic {}: {}", topic_clone, e),
                            }
                            continue;
                        }
                        log::warn!("No handler for incoming message on topic: {}", topic_clone);
                    }
                    Ok(MqttEvent::Incoming(packet)) => { log::debug!("Received other MQTT Packet: {:?}", packet); }
                    Ok(MqttEvent::Outgoing(_)) => { /* log::trace!("MQTT Outgoing"); */ }
                    Err(e) => {
                        log::error!("MQTT Event loop error: {}", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
        *task_guard = Some(spawned_task);
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), Error> {
        let mut client_handle_guard = self.client_handle.write().await;
        let mut task_guard = self.event_loop_task.write().await;

        if let Some(client) = client_handle_guard.take() {
            if let Err(e) = client.disconnect().await { log::error!("MQTT client disconnect error: {}", e); }
            else { log::info!("MQTT client disconnected."); }
        } else { log::warn!("Disconnect called but client was not present."); }

        if let Some(task) = task_guard.take() {
            task.abort();
            log::info!("MQTT event loop task signalled to abort.");
        } else { log::warn!("Disconnect called but event loop task was not present."); }
        Ok(())
    }

    async fn publish(&self, topic: String, payload: String, retain: bool) -> Result<(), Error> {
        let client_guard = self.client_handle.read().await;
        if let Some(client) = client_guard.as_ref() {
            client.publish(topic, QoS::AtLeastOnce, retain, payload.into_bytes()).await
                .map_err(|e| Error::Other(format!("MQTT publish error: {}", e)))
        } else { Err(Error::Other("Not connected".to_string())) }
    }

    async fn subscribe(&self, topic: String, callback: Box<dyn Fn(String, String) + Send + Sync + 'static>) -> Result<(), Error> {
        let client_guard = self.client_handle.read().await;
        if let Some(client) = client_guard.as_ref() {
            client.subscribe(topic.clone(), QoS::AtLeastOnce).await.map_err(|e| Error::Other(format!("MQTT subscribe error: {}", e)))?;
            self.property_subscriptions.write().await.insert(topic, callback);
            Ok(())
        } else { Err(Error::Other("Not connected".to_string())) }
    }

    async fn unsubscribe(&self, topic: &str) -> Result<(), Error> {
        log::debug!("Unsubscribing from topic: {}", topic);
        let client_guard = self.client_handle.read().await;
        if let Some(client) = client_guard.as_ref() {
            client.unsubscribe(topic).await.map_err(|e| Error::Other(format!("MQTT unsubscribe error for topic {}: {}", topic, e)))?;
        } else {
            log::warn!("Attempted to unsubscribe from broker while not connected (topic: {}). Will only clear local callbacks.", topic);
        }

        if self.property_subscriptions.write().await.remove(topic).is_some() {
            log::debug!("Removed property subscription callback for topic: {}", topic);
        }
        if self.event_subscriptions.write().await.remove(topic).is_some() {
            log::debug!("Removed event subscription callback for topic: {}", topic);
        }
        Ok(())
    }

    async fn subscribe_event(&self, topic: String, callback: Box<dyn Fn(HashMap<String, Value>, i64) + Send + Sync + 'static>) -> Result<(), Error> {
        let client_guard = self.client_handle.read().await;
        if let Some(client) = client_guard.as_ref() {
            client.subscribe(topic.clone(), QoS::AtLeastOnce).await.map_err(|e| Error::Other(format!("MQTT subscribe error: {}", e)))?;
            self.event_subscriptions.write().await.insert(topic, callback);
            Ok(())
        } else { Err(Error::Other("Not connected".to_string())) }
    }

    async fn trigger_event(&self, topic: String, params: HashMap<String, Value>) -> Result<(), Error> {
        let wrapper = serde_json::json!({"timestamp": Utc::now().timestamp_millis(), "parameters": params });
        let payload_str = serde_json::to_string(&wrapper)?;
        self.publish(topic, payload_str, false).await
    }

    async fn invoke_operation(&self, operation_topic: String, params: Value, timeout_ms: u64) -> Result<Value, Error> {
        let client_guard = self.client_handle.read().await;
        let client = client_guard.as_ref().ok_or_else(|| Error::Other("Not connected".to_string()))?;

        let request_id = uuid::Uuid::new_v4().to_string();
        let request_publish_topic = format!("{}/request", operation_topic);
        let reply_to_topic = format!("{}/{}", self.client_response_base_topic, request_id);

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.request_map.write().await.insert(reply_to_topic.clone(), tx);

        client.subscribe(reply_to_topic.clone(), QoS::AtLeastOnce).await.map_err(|e| {
            let map_clone = self.request_map.clone(); let topic_c = reply_to_topic.clone();
            tokio::spawn(async move { map_clone.write().await.remove(&topic_c); });
            Error::Other(format!("Failed to subscribe to response topic {}: {}", reply_to_topic, e))
        })?;

        let req_payload_json = serde_json::json!({"params": params, "reply_to": reply_to_topic.clone()});
        let req_payload_str = serde_json::to_string(&req_payload_json)?;

        client.publish(request_publish_topic.clone(), QoS::AtLeastOnce, false, req_payload_str.into_bytes()).await.map_err(|e| {
            let map_clone = self.request_map.clone(); let topic_c = reply_to_topic.clone();
            tokio::spawn(async move { map_clone.write().await.remove(&topic_c); });
            Error::Other(format!("Failed to publish op request to {}: {}", request_publish_topic, e))
        })?;

        log::debug!("Op invoked on {}, reply on {}", request_publish_topic, reply_to_topic);

        let specific_response_topic_clone_for_map_removal = reply_to_topic.clone();
        let specific_response_topic_clone_for_unsubscribe = reply_to_topic.clone();
        let specific_response_topic_clone_for_timeout_log = reply_to_topic.clone();

        let result = match tokio::time::timeout(Duration::from_millis(timeout_ms), rx).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(_)) => Err(Error::Other("Op call failed: response channel error".to_string())),
            Err(_) => {
                self.request_map.write().await.remove(&specific_response_topic_clone_for_timeout_log);
                Err(Error::Other(format!("Op call timed out for topic {} (response on {})", operation_topic, specific_response_topic_clone_for_timeout_log)))
            }
        };

        if let Err(e) = client.unsubscribe(specific_response_topic_clone_for_unsubscribe.clone()).await {
             log::warn!("Failed to unsubscribe from response topic {}: {}", specific_response_topic_clone_for_unsubscribe, e);
        }
        if result.is_ok() || matches!(result, Err(Error::Other(ref s)) if s.contains("response channel error")) {
            self.request_map.write().await.remove(&specific_response_topic_clone_for_map_removal);
        }
        result
    }

    async fn bind_operation(&self, topic: String, callback: Box<dyn Fn(Value) -> Result<Value, Error> + Send + Sync + 'static>) -> Result<(), Error> {
        let client_guard = self.client_handle.read().await;
        let client = client_guard.as_ref().ok_or_else(|| Error::Other("Not connected".to_string()))?;
        let request_topic = format!("{}/request", topic);
        client.subscribe(request_topic.clone(), QoS::AtLeastOnce).await.map_err(|e| Error::Other(format!("Failed to subscribe to op request topic {}: {}", request_topic, e)))?;
        self.operation_bindings.write().await.insert(request_topic.clone(), callback);
        log::info!("Op callback bound for base topic {}, listening on {}", topic, request_topic);
        Ok(())
    }

    async fn query_asset_names(&self, _namespace: Option<&str>, _submodel_names: &[&str]) -> Result<Vec<String>, Error> {
        log::warn!("MqttCommunicationClient::query_asset_names is a placeholder and not fully implemented.");
        Err(Error::Other("query_asset_names not implemented".to_string()))
    }

    async fn query_submodels_for_asset(
        &self,
        namespace: &str,
        asset_name: &str,
    ) -> Result<HashMap<String, Value>, Error> {
        let wildcard_topic = format!("{}/{}/+/_meta", namespace, asset_name);
        log::info!(
            "Querying submodels for asset '{}/{}' using wildcard topic: {}",
            namespace, asset_name, wildcard_topic
        );

        let collected_metas = Arc::new(TokioRwLock::new(HashMap::new()));

        let internal_callback_collected_metas = collected_metas.clone();
        let internal_callback = Box::new(move |topic: String, payload_str: String| {
            log::debug!("query_submodels_for_asset (callback): Received msg on topic: {}", topic);

            let parts: Vec<&str> = topic.split('/').collect();
            if parts.len() == 4 && parts[3] == "_meta" {
                let sm_name = parts[2].to_string();
                match serde_json::from_str::<Value>(&payload_str) {
                    Ok(meta_value) => {
                        let metas_map_clone = internal_callback_collected_metas.clone();
                        tokio::spawn(async move {
                            metas_map_clone.write().await.insert(sm_name, meta_value);
                        });
                    }
                    Err(e) => {
                        log::error!(
                            "query_submodels_for_asset (callback): Failed to deserialize _meta payload for topic '{}'. Error: {}",
                            topic, e
                        );
                    }
                }
            } else {
                log::warn!(
                    "query_submodels_for_asset (callback): Received message on meta wildcard subscription with unexpected topic structure: {}",
                    topic
                );
            }
        });

        self.subscribe(wildcard_topic.clone(), internal_callback).await?;
        log::debug!("query_submodels_for_asset: Subscribed to {}", wildcard_topic);

        tokio::time::sleep(Duration::from_millis(1500)).await; // Configurable duration might be better

        match self.unsubscribe(&wildcard_topic).await {
            Ok(_) => log::debug!("query_submodels_for_asset: Unsubscribed from {}", wildcard_topic),
            Err(e) => log::error!("query_submodels_for_asset: Error during unsubscribe from {}: {}", wildcard_topic, e),
        }

        let final_metas = collected_metas.read().await.clone();
        if final_metas.is_empty() {
            log::warn!("query_submodels_for_asset: No submodels found for asset '{}/{}' on topic '{}'", namespace, asset_name, wildcard_topic);
        } else {
             log::info!(
                "query_submodels_for_asset: Collected {} submodel(s) for asset '{}/{}'",
                final_metas.len(), namespace, asset_name
            );
        }

        Ok(final_metas)
    }
}

// --- In-memory representation structs ---
#[derive(Debug)]
pub struct Property {
    pub name: String,
    pub definition: PropertyDefinition,
    pub value: Arc<std::sync::RwLock<serde_json::Value>>,
    pub parent_topic: String,
    comm_client: Arc<dyn CommunicationClient>,
}

impl Property {
    pub fn new(name: String, definition: PropertyDefinition, comm_client: Arc<dyn CommunicationClient>, parent_topic: String) -> Self {
        Property {
            name,
            definition,
            value: Arc::new(std::sync::RwLock::new(serde_json::Value::Null)),
            parent_topic,
            comm_client,
        }
    }

    pub fn get_value(&self) -> serde_json::Value {
        match self.value.read() {
            Ok(guard) => guard.clone(),
            Err(e) => {
                log::error!("Failed to read property value due to lock poisoning: {}", e);
                serde_json::Value::Null
            }
        }
    }

    pub async fn set_value(&self, new_value: serde_json::Value) -> Result<(), Error> {
        if self.definition.read_only == Some(true) {
            return Err(Error::NotWritable);
        }
        validate_value(&new_value, &self.definition)?; // Validate before setting

        let mut value_guard = self.value.write().map_err(|_| Error::LockError)?;
        *value_guard = new_value;

        let payload_str = serde_json::to_string(&*value_guard)?;
        self.comm_client.publish(self.get_topic(), payload_str, true).await?;
        Ok(())
    }

    pub fn get_topic(&self) -> String {
        format!("{}/{}", self.parent_topic, self.name)
    }

    pub async fn on_change(&self, callback: Box<dyn Fn(Value) + Send + Sync + 'static>) -> Result<(), Error> {
        let value_arc = self.value.clone();
        let internal_callback = move |_topic: String, payload_str: String| {
            match serde_json::from_str::<Value>(&payload_str) {
                Ok(new_prop_value) => {
                    match value_arc.write() {
                        Ok(mut guard) => *guard = new_prop_value.clone(),
                        Err(e) => {
                            log::error!("Failed to write property value after MQTT update (lock poisoned): {}", e);
                            return;
                        }
                    }
                    callback(new_prop_value);
                }
                Err(e) => {
                    log::error!("Failed to deserialize property payload: {}. Payload: {}", e, payload_str);
                }
            }
        };
        self.comm_client.subscribe(self.get_topic(), Box::new(internal_callback)).await
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
    pub fn new(name: String, definition: EventDefinition, comm_client: Arc<dyn CommunicationClient>, parent_topic: String) -> Self {
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
                        validate_value(val, def).map_err(|e| Error::InvalidParameter(format!("Validation failed for event parameter '{}': {}", name, e)))?;
                    } else {
                        return Err(Error::InvalidParameter(format!("Missing event parameter: {}", name)));
                    }
                }
            } else if !param_defs.is_empty() {
                 return Err(Error::InvalidParameter("Event parameters should be an object".to_string()));
            }
        } else if !params.is_null() && !params.as_object().map_or(true, |m| m.is_empty()){
            return Err(Error::InvalidParameter("Event expects no parameters or an empty object".to_string()));
        }

        let now_ms = Utc::now().timestamp_millis();
        let event_payload = serde_json::json!({
            "timestamp": now_ms,
            "parameters": params
        });
        let payload_str = serde_json::to_string(&event_payload)?;
        self.comm_client.publish(self.get_topic(), payload_str, false).await?;
        log::info!("Event '{}' triggered on topic {}", self.name, self.get_topic());
        Ok(())
    }

    pub async fn on_event(&self, callback: Box<dyn Fn(HashMap<String, Value>, i64) + Send + Sync + 'static>) -> Result<(), Error> {
        self.comm_client.subscribe_event(self.get_topic(), callback).await
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
    pub fn new(name: String, definition: OperationDefinition, comm_client: Arc<dyn CommunicationClient>, parent_topic: String) -> Self {
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
                        validate_value(val_from_user, def_val)
                            .map_err(|e| Error::InvalidParameter(format!("Validation failed for parameter '{}': {}", name, e)))?;
                    } else {
                        return Err(Error::InvalidParameter(format!("Missing parameter: {}", name)));
                    }
                }
                for user_param_name in params_obj.keys() {
                    if !param_defs_map.contains_key(user_param_name) {
                        log::warn!("Invoke: Unknown parameter '{}' provided for operation '{}'", user_param_name, self.name);
                    }
                }
            } else if !param_defs_map.is_empty() {
                return Err(Error::InvalidParameter("Parameters should be an object".to_string()));
            }
        } else if !params.is_null() && !params.as_object().map_or(true, |m| m.is_empty()){
            return Err(Error::InvalidParameter("Operation expects no parameters or an empty object".to_string()));
        }

        let response = self.comm_client.invoke_operation(self.get_topic(), params, DEFAULT_OPERATION_TIMEOUT_MS).await?;

        if let Some(resp_def) = self.definition.response.as_ref() {
            validate_value(&response, resp_def)
                .map_err(|e| Error::Other(format!("Invalid response from operation '{}': {}", self.name, e)))?;
        } else if !response.is_null() {
             return Err(Error::Other(format!("Operation '{}' expected no response (void), but got non-null.", self.name)));
        }
        Ok(response)
    }

    pub async fn bind(&self, callback: Box<dyn Fn(Value) -> Result<Value, Error> + Send + Sync + 'static>) -> Result<(), Error> {
        self.comm_client.bind_operation(self.get_topic(), callback).await?;
        log::info!("Operation '{}' bound on topic {}", self.name, self.get_topic());
        Ok(())
    }

    pub fn get_topic(&self) -> String {
        format!("{}/{}", self.parent_topic, self.name)
    }
}

#[derive(Debug)]
pub struct SubModel {
    pub name: String,
    pub properties: HashMap<String, Property>,
    pub events: HashMap<String, Event>,
    pub operations: HashMap<String, Operation>,
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
        asset_endpoint_name: String
    ) -> Result<Self, Error> {
        let submodel_base_name = def.name.clone();
        let submodel_topic = format!("{}/{}", parent_topic_for_submodel, submodel_base_name);

        let mut properties = HashMap::new();
        if let Some(props_def) = &def.properties {
            for (name, prop_def) in props_def {
                properties.insert(name.clone(), Property::new(name.clone(), prop_def.clone(), comm_client.clone(), submodel_topic.clone()));
            }
        }
        let mut events = HashMap::new();
        if let Some(events_def) = &def.events {
            for (name, event_def) in events_def {
                events.insert(name.clone(), Event::new(name.clone(), event_def.clone(), comm_client.clone(), submodel_topic.clone()));
            }
        }
        let mut operations = HashMap::new();
        if let Some(ops_def) = &def.operations {
            for (name, op_def) in ops_def {
                operations.insert(name.clone(), Operation::new(name.clone(), op_def.clone(), comm_client.clone(), submodel_topic.clone()));
            }
        }
        let mut new_submodel = Self {
            name: submodel_base_name, properties, events, operations,
            parent_topic: submodel_topic.clone(),
            comm_client: comm_client.clone(),
        };
        if mode == &Mode::Owner {
            let meta_def_json = serde_json::json!({"type": "object", "readOnly": true, "properties": {"source": {"type": "string"},"submodel_definition": {"type": "object"},"submodel_url": {"type": "string"}}});
            let meta_def: PropertyDefinition = serde_json::from_value(meta_def_json)?;
            let meta_prop_name = "_meta".to_string();
            let meta_property = Property::new(meta_prop_name.clone(), meta_def, comm_client.clone(), new_submodel.get_topic());
            let meta_value_payload = serde_json::json!({"source": format!("{}/{}", asset_namespace, asset_endpoint_name),"submodel_definition": def.clone(),"submodel_url": "file://localhost"});
            meta_property.set_value(meta_value_payload).await?;
            new_submodel.properties.insert(meta_prop_name, meta_property);
        }
        Ok(new_submodel)
    }

    pub fn get_topic(&self) -> String { self.parent_topic.clone() }
    pub fn get_property(&self, name: &str) -> Option<&Property> { self.properties.get(name) }
    pub fn get_event(&self, name: &str) -> Option<&Event> { self.events.get(name) }
    pub fn get_operation(&self, name: &str) -> Option<&Operation> { self.operations.get(name) }
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
    pub fn new(name: String, namespace: String, mode: Mode, endpoint_name: String, comm_client: Arc<dyn CommunicationClient>) -> Self {
        Asset { name, namespace, sub_models: HashMap::new(), mode, endpoint_name, comm_client }
    }
    pub fn get_communication_client(&self) -> Arc<dyn CommunicationClient> { self.comm_client.clone() }

    pub async fn implement_sub_model(&mut self, sub_model_definition_json: &str) -> Result<(), Error> {
        let sub_model_def: SubModelDefinition = serde_json::from_str(sub_model_definition_json)?;
        let sub_model = SubModel::new(sub_model_def.clone(), self.get_topic(), &self.mode, self.comm_client.clone(), self.namespace.clone(), self.endpoint_name.clone()).await?;
        self.sub_models.insert(sub_model_def.name, sub_model);
        Ok(())
    }
    pub fn get_submodel(&self, name: &str) -> Option<&SubModel> { self.sub_models.get(name) }
    pub fn get_topic(&self) -> String { format!("{}/{}", self.namespace, self.name) }
}

// --- AssetManager ---
pub struct AssetManager {
    host: String,
    port: u16,
    namespace: String,
    endpoint_name: String,
    comm_client: Arc<dyn CommunicationClient>,
    endpoint_asset: Option<Asset>,
    is_connected: Arc<TokioRwLock<bool>>,
}

impl AssetManager {
    pub fn new(host: String, port: u16, namespace: String, endpoint_name: String) -> Self {
        let client_id_prefix = format!("{}/{}", namespace, endpoint_name);
        let mqtt_client = MqttCommunicationClient::new(&client_id_prefix, &host, port);
        Self { host, port, namespace, endpoint_name, comm_client: Arc::new(mqtt_client), endpoint_asset: None, is_connected: Arc::new(TokioRwLock::new(false)) }
    }

    async fn create_endpoint_asset(&mut self) -> Result<(), Error> {
        if self.endpoint_asset.is_some() { return Ok(()); }
        let ep_asset_name = "_endpoint".to_string();
        let mut ep_asset = Asset::new(ep_asset_name.clone(), self.namespace.clone(), Mode::Owner, self.endpoint_name.clone(), self.comm_client.clone());
        const ENDPOINT_SUBMODEL_JSON: &str = r#"{"name": "_endpoint", "version": "1.0.0", "properties": {"online": {"type": "boolean"}, "healthy": {"type": "boolean"}}, "operations": {"ping": {"response": {"type": "string"}}, "shutdown": {}, "restart": {}}}"#;
        ep_asset.implement_sub_model(ENDPOINT_SUBMODEL_JSON).await?;
        if let Some(sm) = ep_asset.get_submodel("_endpoint") {
            if let Some(online_prop) = sm.get_property("online") { online_prop.set_value(serde_json::json!(true)).await?; }
            if let Some(healthy_prop) = sm.get_property("healthy") { healthy_prop.set_value(serde_json::json!(true)).await?; }
            if let Some(ping_op) = sm.get_operation("ping") { ping_op.bind(Box::new(|_params| Ok(serde_json::json!("pong")))).await?; }
            if let Some(shutdown_op) = sm.get_operation("shutdown") { shutdown_op.bind(Box::new(move |_params| { log::info!("Shutdown op called (Placeholder)"); Ok(Value::Null) })).await?; }
            if let Some(restart_op) = sm.get_operation("restart") { restart_op.bind(Box::new(|_params| { log::info!("Restart op called (Placeholder)"); Ok(Value::Null) })).await?; }
        }
        self.endpoint_asset = Some(ep_asset);
        log::info!("Endpoint asset '_endpoint' created for AssetManager {}.", self.endpoint_name);
        Ok(())
    }

    pub async fn connect(&mut self) -> Result<(), Error> {
        let mut is_connected_guard = self.is_connected.write().await;
        if *is_connected_guard { log::warn!("AssetManager already connected."); return Ok(()); }
        self.comm_client.connect(&self.host, self.port, &self.namespace, &self.endpoint_name).await?;
        *is_connected_guard = true;
        log::info!("AssetManager connected to MQTT broker at {}:{}", self.host, self.port);
        drop(is_connected_guard);
        self.create_endpoint_asset().await?;
        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), Error> {
        let mut is_connected_guard = self.is_connected.write().await;
        if !*is_connected_guard { log::warn!("AssetManager already disconnected."); return Ok(()); }
        if let Some(ep_asset) = self.endpoint_asset.as_ref() {
            if let Some(sm) = ep_asset.get_submodel("_endpoint") {
                if let Some(online_prop) = sm.get_property("online") {
                    if let Err(e) = online_prop.set_value(serde_json::json!(false)).await { log::error!("Failed to set endpoint offline: {:?}", e); }
                }
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
       if !*self.is_connected.read().await { // Corrected this line
           return Err(Error::Other("AssetManager not connected".to_string()));
       }

       let mut asset = Asset::new(
           name.clone(),
           self.namespace.clone(),
           mode,
           self.endpoint_name.clone(), // Assets created by this manager share its endpoint identity on the network
           self.comm_client.clone(),
       );

       for source_str in submodel_sources {
           if source_str.starts_with("http://") || source_str.starts_with("https://") || source_str.starts_with("file://") {
               log::warn!("Attempted to load submodel from URL '{}', which is not yet implemented.", source_str);
               return Err(Error::UrlLoadingNotImplemented(source_str));
           } else {
               match asset.implement_sub_model(&source_str).await {
                   Ok(_) => log::info!("Implemented submodel for asset '{}' from direct JSON.", name),
                   Err(e) => {
                       log::error!("Failed to implement submodel for asset '{}' from direct JSON: {:?}", name, e);
                       return Err(e);
                   }
               }
           }
       }
       Ok(asset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json; // Ensure this is imported for json! macro
    use std::boxed::Box; // Ensure Box is imported for tests using it

    #[test]
    fn it_compiles() { assert_eq!(2 + 2, 4); }

    #[test]
    fn property_type_deserialization() {
        let json_boolean = "\"boolean\"";
        let type_boolean: PropertyType = serde_json::from_str(json_boolean).unwrap();
        assert_eq!(type_boolean, PropertyType::Boolean);
    }

    #[test]
    fn test_deserialize_bool_property() {
        let json_str = r#"{"type": "boolean", "description": "A switch"}"#;
        let prop_def: PropertyDefinition = serde_json::from_str(json_str).expect("Failed to deserialize bool property");
        assert_eq!(prop_def.type_of, PropertyType::Boolean);
        assert_eq!(prop_def.description.as_deref(), Some("A switch"));
    }

    #[test]
    fn test_deserialize_object_property() {
        let json_str = r#"{
            "type": "object",
            "properties": {
                "x": {"type": "integer"},
                "y": {"type": "number", "description": "Y coordinate"}
            }
        }"#;
        let prop_def: PropertyDefinition = serde_json::from_str(json_str).expect("Failed to deserialize object property");
        assert_eq!(prop_def.type_of, PropertyType::Object);
        let props_map = prop_def.properties.as_ref().expect("Properties map should exist");

        let x_def = props_map.get("x").expect("Property x should exist").as_ref();
        assert_eq!(x_def.type_of, PropertyType::Integer);

        let y_def = props_map.get("y").expect("Property y should exist").as_ref();
        assert_eq!(y_def.type_of, PropertyType::Number);
        assert_eq!(y_def.description.as_deref(), Some("Y coordinate"));
    }

    #[test]
    fn test_deserialize_array_property() {
        let json_str = r#"{"type": "array", "items": {"type": "string"}}"#;
        let prop_def: PropertyDefinition = serde_json::from_str(json_str).expect("Failed to deserialize array property");
        assert_eq!(prop_def.type_of, PropertyType::Array);
        let items_def = prop_def.items.as_ref().expect("Items definition should exist").as_ref();
        assert_eq!(items_def.type_of, PropertyType::String);
    }

    #[test]
    fn test_deserialize_enum_property() {
        let json_str = r#"{"type": "string", "enum": ["ON", "OFF"]}"#;
        let prop_def: PropertyDefinition = serde_json::from_str(json_str).expect("Failed to deserialize enum property");
        assert_eq!(prop_def.type_of, PropertyType::String);
        assert_eq!(prop_def.enum_values, Some(vec![json!("ON"), json!("OFF")]));
    }

    #[test]
    fn test_deserialize_submodel_definition() {
        let json_str = r#"{
            "name": "example_submodel",
            "version": "1.0",
            "properties": {
                "status": {"type": "string", "enum": ["active", "inactive"]},
                "count": {"type": "integer", "readOnly": true}
            },
            "events": {
                "overheat": {"parameters": {"temperature": {"type": "number"}}}
            },
            "operations": {
                "reset": {"response": {"type": "boolean"}}
            }
        }"#;
        let sm_def: SubModelDefinition = serde_json::from_str(json_str).expect("Failed to deserialize submodel");
        assert_eq!(sm_def.name, "example_submodel");
        assert_eq!(sm_def.version.as_deref(), Some("1.0"));
        assert!(sm_def.properties.is_some());
        assert_eq!(sm_def.properties.as_ref().unwrap().len(), 2);
        assert!(sm_def.events.is_some());
        assert_eq!(sm_def.events.as_ref().unwrap().len(), 1);
        assert!(sm_def.operations.is_some());
        assert_eq!(sm_def.operations.as_ref().unwrap().len(), 1);
    }

    // --- Tests for validate_value ---
    #[test]
    fn test_validate_value_simple_types_ok() {
        let p_bool = PropertyDefinition { type_of: PropertyType::Boolean, ..Default::default() };
        assert!(validate_value(&json!(true), &p_bool).is_ok());
        let p_int = PropertyDefinition { type_of: PropertyType::Integer, ..Default::default() };
        assert!(validate_value(&json!(123), &p_int).is_ok());
        let p_num = PropertyDefinition { type_of: PropertyType::Number, ..Default::default() };
        assert!(validate_value(&json!(123.45), &p_num).is_ok());
        assert!(validate_value(&json!(123), &p_num).is_ok()); // Integer is also a number
        let p_str = PropertyDefinition { type_of: PropertyType::String, ..Default::default() };
        assert!(validate_value(&json!("hello"), &p_str).is_ok());
        let p_null = PropertyDefinition { type_of: PropertyType::Null, ..Default::default() };
        assert!(validate_value(&json!(null), &p_null).is_ok());
    }

    #[test]
    fn test_validate_value_simple_types_err() {
        let p_bool = PropertyDefinition { type_of: PropertyType::Boolean, ..Default::default() };
        assert!(validate_value(&json!(123), &p_bool).is_err());
        let p_int = PropertyDefinition { type_of: PropertyType::Integer, ..Default::default() };
        assert!(validate_value(&json!("test"), &p_int).is_err());
        assert!(validate_value(&json!(12.34), &p_int).is_err()); // Number is not necessarily integer
    }

    #[test]
    fn test_validate_value_enum() {
        let p_enum = PropertyDefinition {
            type_of: PropertyType::String,
            enum_values: Some(vec![json!("ON"), json!("OFF")]),
            ..Default::default()
        };
        assert!(validate_value(&json!("ON"), &p_enum).is_ok());
        assert!(validate_value(&json!("STANDBY"), &p_enum).is_err());
        let p_num_enum = PropertyDefinition {
            type_of: PropertyType::Integer,
            enum_values: Some(vec![json!(1), json!(2), json!(3)]),
            ..Default::default()
        };
        assert!(validate_value(&json!(1), &p_num_enum).is_ok());
        assert!(validate_value(&json!(4), &p_num_enum).is_err());
    }

    #[test]
    fn test_validate_value_object_ok() {
        let mut props_map = HashMap::new();
        props_map.insert("x".to_string(), Box::new(PropertyDefinition {type_of: PropertyType::Integer, ..Default::default()}));
        props_map.insert("s".to_string(), Box::new(PropertyDefinition {type_of: PropertyType::String, ..Default::default()}));
        let p_obj = PropertyDefinition { type_of: PropertyType::Object, properties: Some(props_map), ..Default::default()};

        assert!(validate_value(&json!({"x": 10, "s": "test"}), &p_obj).is_ok());
        assert!(validate_value(&json!({"x": 10, "s": "test", "y_extra": true}), &p_obj).is_ok()); // Extra props are ignored
    }

    #[test]
    fn test_validate_value_object_err_subtype() {
        let mut props_map = HashMap::new();
        props_map.insert("x".to_string(), Box::new(PropertyDefinition {type_of: PropertyType::Integer, ..Default::default()}));
        let p_obj = PropertyDefinition { type_of: PropertyType::Object, properties: Some(props_map), ..Default::default()};
        assert!(validate_value(&json!({"x": "not-an-int"}), &p_obj).is_err());
    }

    #[test]
    fn test_validate_value_object_nested() {
        let mut nested_props = HashMap::new();
        nested_props.insert("b".to_string(), Box::new(PropertyDefinition{type_of: PropertyType::Boolean, ..Default::default()}));
        let mut props_map = HashMap::new();
        props_map.insert("nested".to_string(), Box::new(PropertyDefinition {type_of: PropertyType::Object, properties: Some(nested_props), ..Default::default()}));
        let p_obj = PropertyDefinition { type_of: PropertyType::Object, properties: Some(props_map), ..Default::default()};

        assert!(validate_value(&json!({"nested": {"b": true}}), &p_obj).is_ok());
        assert!(validate_value(&json!({"nested": {"b": "true"}}), &p_obj).is_err());
    }


    #[test]
    fn test_validate_value_array_ok() {
        let p_arr = PropertyDefinition {
            type_of: PropertyType::Array,
            items: Some(Box::new(PropertyDefinition {type_of: PropertyType::String, ..Default::default()})),
            ..Default::default()
        };
        assert!(validate_value(&json!(["a", "b", "c"]), &p_arr).is_ok());
        assert!(validate_value(&json!([]), &p_arr).is_ok()); // Empty array is valid
    }

    #[test]
    fn test_validate_value_array_err_subtype() {
        let p_arr = PropertyDefinition {
            type_of: PropertyType::Array,
            items: Some(Box::new(PropertyDefinition {type_of: PropertyType::String, ..Default::default()})),
            ..Default::default()
        };
        assert!(validate_value(&json!(["a", 123]), &p_arr).is_err());
    }

    #[test]
    fn test_validate_value_array_of_objects() {
        let mut item_props = HashMap::new();
        item_props.insert("id".to_string(), Box::new(PropertyDefinition{type_of: PropertyType::Integer, ..Default::default()}));
        let p_arr = PropertyDefinition {
            type_of: PropertyType::Array,
            items: Some(Box::new(PropertyDefinition{type_of: PropertyType::Object, properties: Some(item_props), ..Default::default()})),
            ..Default::default()
        };
        assert!(validate_value(&json!([{"id": 1}, {"id": 2}]), &p_arr).is_ok());
        assert!(validate_value(&json!([{"id": 1}, {"id": "2"}]), &p_arr).is_err());
    }

    #[test]
    fn mqtt_client_instantiation() {
        let _client = MqttCommunicationClient::new("test-client", "localhost", 1883);
    }
    #[tokio::test]
    async fn property_set_value_publishes() {
        #[derive(Clone)]
        struct MockCommClient {
            publish_log: Arc<TokioRwLock<Vec<(String, String, bool)>>>,
            subscribe_log: Arc<TokioRwLock<Vec<String>>>,
            subscribe_event_log: Arc<TokioRwLock<Vec<String>>>,
        }
        impl fmt::Debug for MockCommClient {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct("MockCommClient")
                 .field("publish_log_len", &self.publish_log.blocking_read().len())
                 .field("subscribe_log_len", &self.subscribe_log.blocking_read().len())
                 .field("subscribe_event_log_len", &self.subscribe_event_log.blocking_read().len())
                 .finish()
            }
        }
        #[async_trait]
        impl CommunicationClient for MockCommClient {
            async fn connect(&self, _: &str, _: u16, _: &str, _: &str) -> Result<(), Error> { Ok(()) }
            async fn disconnect(&self) -> Result<(), Error> { Ok(()) }
            async fn publish(&self, topic: String, payload: String, retain: bool) -> Result<(), Error> {
                self.publish_log.write().await.push((topic, payload, retain)); Ok(())
            }
            async fn subscribe(&self, topic: String, _: Box<dyn Fn(String, String) + Send + Sync + 'static>) -> Result<(), Error> {
                self.subscribe_log.write().await.push(topic); Ok(())
            }
            async fn unsubscribe(&self, _topic: &str) -> Result<(), Error> { Ok(()) }
            async fn subscribe_event(&self, topic: String, _: Box<dyn Fn(HashMap<String, Value>, i64) + Send + Sync + 'static>) -> Result<(), Error> {
                self.subscribe_event_log.write().await.push(topic); Ok(())
            }
            async fn trigger_event(&self, _: String, _: HashMap<String, Value>) -> Result<(), Error> { Ok(()) }
            async fn invoke_operation(&self, _: String, _: Value, _: u64) -> Result<Value, Error> { Ok(Value::Null) }
            async fn bind_operation(&self, _: String, _: Box<dyn Fn(Value) -> Result<Value, Error> + Send + Sync + 'static>) -> Result<(), Error> { Ok(()) }
            async fn query_asset_names(&self, _: Option<&str>, _: &[&str]) -> Result<Vec<String>, Error> { Ok(vec![]) }
            async fn query_submodels_for_asset(&self, _: &str, _: &str) -> Result<HashMap<String, Value>, Error> { Ok(HashMap::new()) }
        }
        let mock_comm = Arc::new(MockCommClient{
            publish_log: Arc::new(TokioRwLock::new(Vec::new())),
            subscribe_log: Arc::new(TokioRwLock::new(Vec::new())),
            subscribe_event_log: Arc::new(TokioRwLock::new(Vec::new())),
        });
        let prop_def = PropertyDefinition { type_of: PropertyType::String, description: None, read_only: None, items: None, properties: None, enum_values: None, };
        let prop = Property::new("myProp".to_string(), prop_def, mock_comm.clone(), "asset/sub".to_string());
        let val_str = "test_value";
        assert!(prop.set_value(serde_json::json!(val_str)).await.is_ok());
        let log = mock_comm.publish_log.read().await;
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].0, "asset/sub/myProp");
        assert_eq!(log[0].1, serde_json::json!(val_str).to_string());
        assert_eq!(log[0].2, true);
    }
}
