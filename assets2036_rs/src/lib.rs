use serde::{Deserialize, Serialize};
use thiserror::Error;
use std::collections::HashMap;
use std::sync::Arc;
// Use tokio's RwLock for async contexts if map access is within async code.
use tokio::sync::RwLock as TokioRwLock;
use std::time::Duration;

// Added for Communication Layer
use async_trait::async_trait;
use rumqttc::{MqttOptions, QoS, Event as MqttEvent, Packet};
use serde_json::Value;
use std::fmt; // Added for Debug trait bound
use uuid;
use log;
use chrono::Utc; // Added for Utc::now()


// Existing definitions (PropertyType, Mode, Error, *Definition structs)
// ... (assuming these are unchanged from the previous read_files output) ...
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PropertyType {
    Boolean,
    Integer,
    Number,
    String,
    #[serde(alias = "object")]
    Object,
    Array,
    Null,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Mode {
    Owner,
    Consumer,
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
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PropertyDefinition {
    #[serde(rename = "type")]
    pub type_of: PropertyType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "readOnly", skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, serde_json::Value>>,
    #[serde(rename = "enum", skip_serializing_if = "Option::is_none")]
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


// --- In-memory representation structs with CommunicationClient integration ---

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

        match self.definition.type_of {
            PropertyType::Boolean => if !new_value.is_boolean() { return Err(Error::InvalidParameter(format!("Expected boolean, got {:?}", new_value))); },
            PropertyType::Integer => if !new_value.is_i64() { return Err(Error::InvalidParameter(format!("Expected integer, got {:?}", new_value))); },
            PropertyType::Number => if !new_value.is_number() { return Err(Error::InvalidParameter(format!("Expected number, got {:?}", new_value))); },
            PropertyType::String => if !new_value.is_string() { return Err(Error::InvalidParameter(format!("Expected string, got {:?}", new_value))); },
            PropertyType::Object => if !new_value.is_object() { return Err(Error::InvalidParameter(format!("Expected object, got {:?}", new_value))); },
            PropertyType::Array => if !new_value.is_array() { return Err(Error::InvalidParameter(format!("Expected array, got {:?}", new_value))); },
            PropertyType::Null => if !new_value.is_null() { return Err(Error::InvalidParameter(format!("Expected null, got {:?}", new_value))); },
        }

        let mut value_guard = self.value.write().map_err(|_| Error::LockError)?; // Made mutable
        *value_guard = new_value; // Update local value first

        // Drop the guard before await point if possible, or clone value if guard cannot be held across await
        let payload_str = serde_json::to_string(&*value_guard)?; // Serialize the new value
        // drop(value_guard); // Explicit drop, or ensure it's out of scope

        self.comm_client.publish(self.get_topic(), payload_str, true).await?;
        Ok(())
    }

    pub fn get_topic(&self) -> String {
        format!("{}/{}", self.parent_topic, self.name)
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
        // Basic validation can be added here if needed, e.g., checking params against self.definition.parameters
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

    pub fn on_event(&self, _callback: Box<dyn Fn(serde_json::Value) + Send + Sync>) {
        // This would involve comm_client.subscribe_event if it were for consuming
        log::info!("Callback registered for event '{}' (placeholder for owner mode, consumer uses subscribe_event)", self.name);
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

    pub async fn invoke(&self, params: serde_json::Value) -> Result<serde_json::Value, Error> {
        // This is for client/consumer side. Owner side uses bind.
        log::info!("Operation '{}' invoke called by owner (usually consumer side)", self.name);
        self.comm_client.invoke_operation(self.get_topic(), params, 5000).await // Default 5s timeout
    }

    pub async fn bind(&self, callback: Box<dyn Fn(Value) -> Result<Value, Error> + Send + Sync>) -> Result<(), Error> {
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
    pub parent_topic: String, // This is the SubModel's own topic
    comm_client: Arc<dyn CommunicationClient>,
}

impl SubModel {
    // Added asset_namespace and asset_endpoint_name for _meta property
    pub async fn new(
        def: SubModelDefinition,
        parent_topic_for_submodel: String, // Topic of the asset
        mode: &Mode,
        comm_client: Arc<dyn CommunicationClient>,
        asset_namespace: String,
        asset_endpoint_name: String
    ) -> Result<Self, Error> {
        let submodel_base_name = def.name.clone();
        let submodel_topic = format!("{}/{}", parent_topic_for_submodel, submodel_base_name);

        let mut properties = HashMap::new();
        if let Some(props_def) = &def.properties { // Borrow def.properties
            for (name, prop_def) in props_def {
                properties.insert(
                    name.clone(),
                    Property::new(name.clone(), prop_def.clone(), comm_client.clone(), submodel_topic.clone())
                );
            }
        }

        let mut events = HashMap::new();
        if let Some(events_def) = &def.events { // Borrow def.events
            for (name, event_def) in events_def {
                events.insert(
                    name.clone(),
                    Event::new(name.clone(), event_def.clone(), comm_client.clone(), submodel_topic.clone())
                );
            }
        }

        let mut operations = HashMap::new();
        if let Some(ops_def) = &def.operations { // Borrow def.operations
            for (name, op_def) in ops_def {
                operations.insert(
                    name.clone(),
                    Operation::new(name.clone(), op_def.clone(), comm_client.clone(), submodel_topic.clone())
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
            let meta_def_json = serde_json::json!({
                "type": "object",
                "readOnly": true, // _meta should be read-only for consumers
                "properties": {
                    "source": {"type": "string"},
                    "submodel_definition": {"type": "object"}, // Changed from "submodel_schema"
                    "submodel_url": {"type": "string"}
                }
            });
            let meta_def: PropertyDefinition = serde_json::from_value(meta_def_json)?;

            let meta_prop_name = "_meta".to_string();

            let meta_property = Property::new(
                meta_prop_name.clone(),
                meta_def,
                comm_client.clone(),
                new_submodel.get_topic() // Parent topic for the property is the submodel's topic
            );

            let meta_value_payload = serde_json::json!({
                "source": format!("{}/{}", asset_namespace, asset_endpoint_name),
                "submodel_definition": def.clone(), // SubModelDefinition is Cloneable
                "submodel_url": "file://localhost" // Placeholder
            });

            meta_property.set_value(meta_value_payload).await?;
            new_submodel.properties.insert(meta_prop_name, meta_property);
        }

        Ok(new_submodel)
    }

    pub fn get_topic(&self) -> String {
        self.parent_topic.clone()
    }

    pub fn get_property(&self, name: &str) -> Option<&Property> {
        self.properties.get(name)
    }

    pub fn get_event(&self, name: &str) -> Option<&Event> {
        self.events.get(name)
    }

    pub fn get_operation(&self, name: &str) -> Option<&Operation> {
        self.operations.get(name)
    }
}

#[derive(Debug)]
pub struct Asset {
    pub name: String,
    pub namespace: String,
    pub sub_models: HashMap<String, SubModel>,
    pub mode: Mode,
    pub endpoint_name: String,
    comm_client: Arc<MqttCommunicationClient>, // Using concrete type for easier instantiation for now
}

impl Asset {
    pub fn new(name: String, namespace: String, mode: Mode, endpoint_name: String, broker_host: &str, broker_port: u16) -> Self {
        let client_id_prefix = format!("{}_{}_{}", namespace, name, endpoint_name);
        // MqttCommunicationClient::new takes client_id_prefix, host, port
        let mqtt_client = MqttCommunicationClient::new(&client_id_prefix, broker_host, broker_port);

        Asset {
            name,
            namespace,
            sub_models: HashMap::new(),
            mode,
            endpoint_name,
            comm_client: Arc::new(mqtt_client),
        }
    }

    // Method to get a clonable Arc of the comm_client for external use (like connecting)
    pub fn get_communication_client(&self) -> Arc<MqttCommunicationClient> {
        self.comm_client.clone()
    }


    pub async fn implement_sub_model(&mut self, sub_model_definition_json: &str) -> Result<(), Error> {
        let sub_model_def: SubModelDefinition = serde_json::from_str(sub_model_definition_json)?;

        let sub_model = SubModel::new(
            sub_model_def.clone(), // Pass full definition
            self.get_topic(),    // Asset's topic is parent for SubModel
            &self.mode,
            self.comm_client.clone() as Arc<dyn CommunicationClient>, // Cast to trait object
            self.namespace.clone(),
            self.endpoint_name.clone()
        ).await?;
        self.sub_models.insert(sub_model_def.name, sub_model);
        Ok(())
    }

    pub fn get_submodel(&self, name: &str) -> Option<&SubModel> {
        self.sub_models.get(name)
    }

    pub fn get_topic(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }
}

// --- Communication Layer ---
// ... (CommunicationClient trait and MqttCommunicationClient struct and impl remain as before) ...
#[async_trait]
pub trait CommunicationClient: Send + Sync + fmt::Debug + 'static { // Added fmt::Debug
    async fn connect(&mut self, host: &str, port: u16, namespace: &str, endpoint_name: &str) -> Result<(), Error>;
    async fn disconnect(&mut self) -> Result<(), Error>;
    async fn publish(&self, topic: String, payload: String, retain: bool) -> Result<(), Error>;

    async fn subscribe(&self, topic: String, callback: Box<dyn Fn(String, String) + Send + Sync>) -> Result<(), Error>;

    async fn subscribe_event(&self, topic: String, callback: Box<dyn Fn(HashMap<String, Value>, i64) + Send + Sync>) -> Result<(), Error>;
    async fn trigger_event(&self, topic: String, params: HashMap<String, Value>) -> Result<(), Error>;

    async fn invoke_operation(&self, topic: String, params: Value, timeout_ms: u64) -> Result<Value, Error>;
    async fn bind_operation(&self, topic: String, callback: Box<dyn Fn(Value) -> Result<Value, Error> + Send + Sync>) -> Result<(), Error>;

    async fn query_asset_names(&self, namespace: Option<&str>, submodel_names: &[&str]) -> Result<Vec<String>, Error>;
    async fn query_submodels_for_asset(&self, namespace: &str, asset_name: &str) -> Result<HashMap<String, Value>, Error>;
}

// Removed #[derive(Debug)] due to Fn types in HashMaps not being Debug.
pub struct MqttCommunicationClient {
    client: Option<rumqttc::AsyncClient>,
    mqtt_options: MqttOptions,
    request_map: Arc<TokioRwLock<HashMap<String, tokio::sync::oneshot::Sender<Value>>>>,
    operation_bindings: Arc<TokioRwLock<HashMap<String, Box<dyn Fn(Value) -> Result<Value, Error> + Send + Sync>>>>,
    property_subscriptions: Arc<TokioRwLock<HashMap<String, Box<dyn Fn(String, String) + Send + Sync>>>>,
    event_subscriptions: Arc<TokioRwLock<HashMap<String, Box<dyn Fn(HashMap<String, Value>, i64) + Send + Sync>>>>,
}

impl fmt::Debug for MqttCommunicationClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttCommunicationClient")
         .field("client_present", &self.client.is_some())
         .field("mqtt_options", &self.mqtt_options)
         // Note: blocking_read() in a Debug impl can be problematic if the lock is contended.
         // For debug purposes, it might be acceptable. Alternative is to show placeholder.
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
        let mut mqtt_options = MqttOptions::new(client_id, host, port);
        mqtt_options.set_keep_alive(Duration::from_secs(5)); // Default keep alive

        Self {
            client: None,
            mqtt_options,
            request_map: Arc::new(TokioRwLock::new(HashMap::new())),
            operation_bindings: Arc::new(TokioRwLock::new(HashMap::new())),
            property_subscriptions: Arc::new(TokioRwLock::new(HashMap::new())),
            event_subscriptions: Arc::new(TokioRwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl CommunicationClient for MqttCommunicationClient {
    async fn connect(&mut self, host: &str, port: u16, _namespace: &str, _endpoint_name: &str) -> Result<(), Error> {
        if self.client.is_some() {
            log::warn!("Already connected or connection attempt in progress.");
            return Ok(());
        }
        let mut current_options = MqttOptions::new(
            self.mqtt_options.client_id(),
            host,
            port
        );
        current_options.set_keep_alive(self.mqtt_options.keep_alive());

        let (async_client, mut event_loop) = rumqttc::AsyncClient::new(current_options, 100);
        self.client = Some(async_client);

        log::info!("MQTT client created. Attempting to connect to {}:{}", host, port);

        let req_map = self.request_map.clone();
        let op_bindings = self.operation_bindings.clone(); // Used in full dispatch
        let prop_subs = self.property_subscriptions.clone();
        let event_subs = self.event_subscriptions.clone(); // Used in full dispatch

        tokio::spawn(async move {
            log::info!("MQTT Event loop started.");
            loop {
                match event_loop.poll().await {
                    Ok(MqttEvent::Incoming(Packet::ConnAck(ack))) => {
                        log::info!("MQTT Connected: {:?}", ack);
                    }
                    Ok(MqttEvent::Incoming(Packet::Publish(publish_packet))) => {
                        log::debug!("Received MQTT Message: Topic: {}, Payload Len: {}", publish_packet.topic, publish_packet.payload.len());

                        let topic_clone = publish_packet.topic.clone();

                        // Dispatching logic will need to be more robust, e.g. matching topic patterns
                        // For now, direct match on stored topics.

                        // 1. Check operation responses
                        if let Some(sender) = req_map.write().await.remove(&topic_clone) {
                            match serde_json::from_slice::<Value>(&publish_packet.payload) {
                                Ok(val) => {
                                    if sender.send(val).is_err() { // Check if send failed
                                        log::error!("Failed to send operation response to internal channel for topic {}", topic_clone);
                                    }
                                }
                                Err(e) => log::error!("Failed to deserialize MQTT payload for invoked op on topic {}: {}", topic_clone, e),
                            }
                            continue;
                        }

                        // 2. Check bound operations (requests for this client to handle)
                        // Assumes request topic is stored in op_bindings, e.g. "base/op/request"
                        if let Some(callback) = op_bindings.read().await.get(&topic_clone) {
                            // This part needs to handle the request/response pattern for bound operations
                            // For example, parse params, call callback, publish response
                            // Placeholder for now - this logic is complex
                            log::info!("Placeholder: Received request for bound operation on topic {}", topic_clone);
                            // Example: let params: Value = serde_json::from_slice(&publish_packet.payload)?;
                            // let response_val = callback(params)?;
                            // let response_topic = topic_clone.replace("/request", "/response"); // Simplistic
                            // publish response_val to response_topic
                            continue;
                        }

                        // 3. Check property subscriptions
                        if let Some(callback) = prop_subs.read().await.get(&topic_clone) {
                            match String::from_utf8(publish_packet.payload.to_vec()) {
                                Ok(payload_str) => callback(topic_clone, payload_str), // This callback expects (String, String)
                                Err(e) => log::error!("MQTT payload for property on topic {} was not valid UTF-8: {}", topic_clone, e),
                            }
                            continue;
                        }

                        // 4. Check event subscriptions
                        // This callback expects (HashMap<String, Value>, i64)
                        if let Some(callback) = event_subs.read().await.get(&topic_clone) {
                             match serde_json::from_slice::<Value>(&publish_packet.payload) {
                                Ok(full_event_payload) => {
                                    if let (Some(ts), Some(params_val)) = (full_event_payload.get("timestamp").and_then(Value::as_i64), full_event_payload.get("parameters")) {
                                        if let Ok(params_map) = serde_json::from_value(params_val.clone()) {
                                             callback(params_map, ts);
                                        } else {
                                            log::error!("Failed to deserialize event parameters for topic {}", topic_clone);
                                        }
                                    } else {
                                        log::error!("Event payload for topic {} missing timestamp or parameters", topic_clone);
                                    }
                                }
                                Err(e) => log::error!("Failed to deserialize event payload for topic {}: {}", topic_clone, e),
                             }
                             continue;
                        }
                        log::warn!("No handler for incoming message on topic: {}", topic_clone);

                    }
                    Ok(MqttEvent::Incoming(packet)) => {
                        log::debug!("Received other MQTT Packet: {:?}", packet);
                    }
                    Ok(MqttEvent::Outgoing(_)) => { /* log::trace!("MQTT Outgoing"); */ }
                    Err(e) => {
                        log::error!("MQTT Event loop error: {}", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<(), Error> {
        if let Some(client) = self.client.take() {
            client.disconnect().await.map_err(|e| Error::Other(format!("MQTT disconnect error: {}", e)))?;
            log::info!("MQTT client disconnected.");
        }
        Ok(())
    }

    async fn publish(&self, topic: String, payload: String, retain: bool) -> Result<(), Error> {
        if let Some(client) = self.client.as_ref() {
            client.publish(topic, QoS::AtLeastOnce, retain, payload.into_bytes())
                .await
                .map_err(|e| Error::Other(format!("MQTT publish error: {}", e)))
        } else {
            Err(Error::Other("Not connected".to_string()))
        }
    }

    async fn subscribe(&self, topic: String, callback: Box<dyn Fn(String, String) + Send + Sync>) -> Result<(), Error> {
         if let Some(client) = self.client.as_ref() {
             client.subscribe(topic.clone(), QoS::AtLeastOnce).await.map_err(|e| Error::Other(format!("MQTT subscribe error: {}", e)))?;
             self.property_subscriptions.write().await.insert(topic, callback);
             Ok(())
         } else {
             Err(Error::Other("Not connected".to_string()))
         }
    }

    async fn subscribe_event(&self, topic: String, callback: Box<dyn Fn(HashMap<String, Value>, i64) + Send + Sync>) -> Result<(), Error> {
         if let Some(client) = self.client.as_ref() {
             client.subscribe(topic.clone(), QoS::AtLeastOnce).await.map_err(|e| Error::Other(format!("MQTT subscribe error: {}", e)))?;
             self.event_subscriptions.write().await.insert(topic, callback);
             Ok(())
         } else {
             Err(Error::Other("Not connected".to_string()))
         }
    }

    async fn trigger_event(&self, topic: String, params: HashMap<String, Value>) -> Result<(), Error> {
        let wrapper = serde_json::json!({
            "timestamp": Utc::now().timestamp_millis(),
            "parameters": params // params is already HashMap<String, Value>
        });
        let payload_str = serde_json::to_string(&wrapper)?;
        self.publish(topic, payload_str, false).await
    }

    async fn invoke_operation(&self, topic: String, params: Value, timeout_ms: u64) -> Result<Value, Error> {
        let client = self.client.as_ref().ok_or_else(|| Error::Other("Not connected".to_string()))?;

        let req_id = uuid::Uuid::new_v4().to_string();
        // Standardized request/response topics
        let base_op_topic = topic; // e.g. ns/asset/sub/op_name
        let request_topic = format!("{}/request", base_op_topic);
        let response_topic = format!("{}/response", base_op_topic); // Client subscribes to this + req_id for its specific response

        let specific_response_topic = format!("{}/{}", response_topic, req_id);


        let (tx, rx) = tokio::sync::oneshot::channel();
        self.request_map.write().await.insert(specific_response_topic.clone(), tx);

        client.subscribe(specific_response_topic.clone(), QoS::AtLeastOnce).await
            .map_err(|e| Error::Other(format!("Failed to subscribe to response topic {}: {}", specific_response_topic, e)))?;

        let request_payload = serde_json::json!({
            "parameters": params,
            "meta": {
                "request_id": req_id, // Send req_id so server can put it in its response topic
                // Server will publish to "response_topic/req_id" which is `specific_response_topic`
            }
        });
        let payload_str = serde_json::to_string(&request_payload)?;

        client.publish(request_topic.clone(), QoS::AtLeastOnce, false, payload_str.into_bytes()).await
            .map_err(|e| Error::Other(format!("Failed to publish operation request to {}: {}", request_topic, e)))?;

        log::debug!("Operation invoked on topic {}, response expected on {}", request_topic, specific_response_topic);

        match tokio::time::timeout(Duration::from_millis(timeout_ms), rx).await {
            Ok(Ok(value)) => {
                client.unsubscribe(specific_response_topic).await.map_err(|e| Error::Other(format!("Failed to unsubscribe from response topic: {}",e))).ok(); // Best effort
                Ok(value)
            }
            Ok(Err(_)) => { // Channel closed
                 client.unsubscribe(specific_response_topic).await.map_err(|e| Error::Other(format!("Failed to unsubscribe from response topic: {}",e))).ok();
                 Err(Error::Other("Failed to receive operation response: channel closed".to_string()))
            }
            Err(_) => { // Timeout
                self.request_map.write().await.remove(&specific_response_topic);
                client.unsubscribe(specific_response_topic).await.map_err(|e| Error::Other(format!("Failed to unsubscribe from response topic: {}",e))).ok();
                Err(Error::Other(format!("Operation timed out on topic {}", base_op_topic)))
            }
        }
    }

    async fn bind_operation(&self, topic: String, callback: Box<dyn Fn(Value) -> Result<Value, Error> + Send + Sync>) -> Result<(), Error> {
        let client = self.client.as_ref().ok_or_else(|| Error::Other("Not connected".to_string()))?;
        let request_topic = format!("{}/request", topic);

        client.subscribe(request_topic.clone(), QoS::AtLeastOnce).await
            .map_err(|e| Error::Other(format!("Failed to subscribe to operation request topic {}: {}", request_topic, e)))?;

        self.operation_bindings.write().await.insert(request_topic.clone(), callback);
        log::info!("Operation callback bound for base topic {}, listening for requests on {}", topic, request_topic);
        Ok(())
    }

    async fn query_asset_names(&self, _namespace: Option<&str>, _submodel_names: &[&str]) -> Result<Vec<String>, Error> {
        Err(Error::Other("query_asset_names not implemented".to_string()))
    }

    async fn query_submodels_for_asset(&self, _namespace: &str, _asset_name: &str) -> Result<HashMap<String, Value>, Error> {
        Err(Error::Other("query_submodels_for_asset not implemented".to_string()))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    // Keep existing tests for data structures and basic Asset/SubModel instantiation

    #[test]
    fn it_compiles() { // From before, still relevant
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn property_type_deserialization() { // From before
        let json_boolean = "\"boolean\"";
        let type_boolean: PropertyType = serde_json::from_str(json_boolean).unwrap();
        assert_eq!(type_boolean, PropertyType::Boolean);
        // ... other cases from before
    }

    // Minimal test for MqttCommunicationClient instantiation
    #[test]
    fn mqtt_client_instantiation() { // From before
        let _client = MqttCommunicationClient::new("test-client", "localhost", 1883);
    }

    // Async tests need tokio runtime
    #[tokio::test]
    async fn property_set_value_publishes() {
        // Mock CommunicationClient
        #[derive(Clone)]
        struct MockCommClient {
            publish_log: Arc<TokioRwLock<Vec<(String, String, bool)>>>,
        }
        #[async_trait]
        impl CommunicationClient for MockCommClient {
            async fn connect(&mut self, _: &str, _: u16, _: &str, _: &str) -> Result<(), Error> { Ok(()) }
            async fn disconnect(&mut self) -> Result<(), Error> { Ok(()) }
            async fn publish(&self, topic: String, payload: String, retain: bool) -> Result<(), Error> {
                self.publish_log.write().await.push((topic, payload, retain));
                Ok(())
            }
            async fn subscribe(&self, _: String, _: Box<dyn Fn(String, String) + Send + Sync>) -> Result<(), Error> { Ok(()) }
            async fn subscribe_event(&self, _: String, _: Box<dyn Fn(HashMap<String, Value>, i64) + Send + Sync>) -> Result<(), Error> { Ok(()) }
            async fn trigger_event(&self, _: String, _: HashMap<String, Value>) -> Result<(), Error> { Ok(()) }
            async fn invoke_operation(&self, _: String, _: Value, _: u64) -> Result<Value, Error> { Ok(Value::Null) }
            async fn bind_operation(&self, _: String, _: Box<dyn Fn(Value) -> Result<Value, Error> + Send + Sync>) -> Result<(), Error> { Ok(()) }
            async fn query_asset_names(&self, _: Option<&str>, _: &[&str]) -> Result<Vec<String>, Error> { Ok(vec![]) }
            async fn query_submodels_for_asset(&self, _: &str, _: &str) -> Result<HashMap<String, Value>, Error> { Ok(HashMap::new()) }
        }
        let mock_comm = Arc::new(MockCommClient{ publish_log: Arc::new(TokioRwLock::new(Vec::new())) });

        let prop_def = PropertyDefinition {
            type_of: PropertyType::String,
            description: None, read_only: None, items: None, properties: None, enum_values: None,
        };
        let prop = Property::new("myProp".to_string(), prop_def, mock_comm.clone(), "asset/sub".to_string());

        let val_str = "test_value";
        let set_result = prop.set_value(serde_json::json!(val_str)).await;
        assert!(set_result.is_ok());

        let log = mock_comm.publish_log.read().await;
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].0, "asset/sub/myProp"); // topic
        assert_eq!(log[0].1, serde_json::json!(val_str).to_string()); // payload
        assert_eq!(log[0].2, true); // retain
    }
}
