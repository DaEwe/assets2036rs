use chrono::Utc;
use log;

use serde_json::Value;
use std::boxed::Box;
use std::collections::HashMap;

use std::sync::Arc;

use tokio::sync::RwLock as TokioRwLock;

pub const DEFAULT_OPERATION_TIMEOUT_MS: u64 = 5000;

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
                return Err(Error::InvalidParameter(format!(
                    "Type mismatch: expected boolean, got {:?} (value: {})",
                    value, value
                )));
            }
        }
        PropertyType::Integer => {
            if !value.is_i64() {
                return Err(Error::InvalidParameter(format!(
                    "Type mismatch: expected integer, got {:?} (value: {})",
                    value, value
                )));
            }
        }
        PropertyType::Number => {
            if !value.is_number() {
                return Err(Error::InvalidParameter(format!(
                    "Type mismatch: expected number, got {:?} (value: {})",
                    value, value
                )));
            }
        }
        PropertyType::String => {
            if !value.is_string() {
                return Err(Error::InvalidParameter(format!(
                    "Type mismatch: expected string, got {:?} (value: {})",
                    value, value
                )));
            }
        }
        PropertyType::Null => {
            if !value.is_null() {
                return Err(Error::InvalidParameter(format!(
                    "Type mismatch: expected null, got {:?} (value: {})",
                    value, value
                )));
            }
        }
        PropertyType::Object => {
            if !value.is_object() {
                return Err(Error::InvalidParameter(format!(
                    "Type mismatch: expected object, got {:?} (value: {})",
                    value, value
                )));
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
                return Err(Error::InvalidParameter(format!(
                    "Type mismatch: expected array, got {:?} (value: {})",
                    value, value
                )));
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
            return Err(Error::InvalidParameter(format!(
                "Value {} not in allowed enum values: {:?}",
                value, allowed_values
            )));
        }
    }
    Ok(())
}

#[derive(Debug)]
pub struct Property {
    pub name: String,
    pub definition: PropertyDefinition,
    pub value: Arc<std::sync::RwLock<serde_json::Value>>,
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
        validate_value(&new_value, &self.definition)?;
        let mut value_guard = self.value.write().map_err(|_| Error::LockError)?;
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
                        validate_value(val, def).map_err(|e| {
                            Error::InvalidParameter(format!(
                                "Validation failed for event parameter '{}': {}",
                                name, e
                            ))
                        })?;
                    } else {
                        return Err(Error::InvalidParameter(format!(
                            "Missing event parameter: {}",
                            name
                        )));
                    }
                }
            } else if !param_defs.is_empty() {
                return Err(Error::InvalidParameter(
                    "Event parameters should be an object".to_string(),
                ));
            }
        } else if !params.is_null() && !params.as_object().map_or(true, |m| m.is_empty()) {
            return Err(Error::InvalidParameter(
                "Event expects no parameters or an empty object".to_string(),
            ));
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
                            Error::InvalidParameter(format!(
                                "Validation failed for parameter '{}': {}",
                                name, e
                            ))
                        })?;
                    } else {
                        return Err(Error::InvalidParameter(format!(
                            "Missing parameter: {}",
                            name
                        )));
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
                return Err(Error::InvalidParameter(
                    "Parameters should be an object".to_string(),
                ));
            }
        } else if !params.is_null() && !params.as_object().map_or(true, |m| m.is_empty()) {
            return Err(Error::InvalidParameter(
                "Operation expects no parameters or an empty object".to_string(),
            ));
        }
        let response = self
            .comm_client
            .invoke_operation(self.get_topic(), params, DEFAULT_OPERATION_TIMEOUT_MS)
            .await?;
        if let Some(resp_def) = self.definition.response.as_ref() {
            validate_value(&response, resp_def).map_err(|e| {
                Error::Other(format!(
                    "Invalid response from operation '{}': {}",
                    self.name, e
                ))
            })?;
        } else if !response.is_null() {
            return Err(Error::Other(format!(
                "Operation '{}' expected no response (void), but got non-null.",
                self.name
            )));
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
        asset_endpoint_name: String,
    ) -> Result<Self, Error> {
        let submodel_base_name = def.name.clone();
        let submodel_topic = format!("{}/{}", parent_topic_for_submodel, submodel_base_name);
        let mut properties = HashMap::new();
        if let Some(props_def) = &def.properties {
            for (name, prop_def) in props_def {
                properties.insert(
                    name.clone(),
                    Property::new(
                        name.clone(),
                        prop_def.clone(),
                        comm_client.clone(),
                        submodel_topic.clone(),
                    ),
                );
            }
        }
        let mut events = HashMap::new();
        if let Some(events_def) = &def.events {
            for (name, event_def) in events_def {
                events.insert(
                    name.clone(),
                    Event::new(
                        name.clone(),
                        event_def.clone(),
                        comm_client.clone(),
                        submodel_topic.clone(),
                    ),
                );
            }
        }
        let mut operations = HashMap::new();
        if let Some(ops_def) = &def.operations {
            for (name, op_def) in ops_def {
                operations.insert(
                    name.clone(),
                    Operation::new(
                        name.clone(),
                        op_def.clone(),
                        comm_client.clone(),
                        submodel_topic.clone(),
                    ),
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
            let meta_property = Property::new(
                meta_prop_name.clone(),
                meta_def,
                comm_client.clone(),
                new_submodel.get_topic(),
            );
            let meta_value_payload = serde_json::json!({"source": format!("{}/{}", asset_namespace, asset_endpoint_name),"submodel_definition": def.clone(),"submodel_url": "file://localhost"});
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
}

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
        Self {
            host,
            port,
            namespace,
            endpoint_name,
            comm_client: Arc::new(mqtt_client),
            endpoint_asset: None,
            is_connected: Arc::new(TokioRwLock::new(false)),
        }
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
        const ENDPOINT_SUBMODEL_JSON: &str = r#"{"name": "_endpoint", "version": "1.0.0", "properties": {"online": {"type": "boolean"}, "healthy": {"type": "boolean"}}, "operations": {"ping": {"response": {"type": "string"}}, "shutdown": {}, "restart": {}}}"#;
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
        self.create_endpoint_asset().await?;
        Ok(())
    }
    pub async fn disconnect(&self) -> Result<(), Error> {
        let mut is_connected_guard = self.is_connected.write().await;
        if !*is_connected_guard {
            log::warn!("AssetManager already disconnected.");
            return Ok(());
        }
        if let Some(ep_asset) = self.endpoint_asset.as_ref() {
            if let Some(sm) = ep_asset.get_submodel("_endpoint") {
                if let Some(online_prop) = sm.get_property("online") {
                    if let Err(e) = online_prop.set_value(serde_json::json!(false)).await {
                        log::error!("Failed to set endpoint offline: {:?}", e);
                    }
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
        if !*self.is_connected.read().await {
            return Err(Error::Other("AssetManager not connected".to_string()));
        }
        let mut asset = Asset::new(
            name.clone(),
            self.namespace.clone(),
            mode,
            self.endpoint_name.clone(),
            self.comm_client.clone(),
        );
        for source_str in submodel_sources {
            if source_str.starts_with("http://")
                || source_str.starts_with("https://")
                || source_str.starts_with("file://")
            {
                log::warn!(
                    "Attempted to load submodel from URL '{}', which is not yet implemented.",
                    source_str
                );
                return Err(Error::UrlLoadingNotImplemented(source_str));
            } else {
                match asset.implement_sub_model(&source_str).await {
                    Ok(_) => log::info!(
                        "Implemented submodel for asset '{}' from direct JSON.",
                        name
                    ),
                    Err(e) => {
                        log::error!(
                            "Failed to implement submodel for asset '{}' from direct JSON: {:?}",
                            name,
                            e
                        );
                        return Err(e);
                    }
                }
            }
        }
        Ok(asset)
    }
    pub async fn create_asset_proxy(
        &self,
        namespace: String,
        asset_name: String,
    ) -> Result<Asset, Error> {
        if !*self.is_connected.read().await {
            return Err(Error::Other("AssetManager not connected".to_string()));
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
