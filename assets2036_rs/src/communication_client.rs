use async_trait::async_trait;
use chrono::Utc;
use rumqttc::{Event as MqttEvent, MqttOptions, Packet, QoS};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock as TokioRwLock;
use uuid;

use crate::error::Error;

#[async_trait]
pub trait CommunicationClient: Send + Sync + fmt::Debug + 'static {
    async fn connect(
        &self,
        host: &str,
        port: u16,
        namespace: &str,
        endpoint_name: &str,
    ) -> Result<(), Error>;
    async fn disconnect(&self) -> Result<(), Error>;
    async fn publish(&self, topic: String, payload: String, retain: bool) -> Result<(), Error>;
    async fn subscribe(
        &self,
        topic: String,
        callback: Box<dyn Fn(String, String) + Send + Sync + 'static>,
    ) -> Result<(), Error>;
    async fn unsubscribe(&self, topic: &str) -> Result<(), Error>;
    async fn subscribe_event(
        &self,
        topic: String,
        callback: Box<dyn Fn(HashMap<String, Value>, i64) + Send + Sync + 'static>,
    ) -> Result<(), Error>;
    async fn trigger_event(
        &self,
        topic: String,
        params: HashMap<String, Value>,
    ) -> Result<(), Error>;
    async fn invoke_operation(
        &self,
        operation_topic: String,
        params: Value,
        timeout_ms: u64,
    ) -> Result<Value, Error>;
    async fn bind_operation(
        &self,
        topic: String,
        callback: Box<dyn Fn(Value) -> Result<Value, Error> + Send + Sync + 'static>,
    ) -> Result<(), Error>;
    async fn query_asset_names(
        &self,
        namespace: Option<&str>,
        submodel_names: &[&str],
    ) -> Result<Vec<String>, Error>;
    async fn query_submodels_for_asset(
        &self,
        namespace: &str,
        asset_name: &str,
    ) -> Result<HashMap<String, Value>, Error>;
}

pub struct MqttCommunicationClient {
    client_handle: Arc<TokioRwLock<Option<rumqttc::AsyncClient>>>,
    event_loop_task: Arc<TokioRwLock<Option<tokio::task::JoinHandle<()>>>>,
    mqtt_options: MqttOptions,
    client_response_base_topic: String,
    request_map: Arc<TokioRwLock<HashMap<String, tokio::sync::oneshot::Sender<Value>>>>,
    operation_bindings: Arc<
        TokioRwLock<
            HashMap<String, Box<dyn Fn(Value) -> Result<Value, Error> + Send + Sync + 'static>>,
        >,
    >,
    property_subscriptions:
        Arc<TokioRwLock<HashMap<String, Box<dyn Fn(String, String) + Send + Sync + 'static>>>>,
    event_subscriptions: Arc<
        TokioRwLock<
            HashMap<String, Box<dyn Fn(HashMap<String, Value>, i64) + Send + Sync + 'static>>,
        >,
    >,
}

impl fmt::Debug for MqttCommunicationClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MqttCommunicationClient")
            .field(
                "client_present",
                &self.client_handle.blocking_read().is_some(),
            )
            .field(
                "event_loop_task_present",
                &self.event_loop_task.blocking_read().is_some(),
            )
            .field("mqtt_options", &self.mqtt_options)
            .field(
                "client_response_base_topic",
                &self.client_response_base_topic,
            )
            .field("request_map_len", &self.request_map.blocking_read().len())
            .field(
                "operation_bindings_len",
                &self.operation_bindings.blocking_read().len(),
            )
            .field(
                "property_subscriptions_len",
                &self.property_subscriptions.blocking_read().len(),
            )
            .field(
                "event_subscriptions_len",
                &self.event_subscriptions.blocking_read().len(),
            )
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
    async fn connect(
        &self,
        host: &str,
        port: u16,
        _namespace: &str,
        _endpoint_name: &str,
    ) -> Result<(), Error> {
        let mut client_handle_guard = self.client_handle.write().await;
        let mut task_guard = self.event_loop_task.write().await;
        if client_handle_guard.is_some() || task_guard.is_some() {
            log::warn!("Already connected or event loop task running.");
            return Ok(());
        }
        let mut current_connect_options =
            MqttOptions::new(self.mqtt_options.client_id(), host, port);
        current_connect_options.set_keep_alive(self.mqtt_options.keep_alive());
        let (new_async_client, mut event_loop) =
            rumqttc::AsyncClient::new(current_connect_options, 100);
        *client_handle_guard = Some(new_async_client);
        log::info!(
            "MQTT client created. Attempting to connect to {}:{}",
            host,
            port
        );
        let req_map = self.request_map.clone();
        let op_bindings = self.operation_bindings.clone();
        let prop_subs = self.property_subscriptions.clone();
        let event_subs = self.event_subscriptions.clone();
        let spawned_task = tokio::spawn(async move {
            log::info!("MQTT Event loop started.");
            loop {
                match event_loop.poll().await {
                    Ok(MqttEvent::Incoming(Packet::ConnAck(ack))) => {
                        log::info!("MQTT Connected: {:?}", ack);
                    }
                    Ok(MqttEvent::Incoming(Packet::Publish(publish_packet))) => {
                        let topic_clone = publish_packet.topic.clone();
                        log::debug!(
                            "Received MQTT Message: Topic: {}, Payload Len: {}",
                            topic_clone,
                            publish_packet.payload.len()
                        );
                        if let Some(sender) = req_map.write().await.remove(&topic_clone) {
                            match serde_json::from_slice::<Value>(&publish_packet.payload) {
                                Ok(val) => { if sender.send(val).is_err() { log::error!("Failed to send op response to internal channel for topic {}", topic_clone); } }
                                Err(e) => log::error!("Failed to deserialize MQTT payload for invoked op on topic {}: {}", topic_clone, e),
                            }
                            continue;
                        }
                        let op_bindings_guard = op_bindings.read().await;
                        if let Some(op_callback) = op_bindings_guard.get(&topic_clone) {
                            log::info!(
                                "Received request for bound operation on topic {}",
                                topic_clone
                            );
                            // TODO: Full logic for bound operation handling
                            drop(op_bindings_guard);
                            continue;
                        }
                        drop(op_bindings_guard);

                        if let Some(callback) = prop_subs.read().await.get(&topic_clone) {
                            match String::from_utf8(publish_packet.payload.to_vec()) {
                                Ok(payload_str) => callback(topic_clone, payload_str),
                                Err(e) => log::error!(
                                    "MQTT payload for property on topic {} was not valid UTF-8: {}",
                                    topic_clone,
                                    e
                                ),
                            }
                            continue;
                        }
                        if let Some(event_callback) = event_subs.read().await.get(&topic_clone) {
                            match serde_json::from_slice::<Value>(&publish_packet.payload) {
                                Ok(full_event_payload) => {
                                    if let (Some(ts_val), Some(params_val)) = (
                                        full_event_payload.get("timestamp"),
                                        full_event_payload.get("parameters"),
                                    ) {
                                        if let (Some(ts), Some(params_obj)) =
                                            (ts_val.as_i64(), params_val.as_object())
                                        {
                                            let params: HashMap<String, Value> = params_obj
                                                .iter()
                                                .map(|(k, v)| (k.clone(), v.clone()))
                                                .collect();
                                            event_callback(params, ts);
                                        } else {
                                            log::error!("Event payload for topic {} had incorrect types for ts/params.", topic_clone);
                                        }
                                    } else {
                                        log::error!("Event payload for topic {} missing 'timestamp' or 'parameters'.", topic_clone);
                                    }
                                }
                                Err(e) => log::error!(
                                    "Failed to deserialize event payload for topic {}: {}",
                                    topic_clone,
                                    e
                                ),
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
        *task_guard = Some(spawned_task);
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), Error> {
        let mut client_handle_guard = self.client_handle.write().await;
        let mut task_guard = self.event_loop_task.write().await;
        if let Some(client) = client_handle_guard.take() {
            if let Err(e) = client.disconnect().await {
                log::error!("MQTT client disconnect error: {}", e);
            } else {
                log::info!("MQTT client disconnected.");
            }
        } else {
            log::warn!("Disconnect called but client was not present.");
        }
        if let Some(task) = task_guard.take() {
            task.abort();
            log::info!("MQTT event loop task signalled to abort.");
        } else {
            log::warn!("Disconnect called but event loop task was not present.");
        }
        Ok(())
    }

    async fn publish(&self, topic: String, payload: String, retain: bool) -> Result<(), Error> {
        let client_guard = self.client_handle.read().await;
        if let Some(client) = client_guard.as_ref() {
            client
                .publish(topic, QoS::AtLeastOnce, retain, payload.into_bytes())
                .await
                .map_err(|e| Error::Other(format!("MQTT publish error: {}", e)))
        } else {
            Err(Error::Other("Not connected".to_string()))
        }
    }

    async fn subscribe(
        &self,
        topic: String,
        callback: Box<dyn Fn(String, String) + Send + Sync + 'static>,
    ) -> Result<(), Error> {
        let client_guard = self.client_handle.read().await;
        if let Some(client) = client_guard.as_ref() {
            client
                .subscribe(topic.clone(), QoS::AtLeastOnce)
                .await
                .map_err(|e| Error::Other(format!("MQTT subscribe error: {}", e)))?;
            self.property_subscriptions
                .write()
                .await
                .insert(topic, callback);
            Ok(())
        } else {
            Err(Error::Other("Not connected".to_string()))
        }
    }

    async fn unsubscribe(&self, topic: &str) -> Result<(), Error> {
        log::debug!("Unsubscribing from topic: {}", topic);
        let client_guard = self.client_handle.read().await;
        if let Some(client) = client_guard.as_ref() {
            client.unsubscribe(topic).await.map_err(|e| {
                Error::Other(format!("MQTT unsubscribe error for topic {}: {}", topic, e))
            })?;
        } else {
            log::warn!("Attempted to unsubscribe from broker while not connected (topic: {}). Will only clear local callbacks.", topic);
        }
        if self
            .property_subscriptions
            .write()
            .await
            .remove(topic)
            .is_some()
        {
            log::debug!(
                "Removed property subscription callback for topic: {}",
                topic
            );
        }
        if self
            .event_subscriptions
            .write()
            .await
            .remove(topic)
            .is_some()
        {
            log::debug!("Removed event subscription callback for topic: {}", topic);
        }
        Ok(())
    }

    async fn subscribe_event(
        &self,
        topic: String,
        callback: Box<dyn Fn(HashMap<String, Value>, i64) + Send + Sync + 'static>,
    ) -> Result<(), Error> {
        let client_guard = self.client_handle.read().await;
        if let Some(client) = client_guard.as_ref() {
            client
                .subscribe(topic.clone(), QoS::AtLeastOnce)
                .await
                .map_err(|e| Error::Other(format!("MQTT subscribe error: {}", e)))?;
            self.event_subscriptions
                .write()
                .await
                .insert(topic, callback);
            Ok(())
        } else {
            Err(Error::Other("Not connected".to_string()))
        }
    }

    async fn trigger_event(
        &self,
        topic: String,
        params: HashMap<String, Value>,
    ) -> Result<(), Error> {
        let wrapper =
            serde_json::json!({"timestamp": Utc::now().timestamp_millis(), "parameters": params });
        let payload_str = serde_json::to_string(&wrapper)?;
        self.publish(topic, payload_str, false).await
    }

    async fn invoke_operation(
        &self,
        operation_topic: String,
        params: Value,
        timeout_ms: u64,
    ) -> Result<Value, Error> {
        let client_guard = self.client_handle.read().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| Error::Other("Not connected".to_string()))?;
        let request_id = uuid::Uuid::new_v4().to_string();
        let request_publish_topic = format!("{}/request", operation_topic);
        let reply_to_topic = format!("{}/{}", self.client_response_base_topic, request_id);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.request_map
            .write()
            .await
            .insert(reply_to_topic.clone(), tx);

        let topic_for_subscribe_and_error = reply_to_topic.clone();
        client
            .subscribe(topic_for_subscribe_and_error.clone(), QoS::AtLeastOnce)
            .await
            .map_err(|e| {
                let map_clone = self.request_map.clone();
                let topic_for_remove_in_error = topic_for_subscribe_and_error.clone();
                tokio::spawn(async move {
                    map_clone.write().await.remove(&topic_for_remove_in_error);
                });
                Error::Other(format!(
                    "Failed to subscribe to response topic {}: {}",
                    topic_for_subscribe_and_error, e
                ))
            })?;

        let req_payload_json =
            serde_json::json!({"params": params, "reply_to": reply_to_topic.clone()});
        let req_payload_str = serde_json::to_string(&req_payload_json)?;

        let topic_for_publish_error_cleanup = reply_to_topic.clone();
        client
            .publish(
                request_publish_topic.clone(),
                QoS::AtLeastOnce,
                false,
                req_payload_str.into_bytes(),
            )
            .await
            .map_err(|e| {
                let map_clone = self.request_map.clone();
                let topic_for_remove_in_publish_error = topic_for_publish_error_cleanup.clone();
                tokio::spawn(async move {
                    map_clone
                        .write()
                        .await
                        .remove(&topic_for_remove_in_publish_error);
                });
                Error::Other(format!(
                    "Failed to publish op request to {}: {}",
                    request_publish_topic, e
                ))
            })?;
        log::debug!(
            "Op invoked on {}, reply on {}",
            request_publish_topic,
            reply_to_topic
        );
        let result = match tokio::time::timeout(Duration::from_millis(timeout_ms), rx).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(_)) => Err(Error::Other(
                "Op call failed: response channel error".to_string(),
            )),
            Err(_) => {
                self.request_map.write().await.remove(&reply_to_topic);
                Err(Error::Other(format!(
                    "Op call timed out for topic {} (response on {})",
                    operation_topic, reply_to_topic
                )))
            }
        };

        if let Err(e) = client.unsubscribe(reply_to_topic.clone()).await {
            log::warn!(
                "Failed to unsubscribe from response topic {}: {}",
                reply_to_topic,
                e
            );
        }

        if result.is_ok()
            || matches!(result, Err(Error::Other(ref s)) if s.contains("response channel error"))
        {
            self.request_map.write().await.remove(&reply_to_topic);
        }
        result
    }

    async fn bind_operation(
        &self,
        topic: String,
        callback: Box<dyn Fn(Value) -> Result<Value, Error> + Send + Sync + 'static>,
    ) -> Result<(), Error> {
        let client_guard = self.client_handle.read().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| Error::Other("Not connected".to_string()))?;
        let request_topic = format!("{}/request", topic);
        client
            .subscribe(request_topic.clone(), QoS::AtLeastOnce)
            .await
            .map_err(|e| {
                Error::Other(format!(
                    "Failed to subscribe to op request topic {}: {}",
                    request_topic, e
                ))
            })?;
        self.operation_bindings
            .write()
            .await
            .insert(request_topic.clone(), callback);
        log::info!(
            "Op callback bound for base topic {}, listening on {}",
            topic,
            request_topic
        );
        Ok(())
    }

    async fn query_asset_names(
        &self,
        _namespace: Option<&str>,
        _submodel_names: &[&str],
    ) -> Result<Vec<String>, Error> {
        log::warn!("MqttCommunicationClient::query_asset_names is a placeholder and not fully implemented.");
        Err(Error::Other(
            "query_asset_names not implemented".to_string(),
        ))
    }
    async fn query_submodels_for_asset(
        &self,
        namespace: &str,
        asset_name: &str,
    ) -> Result<HashMap<String, Value>, Error> {
        let wildcard_topic = format!("{}/{}/+/_meta", namespace, asset_name);
        log::info!(
            "Querying submodels for asset '{}/{}' using wildcard topic: {}",
            namespace,
            asset_name,
            wildcard_topic
        );
        let collected_metas = Arc::new(TokioRwLock::new(HashMap::new()));
        let internal_callback_collected_metas = collected_metas.clone();
        let internal_callback = Box::new(move |topic: String, payload_str: String| {
            log::debug!(
                "query_submodels_for_asset (callback): Received msg on topic: {}",
                topic
            );
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
                        log::error!("query_submodels_for_asset (callback): Failed to deserialize _meta for topic '{}'. Error: {}",topic, e);
                    }
                }
            } else {
                log::warn!("query_submodels_for_asset (callback): Msg on meta wildcard with unexpected topic: {}",topic);
            }
        });
        self.subscribe(wildcard_topic.clone(), internal_callback)
            .await?;
        log::debug!(
            "query_submodels_for_asset: Subscribed to {}",
            wildcard_topic
        );
        tokio::time::sleep(Duration::from_millis(1500)).await;
        match self.unsubscribe(&wildcard_topic).await {
            Ok(_) => log::debug!(
                "query_submodels_for_asset: Unsubscribed from {}",
                wildcard_topic
            ),
            Err(e) => log::error!(
                "query_submodels_for_asset: Error during unsubscribe from {}: {}",
                wildcard_topic,
                e
            ),
        }
        let final_metas = collected_metas.read().await.clone();
        if final_metas.is_empty() {
            log::warn!(
                "query_submodels_for_asset: No submodels found for asset '{}/{}' on topic '{}'",
                namespace,
                asset_name,
                wildcard_topic
            );
        } else {
            log::info!(
                "query_submodels_for_asset: Collected {} submodel(s) for asset '{}/{}'",
                final_metas.len(),
                namespace,
                asset_name
            );
        }
        Ok(final_metas)
    }
}
