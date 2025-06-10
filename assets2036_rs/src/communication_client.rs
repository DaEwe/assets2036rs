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
    async fn query_asset_children(
        &self,
        parent_namespace: &str,
        parent_asset_name: &str,
    ) -> Result<Vec<crate::ChildAssetInfo>, Error>;
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
        _namespace: &str, // namespace and endpoint_name are used for client_id generation in new()
        _endpoint_name: &str,
    ) -> Result<(), Error> {
        let mut client_handle_guard = self.client_handle.write().await;
        let mut task_guard = self.event_loop_task.write().await;

        if client_handle_guard.is_some() || task_guard.is_some() {
            log::warn!("Already connected or event loop task running.");
            return Ok(());
        }

        // Note: MqttOptions are already configured in new().
        // Re-creating or altering them here for host/port might be redundant if new() sets them correctly.
        // Assuming self.mqtt_options is the source of truth for connection params.
        // However, rumqttc::AsyncClient::new needs them directly.
        // Let's ensure host and port from args are used for this specific connection attempt.
        let mut connection_options = self.mqtt_options.clone();
        connection_options.set_broker_addr(format!("{}:{}", host, port));


        // Attempt to establish the connection
        let (new_async_client, mut event_loop) =
            match rumqttc::AsyncClient::new(connection_options.clone(), 100) {
                Ok(client_tuple) => client_tuple,
                Err(e) => {
                    return Err(Error::ConnectionError {
                        host: host.to_string(),
                        port,
                        details: format!("Failed to create AsyncClient: {}", e),
                    });
                }
            };

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
                        if ack.code == rumqttc::ConnectReturnCode::Success {
                            log::info!("MQTT Connected: {:?}", ack.code);
                        } else {
                            log::error!("MQTT Connection failed: {:?}", ack.code);
                            // This error occurs within the event loop, difficult to propagate to connect() caller directly.
                            // For robust connect error handling, connect() might need to await this ack.
                            // For now, logging and the loop continues/reconnects or client is dropped.
                        }
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
                            // TODO: Full logic for bound operation handling, including sending response
                            // For now, just logging and continuing
                            drop(op_bindings_guard); // release lock before potentially long callback
                            // let result = op_callback(params_from_payload); // This needs payload parsing
                            // Then publish result to reply_to topic from payload
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
                        log::error!("MQTT Event loop error: {}. Attempting to handle.", e);
                        // This error might indicate a disconnect or other critical issue.
                        // Depending on the error, might need to signal wider system or attempt reconnect.
                        // For now, just logging and sleeping.
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
        *task_guard = Some(spawned_task);
        // Note: True connection status depends on ConnAck in event_loop.
        // This method completes before ConnAck is guaranteed. For robust connect,
        // a mechanism to await ConnAck or a status check would be needed.
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), Error> {
        let mut client_handle_guard = self.client_handle.write().await;
        let mut task_guard = self.event_loop_task.write().await;
        if let Some(client) = client_handle_guard.take() {
            if let Err(e) = client.disconnect().await {
                log::error!("MQTT client disconnect error: {}", e);
                // Even if disconnect fails, proceed to abort task
                // Return a communication error
                if let Some(task) = task_guard.take() {
                    task.abort();
                    log::info!("MQTT event loop task signalled to abort due to disconnect error.");
                }
                return Err(Error::CommunicationError(format!(
                    "MQTT client disconnect error: {}",
                    e
                )));
            } else {
                log::info!("MQTT client disconnected successfully.");
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
                .publish(
                    topic.clone(),
                    QoS::AtLeastOnce,
                    retain,
                    payload.into_bytes(),
                )
                .await
                .map_err(|e| Error::PublishFailed {
                    topic,
                    details: e.to_string(),
                })
        } else {
            Err(Error::CommunicationError("Not connected".to_string()))
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
                .map_err(|e| Error::SubscriptionFailed {
                    topic: topic.clone(),
                    details: e.to_string(),
                })?;
            self.property_subscriptions
                .write()
                .await
                .insert(topic, callback);
            Ok(())
        } else {
            Err(Error::CommunicationError("Not connected".to_string()))
        }
    }

    async fn unsubscribe(&self, topic: &str) -> Result<(), Error> {
        log::debug!("Unsubscribing from topic: {}", topic);
        let client_guard = self.client_handle.read().await;
        if let Some(client) = client_guard.as_ref() {
            client.unsubscribe(topic).await.map_err(|e| {
                Error::CommunicationError(format!(
                    "MQTT unsubscribe error for topic {}: {}",
                    topic, e
                ))
            })?;
        } else {
            // Not an error if not connected, just can't send unsubscribe to broker.
            log::warn!("Attempted to unsubscribe from broker while not connected (topic: {}). Will only clear local callbacks.", topic);
        }

        // Always remove local subscriptions regardless of connection state
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
                .map_err(|e| Error::SubscriptionFailed {
                    topic: topic.clone(),
                    details: e.to_string(),
                })?;
            self.event_subscriptions
                .write()
                .await
                .insert(topic, callback);
            Ok(())
        } else {
            Err(Error::CommunicationError("Not connected".to_string()))
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
        let client = client_guard.as_ref().ok_or_else(|| {
            Error::CommunicationError("Not connected".to_string())
        })?;

        let request_id = uuid::Uuid::new_v4().to_string();
        let request_publish_topic = format!("{}/request", operation_topic);
        let reply_to_topic = format!("{}/{}", self.client_response_base_topic, request_id);

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.request_map
            .write()
            .await
            .insert(reply_to_topic.clone(), tx);

        // Subscribe to the reply topic
        client
            .subscribe(reply_to_topic.clone(), QoS::AtLeastOnce)
            .await
            .map_err(|e| {
                // Cleanup request_map if subscribe fails
                let reply_topic_clone = reply_to_topic.clone();
                let req_map_clone = self.request_map.clone();
                tokio::spawn(async move {
                    req_map_clone.write().await.remove(&reply_topic_clone);
                });
                Error::SubscriptionFailed {
                    topic: reply_to_topic.clone(),
                    details: e.to_string(),
                }
            })?;

        let req_payload_json =
            serde_json::json!({"params": params, "reply_to": reply_to_topic.clone()});
        let req_payload_str = serde_json::to_string(&req_payload_json)?; // Can return JsonError

        // Publish the request
        client
            .publish(
                request_publish_topic.clone(),
                QoS::AtLeastOnce,
                false,
                req_payload_str.into_bytes(),
            )
            .await
            .map_err(|e| {
                // Cleanup request_map if publish fails
                let reply_topic_clone = reply_to_topic.clone();
                let req_map_clone = self.request_map.clone();
                tokio::spawn(async move {
                    req_map_clone.write().await.remove(&reply_topic_clone);
                });
                Error::PublishFailed {
                    topic: request_publish_topic.clone(),
                    details: e.to_string(),
                }
            })?;

        log::debug!(
            "Op invoked on {}, reply on {}",
            request_publish_topic,
            reply_to_topic
        );

        // Wait for the response with timeout
        let result = match tokio::time::timeout(Duration::from_millis(timeout_ms), rx).await {
            Ok(Ok(value)) => Ok(value), // Successfully received response
            Ok(Err(_)) => {
                // Sender was dropped, meaning response processing failed or channel closed prematurely
                self.request_map.write().await.remove(&reply_to_topic); // Ensure cleanup
                Err(Error::CommunicationError(
                    "Operation call failed: response channel error before value received".to_string(),
                ))
            }
            Err(_) => {
                // Timeout
                self.request_map.write().await.remove(&reply_to_topic); // Ensure cleanup
                Err(Error::OperationTimeoutError {
                    operation: operation_topic.clone(),
                    timeout_ms,
                })
            }
        };

        // Unsubscribe from the reply topic
        // This should happen regardless of the outcome of the operation invocation if subscribe was successful
        if let Err(e) = client.unsubscribe(reply_to_topic.clone()).await {
            log::warn!(
                "Failed to unsubscribe from response topic {}: {}. Potential for orphaned subscriptions.",
                reply_to_topic,
                e
            );
            // Not returning this error as the primary error of the operation, but logging it is important.
        }

        // If result was Ok, or if it was a response channel error (meaning we got something or sender dropped),
        // the entry in request_map should have been removed. This check is slightly redundant now with cleanup in error paths.
        if result.is_ok() || matches!(result, Err(Error::CommunicationError(_))) {
             // For OperationTimeoutError, it's already removed.
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
        let client = client_guard.as_ref().ok_or_else(|| {
            Error::CommunicationError("Not connected".to_string())
        })?;

        let request_topic = format!("{}/request", topic);
        client
            .subscribe(request_topic.clone(), QoS::AtLeastOnce)
            .await
            .map_err(|e| Error::SubscriptionFailed {
                topic: request_topic.clone(),
                details: e.to_string(),
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
        // This should ideally perform a discovery mechanism, e.g., using retained messages or a discovery topic.
        Err(Error::CommunicationError(
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
            if parts.len() == 4 && parts[3] == "_meta" { // e.g. <namespace>/<asset_name>/<submodel_name>/_meta
                let sm_name = parts[2].to_string();
                match serde_json::from_str::<Value>(&payload_str) {
                    Ok(meta_value) => {
                        let metas_map_clone = internal_callback_collected_metas.clone();
                        // Spawning a task to avoid blocking the MQTT event loop
                        tokio::spawn(async move {
                            metas_map_clone.write().await.insert(sm_name, meta_value);
                        });
                    }
                    Err(e) => {
                        log::error!("query_submodels_for_asset (callback): Failed to deserialize _meta for topic '{}'. Error: {}",topic, e);
                    }
                }
            } else {
                log::warn!("query_submodels_for_asset (callback): Msg on meta wildcard with unexpected topic structure: {}",topic);
            }
        });

        // Subscribe to the wildcard topic
        self.subscribe(wildcard_topic.clone(), internal_callback).await?;
        log::debug!("query_submodels_for_asset: Subscribed to {}", wildcard_topic);

        // Wait for a period to collect responses. This is a common but somewhat fragile pattern for discovery.
        // A more robust solution might involve a specific discovery protocol or awaiting a known number of responses if possible.
        tokio::time::sleep(Duration::from_millis(1500)).await; // TODO: Make timeout configurable or use a better discovery mechanism

        // Unsubscribe from the wildcard topic
        if let Err(e) = self.unsubscribe(&wildcard_topic).await {
            log::error!(
                "query_submodels_for_asset: Error during unsubscribe from {}: {}. Proceeding with collected data.",
                wildcard_topic, e
            );
            // Don't return error here, try to return what was collected.
        } else {
            log::debug!("query_submodels_for_asset: Unsubscribed from {}", wildcard_topic);
        }

        let final_metas = collected_metas.read().await.clone();
        if final_metas.is_empty() {
            // This might not be an error condition always, could be a valid state.
            // Using DiscoveryNoData to indicate that query executed but found nothing.
            log::warn!(
                "query_submodels_for_asset: No submodels found for asset '{}/{}' on topic '{}'. This might be expected or indicate an issue.",
                namespace, asset_name, wildcard_topic
            );
             return Err(Error::DiscoveryNoData(format!("asset '{}/{}'", namespace, asset_name)));
        } else {
            log::info!(
                "query_submodels_for_asset: Collected {} submodel(s) for asset '{}/{}'",
                final_metas.len(), namespace, asset_name
            );
        }
        Ok(final_metas)
    }

    async fn query_asset_children(
        &self,
        parent_namespace: &str,
        parent_asset_name: &str,
    ) -> Result<Vec<crate::ChildAssetInfo>, Error> {
        let discovery_topic = "+/+/_relations/belongs_to";
        log::info!(
            "Querying child assets for '{}/{}' using wildcard topic: {}",
            parent_namespace,
            parent_asset_name,
            discovery_topic
        );

        let collected_children = Arc::new(TokioRwLock::new(Vec::new()));
        let internal_callback_collected_children = collected_children.clone();

        let expected_parent_ns = parent_namespace.to_string();
        let expected_parent_name = parent_asset_name.to_string();

        let internal_callback = Box::new(move |topic: String, payload_str: String| {
            log::debug!(
                "query_asset_children (callback): Received msg on topic: {}",
                topic
            );
            match serde_json::from_str::<Value>(&payload_str) {
                Ok(payload_value) => {
                    if let (Some(ns_val), Some(name_val)) = (
                        payload_value.get("namespace"),
                        payload_value.get("asset_name"),
                    ) {
                        if let (Some(ns_str), Some(name_str)) =
                            (ns_val.as_str(), name_val.as_str())
                        {
                            if ns_str == expected_parent_ns && name_str == expected_parent_name {
                                // Payload matches the parent we are looking for.
                                // Now extract child info from the topic: <child_ns>/<child_name>/_relations/belongs_to
                                let parts: Vec<&str> = topic.split('/').collect();
                                if parts.len() == 4
                                    && parts[2] == "_relations"
                                    && parts[3] == "belongs_to"
                                {
                                    let child_info = crate::ChildAssetInfo {
                                        namespace: parts[0].to_string(),
                                        name: parts[1].to_string(),
                                    };
                                    log::debug!("Found child asset: {:?}", child_info);
                                    let children_clone =
                                        internal_callback_collected_children.clone();
                                    tokio::spawn(async move {
                                        children_clone.write().await.push(child_info);
                                    });
                                } else {
                                    log::warn!("query_asset_children (callback): Topic '{}' matched parent but had unexpected structure.", topic);
                                }
                            }
                        } else {
                            log::debug!("query_asset_children (callback): Payload 'namespace' or 'asset_name' not strings for topic '{}'", topic);
                        }
                    } else {
                        log::debug!("query_asset_children (callback): Payload missing 'namespace' or 'asset_name' for topic '{}'", topic);
                    }
                }
                Err(e) => {
                    log::error!("query_asset_children (callback): Failed to deserialize belongs_to payload for topic '{}'. Error: {}",topic, e);
                }
            }
        });

        self.subscribe(discovery_topic.to_string(), internal_callback)
            .await?;
        log::debug!(
            "query_asset_children: Subscribed to {}",
            discovery_topic
        );

        // TODO: Make timeout configurable. Using similar timeout as query_submodels_for_asset for now.
        tokio::time::sleep(Duration::from_millis(1500)).await;

        if let Err(e) = self.unsubscribe(discovery_topic).await {
            log::error!(
                "query_asset_children: Error during unsubscribe from {}: {}. Proceeding with collected data.",
                discovery_topic, e
            );
        } else {
            log::debug!(
                "query_asset_children: Unsubscribed from {}",
                discovery_topic
            );
        }

        let final_children = collected_children.read().await.clone();
        if final_children.is_empty() {
            log::warn!(
                "query_asset_children: No children found for asset '{}/{}' on topic '{}'",
                parent_namespace,
                parent_asset_name,
                discovery_topic
            );
            // It's not necessarily an error if no children are found.
            // Depending on strictness, one might return Error::DiscoveryNoData here.
            // For now, returning an empty Vec is consistent with finding "no data".
        } else {
            log::info!(
                "query_asset_children: Collected {} child(ren) for asset '{}/{}'",
                final_children.len(),
                parent_namespace,
                parent_asset_name
            );
        }
        Ok(final_children)
    }
}
