use assets2036_rs::*;
use async_trait::async_trait;

use assets2036_rs::{
    validate_value, CommunicationClient, Error, PropertyDefinition, PropertyType,
};
use serde_json::{json, Value};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::{boxed::Box, collections::HashMap};
use tokio::sync::Mutex as TokioMutex;
use tokio::time::Duration;

type PropertyCallbackForMock = Box<dyn Fn(String, String) + Send + Sync + 'static>;
type EventCallbackForMock = Box<dyn Fn(HashMap<String, Value>, i64) + Send + Sync + 'static>;
type OperationCallback = Box<dyn Fn(Value) -> Result<Value, Error> + Send + Sync + 'static>;
type ConditionalPublishErrorFn = Box<dyn Fn(&str) -> Option<Error> + Send + Sync>;


#[derive(Debug, Default, Clone)]
pub struct RecordedPublish {
    pub topic: String,
    pub payload: String,
    pub retain: bool,
}

#[derive(Debug, Default, Clone)]
pub struct RecordedSubscription {
    pub topic: String,
}

#[derive(Debug, Default, Clone)]
pub struct RecordedEventSubscription {
    pub topic: String,
}

#[derive(Debug, Default, Clone)]
pub struct RecordedOperationBinding {
    pub topic: String,
}

#[derive(Debug, Default, Clone)]
pub struct RecordedOperationInvocation {
    pub topic: String,
    pub params: Value,
    pub timeout_ms: u64,
}

#[derive(Default)]
pub struct MockCommunicationClient {
    connect_count: TokioMutex<usize>,
    disconnect_count: TokioMutex<usize>,
    publishes: TokioMutex<Vec<RecordedPublish>>,
    subscriptions: TokioMutex<Vec<RecordedSubscription>>,
    event_subscriptions: TokioMutex<Vec<RecordedEventSubscription>>,
    unsubscriptions: TokioMutex<Vec<String>>,
    operation_bindings: TokioMutex<Vec<RecordedOperationBinding>>,
    operation_invocations: TokioMutex<Vec<RecordedOperationInvocation>>,

    invoke_operation_results: TokioMutex<HashMap<String, Result<Value, Error>>>,
    query_submodels_responses: TokioMutex<Option<Result<HashMap<String, Value>, Error>>>,
    query_asset_names_responses: TokioMutex<Option<Result<Vec<String>, Error>>>,
    query_asset_children_responses: TokioMutex<Option<Result<Vec<ChildAssetInfo>, Error>>>,

    next_publish_error: TokioMutex<Option<Error>>,
    next_subscribe_error: TokioMutex<Option<Error>>,
    next_bind_operation_error: TokioMutex<Option<Error>>,
    conditional_publish_error: TokioMutex<Option<ConditionalPublishErrorFn>>,


    pub property_callbacks: TokioMutex<HashMap<String, PropertyCallbackForMock>>,
    pub event_callbacks: TokioMutex<HashMap<String, EventCallbackForMock>>,
    pub bound_op_callbacks: TokioMutex<HashMap<String, OperationCallback>>,
}

impl fmt::Debug for MockCommunicationClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockCommunicationClient")
            .field("connect_count", &self.connect_count)
            .field("disconnect_count", &self.disconnect_count)
            .field("publishes_len", &self.publishes.blocking_lock().len())
            .field("subscriptions_len", &self.subscriptions.blocking_lock().len())
            .field("event_subscriptions_len", &self.event_subscriptions.blocking_lock().len())
            .field("unsubscriptions_len", &self.unsubscriptions.blocking_lock().len())
            .field("operation_bindings_len", &self.operation_bindings.blocking_lock().len())
            .field("operation_invocations_len", &self.operation_invocations.blocking_lock().len())
            .finish_non_exhaustive()
    }
}

impl MockCommunicationClient {
    pub fn new() -> Self {
        Default::default()
    }

    pub async fn set_invoke_operation_response(&self, topic: &str, response: Result<Value, Error>) {
        self.invoke_operation_results
            .lock()
            .await
            .insert(topic.to_string(), response);
    }

    pub async fn set_query_submodels_response(&self, response: Result<HashMap<String, Value>, Error>) {
        *self.query_submodels_responses.lock().await = Some(response);
    }

    pub async fn set_query_asset_names_response(&self, response: Result<Vec<String>, Error>) {
        *self.query_asset_names_responses.lock().await = Some(response);
    }

    pub async fn set_query_asset_children_response(&self, response: Result<Vec<ChildAssetInfo>, Error>) {
        *self.query_asset_children_responses.lock().await = Some(response);
    }

    pub async fn set_publish_should_fail(&self, error: Option<Error>) {
        *self.next_publish_error.lock().await = error;
    }

    pub async fn set_conditional_publish_error(&self, checker: Option<ConditionalPublishErrorFn>) {
        *self.conditional_publish_error.lock().await = checker;
    }

    pub async fn set_subscribe_should_fail(&self, error: Option<Error>) {
        *self.next_subscribe_error.lock().await = error;
    }

    pub async fn set_bind_operation_should_fail(&self, error: Option<Error>) {
        *self.next_bind_operation_error.lock().await = error;
    }

    pub async fn get_publishes(&self) -> Vec<RecordedPublish> {
        self.publishes.lock().await.clone()
    }
    pub async fn get_subscriptions(&self) -> Vec<RecordedSubscription> {
        self.subscriptions.lock().await.clone()
    }
     pub async fn get_event_subscriptions(&self) -> Vec<RecordedEventSubscription> {
        self.event_subscriptions.lock().await.clone()
    }
    pub async fn get_unsubscriptions(&self) -> Vec<String> {
        self.unsubscriptions.lock().await.clone()
    }
    pub async fn get_operation_bindings(&self) -> Vec<RecordedOperationBinding> {
        self.operation_bindings.lock().await.clone()
    }
    pub async fn get_operation_invocations(&self) -> Vec<RecordedOperationInvocation> {
        self.operation_invocations.lock().await.clone()
    }
    pub async fn get_connect_count(&self) -> usize {
        *self.connect_count.lock().await
    }

    pub async fn simulate_property_update(&self, topic: &str, payload: String) {
        if let Some(callback) = self.property_callbacks.lock().await.get(topic) {
            callback(topic.to_string(), payload);
        }
    }
    pub async fn simulate_event_received( &self, topic: &str, params: HashMap<String, Value>, timestamp: i64,) {
        if let Some(callback) = self.event_callbacks.lock().await.get(topic) {
            callback(params, timestamp);
        }
    }
    pub async fn simulate_operation_call( &self, topic: &str, params: Value, ) -> Option<Result<Value, Error>> {
        let request_topic = format!("{}/request", topic);
        match self.bound_op_callbacks.lock().await.get(&request_topic) {
            Some(cb) => Some(cb(params)),
            None => None,
        }
    }
}

#[async_trait]
impl CommunicationClient for MockCommunicationClient {
    async fn connect( &self, _host: &str, _port: u16, _namespace: &str, _endpoint_name: &str, ) -> Result<(), Error> {
        *self.connect_count.lock().await += 1;
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), Error> {
        *self.disconnect_count.lock().await += 1;
        Ok(())
    }

    async fn publish(&self, topic: String, payload: String, retain: bool) -> Result<(), Error> {
        if let Some(err) = self.next_publish_error.lock().await.take() {
            return Err(err);
        }
        if let Some(checker) = self.conditional_publish_error.lock().await.as_ref() {
            if let Some(err) = checker(&topic) {
                // Note: We need to decide if conditional errors are also one-time.
                // For now, let's make them persistent unless explicitly cleared by setting checker to None.
                // If one-time is needed, the checker itself or this code needs to manage that state.
                return Err(err);
            }
        }
        self.publishes.lock().await.push(RecordedPublish {
            topic,
            payload,
            retain,
        });
        Ok(())
    }

    async fn subscribe( &self, topic: String, callback: PropertyCallbackForMock, ) -> Result<(), Error> {
        if let Some(err) = self.next_subscribe_error.lock().await.take() {
            return Err(err);
        }
        self.subscriptions.lock().await.push(RecordedSubscription {
            topic: topic.clone(),
        });
        self.property_callbacks.lock().await.insert(topic, callback);
        Ok(())
    }

    async fn unsubscribe(&self, topic: &str) -> Result<(), Error> {
        self.unsubscriptions.lock().await.push(topic.to_string());
        self.property_callbacks.lock().await.remove(topic);
        self.event_callbacks.lock().await.remove(topic);
        Ok(())
    }

    async fn subscribe_event( &self, topic: String, callback: EventCallbackForMock, ) -> Result<(), Error> {
        if let Some(err) = self.next_subscribe_error.lock().await.take() {
            return Err(err);
        }
        self.event_subscriptions
            .lock()
            .await
            .push(RecordedEventSubscription {
                topic: topic.clone(),
            });
        self.event_callbacks.lock().await.insert(topic, callback);
        Ok(())
    }

    async fn trigger_event( &self, topic: String, params: HashMap<String, Value>, ) -> Result<(), Error> {
        // This will internally call self.publish, which now has conditional error logic
        let now_ms = chrono::Utc::now().timestamp_millis();
        let event_payload = serde_json::json!({
            "timestamp": now_ms,
            "parameters": params
        });
        let payload_str = serde_json::to_string(&event_payload).map_err(|e| Error::JsonError(e))?;
        self.publish(topic, payload_str, false).await
    }

    async fn invoke_operation( &self, topic: String, params: Value, timeout_ms: u64, ) -> Result<Value, Error> {
        self.operation_invocations
            .lock()
            .await
            .push(RecordedOperationInvocation {
                topic: topic.clone(),
                params,
                timeout_ms,
            });
        match self.invoke_operation_results.lock().await.remove(&topic) {
            Some(res) => res,
            None => Ok(Value::Null),
        }
    }

    async fn bind_operation( &self, topic: String, callback: OperationCallback, ) -> Result<(), Error> {
        if let Some(err) = self.next_bind_operation_error.lock().await.take() {
            return Err(err);
        }
        let request_topic = format!("{}/request", topic);
        self.operation_bindings
            .lock()
            .await
            .push(RecordedOperationBinding {
                topic: topic.clone(),
            });
        self.bound_op_callbacks
            .lock()
            .await
            .insert(request_topic, callback);
        Ok(())
    }

    async fn query_submodels_for_asset( &self, _namespace: &str, _asset_name: &str, ) -> Result<HashMap<String, Value>, Error> {
        match self.query_submodels_responses.lock().await.take() {
            Some(res) => res,
            None => Ok(HashMap::new()),
        }
    }

    async fn query_asset_names( &self, _namespace: Option<&str>, _submodel_names: &[&str], ) -> Result<Vec<String>, Error> {
        match self.query_asset_names_responses.lock().await.take() {
            Some(res) => res,
            None => Ok(Vec::new()),
        }
    }

    async fn query_asset_children( &self, _parent_namespace: &str, _parent_asset_name: &str, ) -> Result<Vec<ChildAssetInfo>, Error> {
        match self.query_asset_children_responses.lock().await.take() {
            Some(res) => res,
            None => Ok(Vec::new()),
        }
    }
}

// --- Tests for Property::delete ---

#[tokio::test]
async fn test_property_delete_publishes_empty_retained_and_clears_local() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let prop_def_json = r#"{"type": "string"}"#;
    let prop_def: PropertyDefinition = serde_json::from_str(prop_def_json).unwrap();

    let parent_topic = "test_ns/test_asset/test_submodel_for_delete";
    let prop_name = "myDeletableProp";
    let full_topic = format!("{}/{}", parent_topic, prop_name);

    let property = Property::new(
        prop_name,
        prop_def,
        mock_client.clone() as Arc<dyn CommunicationClient>,
        parent_topic,
    );

    let initial_value = json!("Initial Value");
    let set_result = property.set_value(initial_value.clone()).await;
    assert!(set_result.is_ok(), "Failed to set initial property value");
    assert_eq!(property.get_value().await, initial_value, "Initial value not set correctly");

    mock_client.publishes.lock().await.clear();

    let delete_result = property.delete().await;
    assert!(delete_result.is_ok(), "property.delete() failed: {:?}", delete_result.err());

    assert_eq!(property.get_value().await, Value::Null, "Local property value should be Null after delete");

    let publishes = mock_client.get_publishes().await;
    assert_eq!(publishes.len(), 1, "Expected exactly one publish for delete operation");

    let publish_action = &publishes[0];
    assert_eq!(publish_action.topic, full_topic, "Publish topic mismatch");
    assert_eq!(publish_action.payload, "", "Payload for delete should be an empty string");
    assert_eq!(publish_action.retain, true, "Retain flag for delete should be true");
}

#[tokio::test]
async fn test_property_delete_read_only_fails() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let prop_def_json = r#"{"type": "string", "readOnly": true}"#;
    let prop_def: PropertyDefinition = serde_json::from_str(prop_def_json).unwrap();

    let parent_topic = "test_ns/test_asset/test_submodel_for_delete_ro";
    let prop_name = "myReadOnlyProp";

    let property = Property::new(
        prop_name,
        prop_def,
        mock_client.clone() as Arc<dyn CommunicationClient>,
        parent_topic,
    );

    let delete_result = property.delete().await;
    assert!(delete_result.is_err(), "Expected delete on read-only property to fail");

    match delete_result.err().unwrap() {
        Error::NotWritable => { }
        e => panic!("Expected Error::NotWritable, but got {:?}", e),
    }

    let publishes = mock_client.get_publishes().await;
    assert!(publishes.is_empty(), "No publish should occur when trying to delete a read-only property");

    assert_eq!(property.get_value().await, Value::Null, "Read-only property value should remain as it was (Null by default here)");
}

// --- Tests for Health Monitor ---

#[tokio::test]
async fn test_health_monitor_updates_healthy_property_and_shuts_down() {
    let mock_comm_client = Arc::new(MockCommunicationClient::new());
    let mut asset_manager = AssetManager::new(
        "localhost",
        1883,
        "test_health_ns",
        "test_health_mgr_ep",
    );

    match asset_manager.connect().await {
        Ok(_) => log::info!("AssetManager connected for health test."),
        Err(e) => {
            log::warn!("Connection failed during test setup: {:?}. This test might not be able to verify 'healthy' property updates via AssetManager's comm_client.", e);
        }
    }

    let healthy_prop_for_task = asset_manager
        .endpoint_asset
        .as_ref()
        .and_then(|asset| asset.get_submodel("_endpoint"))
        .and_then(|sm| sm.get_property("healthy"));

    if healthy_prop_for_task.is_none() {
        log::warn!("Could not get 'healthy' property from AssetManager's endpoint. Test will skip AM integration part.");
        let healthy_prop_def: PropertyDefinition = serde_json::from_str(r#"{"type": "boolean"}"#).unwrap();
        let _mock_healthy_prop = Arc::new(Property::new("healthy", healthy_prop_def, mock_comm_client.clone(), "test_health_ns/test_health_mgr_ep/_endpoint"));
        assert!(healthy_prop_for_task.is_some(), "AssetManager's _endpoint 'healthy' property not found after connect. Ensure AM uses the mock client.");
    }

    let callback_execution_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let make_healthy = Arc::new(std::sync::atomic::AtomicBool::new(true));

    let cb_count_clone = callback_execution_count.clone();
    let make_healthy_clone = make_healthy.clone();

    asset_manager.set_healthy_callback(
        move || {
            cb_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            make_healthy_clone.load(std::sync::atomic::Ordering::SeqCst)
        },
        1,
        false,
    );

    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;

    let count = callback_execution_count.load(std::sync::atomic::Ordering::SeqCst);
    assert!(count >= 2, "Callback should have been executed at least twice. Count: {}", count);

    let publishes = mock_comm_client.get_publishes().await;
    let healthy_publishes: Vec<_> = publishes.iter().filter(|p| p.topic.ends_with("_endpoint/healthy")).collect();

    assert!(!healthy_publishes.is_empty(), "Healthy property should have been published at least once by the monitor. Publishes: {:?}", publishes);
    assert_eq!(healthy_publishes.first().unwrap().payload, "true");

    make_healthy.store(false, std::sync::atomic::Ordering::SeqCst);
    let current_pub_count = healthy_publishes.len();

    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    let publishes_after_change = mock_comm_client.get_publishes().await;
    let healthy_publishes_after_change: Vec<_> = publishes_after_change.iter().filter(|p| p.topic.ends_with("_endpoint/healthy")).collect();

    assert!(healthy_publishes_after_change.len() > current_pub_count, "Healthy property should have been published again after callback change. Prev count: {}, New count: {}", current_pub_count, healthy_publishes_after_change.len());
    assert_eq!(healthy_publishes_after_change.last().unwrap().payload, "false", "Healthy property should now be false. Last publish: {:?}", healthy_publishes_after_change.last());

    let (tx, mut rx_signal) = tokio::sync::mpsc::channel(1);
    asset_manager.set_healthy_callback(
        move || {
            let _ = tx.try_send(());
            true
        },
        10,
        false,
    );
    log::info!("Set new dummy callback to stop previous monitor.");

    let publishes_before_new_cb_fires_check = mock_comm_client.get_publishes().await;
    let healthy_publishes_len_at_stop_signal = publishes_before_new_cb_fires_check.iter().filter(|p| p.topic.ends_with("_endpoint/healthy")).count();

    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;

    let publishes_after_stop_wait = mock_comm_client.get_publishes().await;
    let healthy_publishes_after_stop_wait_count = publishes_after_stop_wait.iter().filter(|p| p.topic.ends_with("_endpoint/healthy")).count();

    if let Ok(_) = rx_signal.try_recv() {
         panic!("New health callback with 10s interval should not have executed yet.");
    }

    assert_eq!(healthy_publishes_after_stop_wait_count, healthy_publishes_len_at_stop_signal,
        "No new 'healthy' state should have been published by the old monitor. Count before: {}, Count after: {}",
        healthy_publishes_len_at_stop_signal, healthy_publishes_after_stop_wait_count
    );

    asset_manager.disconnect().await.unwrap();
    log::info!("AssetManager disconnected, stopping the dummy health monitor.");

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
}

// --- Tests for AssetManager::create_asset with URL loading ---

#[tokio::test]
async fn test_create_asset_with_file_url_submodel_success() {
    let mock_comm_client = Arc::new(MockCommunicationClient::new());
    let mut asset_manager = AssetManager::new(
        "localhost",
        1883,
        "test_file_url_ns",
        "file_url_mgr_ep"
    );
    asset_manager.connect().await.expect("AssetManager connect failed");

    let mut path = std::env::current_dir().unwrap();
    path.push("tests");
    path.push("test_submodels");
    path.push("simple_prop_sm.json");

    let file_url = url::Url::from_file_path(&path)
        .expect("Failed to create file URL")
        .to_string();

    let asset_result = asset_manager
        .create_asset(
            "TestAssetFile",
            vec![file_url.clone()],
            Mode::Owner,
        )
        .await;

    assert!(asset_result.is_ok(), "create_asset with file URL failed: {:?}", asset_result.err());
    let asset = asset_result.unwrap();
    assert_eq!(asset.name, "TestAssetFile");
    assert!(asset.get_submodel("SimplePropSM").is_some(), "Submodel 'SimplePropSM' should be implemented");
    assert!(asset.get_submodel("SimplePropSM").unwrap().get_property("status").is_some(), "Property 'status' should exist in 'SimplePropSM'");

    let publishes = mock_comm_client.get_publishes().await;
    let meta_publish = publishes.iter().find(|p|
        p.topic == "test_file_url_ns/TestAssetFile/SimplePropSM/_meta" && p.retain
    );
    assert!(meta_publish.is_some(), "_meta for SimplePropSM was not published. Publishes: {:?}", publishes);

    if let Some(publish) = meta_publish {
        let meta_payload: Value = serde_json::from_str(&publish.payload).unwrap();
        assert_eq!(meta_payload["submodel_definition"]["name"], "SimplePropSM");
        assert_eq!(meta_payload["submodel_url"], file_url);
    }
}

#[tokio::test]
async fn test_create_asset_with_file_url_not_found() {
    let mock_comm_client = Arc::new(MockCommunicationClient::new());
    let mut asset_manager = AssetManager::new(
        "localhost",
        1883,
        "test_file_not_found_ns",
        "file_not_found_mgr_ep"
    );
    asset_manager.connect().await.expect("AssetManager connect failed");

    let mut path = std::env::current_dir().unwrap();
    path.push("tests");
    path.push("test_submodels");
    path.push("non_existent_sm.json");

    let file_url = url::Url::from_file_path(&path)
        .expect("Failed to create file URL for non-existent file")
        .to_string();

    let asset_result = asset_manager
        .create_asset(
            "TestAssetFileNotFound",
            vec![file_url.clone()],
            Mode::Owner,
        )
        .await;

    assert!(asset_result.is_err(), "Expected create_asset to fail for non-existent file URL");
    match asset_result.err().unwrap() {
        Error::FileReadError { path: error_path, details } => {
            assert_eq!(error_path, path.to_string_lossy());
            log::debug!("FileReadError details: {}", details);
        }
        e => panic!("Expected FileReadError, got {:?}", e),
    }
}

#[tokio::test]
async fn test_create_asset_with_http_url_submodel_success() {
    let mock_comm_client = Arc::new(MockCommunicationClient::new());
    let mut asset_manager = AssetManager::new("localhost", 1883, "test_http_ns", "http_mgr_ep");
    asset_manager.connect().await.expect("AssetManager connect failed");

    let server = httpmock::MockServer::start();
    let submodel_json_content = r#"
        {
            "name": "HttpLoadedSM",
            "properties": { "data": {"type": "string"} }
        }
    "#;
    let sm_mock = server.mock(|when, then| {
        when.method(httpmock::MockHTTPMethod::GET).path("/submodels/HttpLoadedSM.json");
        then.status(200)
            .header("content-type", "application/json")
            .body(submodel_json_content);
    });

    let http_url = server.url("/submodels/HttpLoadedSM.json");

    let asset_result = asset_manager
        .create_asset("TestAssetHttp", vec![http_url.clone()], Mode::Owner)
        .await;

    sm_mock.assert();
    assert!(asset_result.is_ok(), "create_asset with HTTP URL failed: {:?}", asset_result.err());
    let asset = asset_result.unwrap();
    assert_eq!(asset.name, "TestAssetHttp");
    assert!(asset.get_submodel("HttpLoadedSM").is_some(), "Submodel 'HttpLoadedSM' should be implemented");
    assert!(asset.get_submodel("HttpLoadedSM").unwrap().get_property("data").is_some());

    let publishes = mock_comm_client.get_publishes().await;
    let meta_publish = publishes.iter().find(|p|
        p.topic == "test_http_ns/TestAssetHttp/HttpLoadedSM/_meta" && p.retain
    );
    assert!(meta_publish.is_some(), "_meta for HttpLoadedSM was not published. Publishes: {:?}", publishes);
    if let Some(publish) = meta_publish {
        let meta_payload: Value = serde_json::from_str(&publish.payload).unwrap();
        assert_eq!(meta_payload["submodel_definition"]["name"], "HttpLoadedSM");
        assert_eq!(meta_payload["submodel_url"], http_url);
    }
}

#[tokio::test]
async fn test_create_asset_with_http_url_server_error() {
    let mock_comm_client = Arc::new(MockCommunicationClient::new());
    let mut asset_manager = AssetManager::new("localhost", 1883, "test_http_err_ns", "http_err_mgr_ep");
    asset_manager.connect().await.expect("AssetManager connect failed");

    let server = httpmock::MockServer::start();
    let sm_mock = server.mock(|when, then| {
        when.method(httpmock::MockHTTPMethod::GET).path("/submodels/ErrorSM.json");
        then.status(500);
    });

    let http_url = server.url("/submodels/ErrorSM.json");

    let asset_result = asset_manager
        .create_asset("TestAssetHttpError", vec![http_url.clone()], Mode::Owner)
        .await;

    sm_mock.assert();
    assert!(asset_result.is_err(), "Expected create_asset to fail for HTTP 500 error");
    match asset_result.err().unwrap() {
        Error::HttpRequestError { url, details } => {
            assert_eq!(url, http_url);
            assert!(details.contains("status: 500"), "Error details mismatch: {}", details);
        }
        e => panic!("Expected HttpRequestError, got {:?}", e),
    }
}

#[tokio::test]
async fn test_create_asset_with_invalid_url() {
    let mock_comm_client = Arc::new(MockCommunicationClient::new());
    let mut asset_manager = AssetManager::new("localhost", 1883, "test_invalid_url_ns", "invalid_url_mgr_ep");
    asset_manager.connect().await.expect("AssetManager connect failed");

    let invalid_url = "http://invalid url with spaces.json";

    let asset_result = asset_manager
        .create_asset("TestAssetInvalidUrl", vec![invalid_url.to_string()], Mode::Owner)
        .await;

    assert!(asset_result.is_err(), "Expected create_asset to fail for invalid URL");
    match asset_result.err().unwrap() {
        Error::UrlParseError(details) => {
            assert!(details.contains("invalid character") || details.contains("relative URL without a base") || details.contains("empty host"), "Unexpected URL parse error details: {}", details);
        }
        e => panic!("Expected UrlParseError, got {:?}", e),
    }
}

#[tokio::test]
async fn test_create_asset_with_unsupported_scheme_url() {
    let mock_comm_client = Arc::new(MockCommunicationClient::new());
    let mut asset_manager = AssetManager::new("localhost", 1883, "test_unsupported_ns", "unsupported_mgr_ep");
    asset_manager.connect().await.expect("AssetManager connect failed");

    let unsupported_scheme_url = "ftp://example.com/submodel.json";

    let asset_result = asset_manager
        .create_asset("TestAssetUnsupportedScheme", vec![unsupported_scheme_url.to_string()], Mode::Owner)
        .await;

    assert!(asset_result.is_err(), "Expected create_asset to fail for unsupported URL scheme");
    match asset_result.err().unwrap() {
        Error::UrlParseError(details) => {
            assert!(details.contains("Unsupported URL scheme: ftp") || details.contains("unknown scheme"), "Error details mismatch: {}", details);
        }
        e => panic!("Expected UrlParseError for unsupported scheme, got {:?}", e),
    }
}

#[tokio::test]
async fn test_create_asset_with_mixed_sources_json_and_file_url() {
    let mock_comm_client = Arc::new(MockCommunicationClient::new());
    let mut asset_manager = AssetManager::new("localhost", 1883, "test_mixed_ns", "mixed_mgr_ep");
    asset_manager.connect().await.expect("AssetManager connect failed");

    let mut path = std::env::current_dir().unwrap();
    path.push("tests");
    path.push("test_submodels");
    path.push("simple_prop_sm.json");
    let file_url = url::Url::from_file_path(&path).unwrap().to_string();

    let direct_json_sm = r#"
        {
            "name": "DirectJsonSM",
            "properties": { "temperature": {"type": "number"} }
        }
    "#;

    let asset_result = asset_manager
        .create_asset(
            "TestAssetMixedSources",
            vec![direct_json_sm.to_string(), file_url.clone()],
            Mode::Owner,
        )
        .await;

    assert!(asset_result.is_ok(), "create_asset with mixed sources failed: {:?}", asset_result.err());
    let asset = asset_result.unwrap();
    assert_eq!(asset.name, "TestAssetMixedSources");

    assert!(asset.get_submodel("DirectJsonSM").is_some(), "Submodel 'DirectJsonSM' should be implemented");
    assert!(asset.get_submodel("DirectJsonSM").unwrap().get_property("temperature").is_some());

    assert!(asset.get_submodel("SimplePropSM").is_some(), "Submodel 'SimplePropSM' (from file) should be implemented");
    assert!(asset.get_submodel("SimplePropSM").unwrap().get_property("status").is_some());

    let publishes = mock_comm_client.get_publishes().await;
    let direct_json_meta = publishes.iter().find(|p| p.topic.ends_with("DirectJsonSM/_meta"));
    let file_url_meta = publishes.iter().find(|p| p.topic.ends_with("SimplePropSM/_meta"));

    assert!(direct_json_meta.is_some(), "_meta for DirectJsonSM not found.");
    assert!(file_url_meta.is_some(), "_meta for SimplePropSM not found.");

    if let Some(publish) = direct_json_meta {
        let meta_payload: Value = serde_json::from_str(&publish.payload).unwrap();
        assert_eq!(meta_payload["submodel_definition"]["name"], "DirectJsonSM");
        assert!(meta_payload["submodel_url"].as_str().unwrap_or_default().contains("localhost"));
    }
    if let Some(publish) = file_url_meta {
        let meta_payload: Value = serde_json::from_str(&publish.payload).unwrap();
        assert_eq!(meta_payload["submodel_definition"]["name"], "SimplePropSM");
        assert_eq!(meta_payload["submodel_url"], file_url);
    }
}

// --- Tests for Wait For Online ---

fn get_mock_endpoint_meta_response(namespace: &str, asset_name: &str, _online_status_init: Option<bool>) -> HashMap<String, Value> {
    let endpoint_sm_def: SubModelDefinition = serde_json::from_str(ENDPOINT_SUBMODEL_JSON).unwrap();
    let meta_value = json!({
        "source": format!("{}/{}", namespace, asset_name),
        "submodel_definition": endpoint_sm_def,
        "submodel_url": "file://localhost/_endpoint.json"
    });
    let mut submodels_map = HashMap::new();
    submodels_map.insert("_endpoint".to_string(), meta_value);
    submodels_map
}


#[tokio::test]
async fn test_create_asset_proxy_waits_for_online_success() {
    let mock_comm_client = Arc::new(MockCommunicationClient::new());
    let asset_manager = AssetManager::new(
        "localhost", 1883, "test_wait_ns", "wait_mgr_ep"
    );

    let proxy_ns = "proxy_ns_online";
    let proxy_name = "proxy_asset_online";
    let online_topic = format!("{}/{}/_endpoint/online", proxy_ns, proxy_name);

    mock_comm_client.set_query_submodels_response(Ok(get_mock_endpoint_meta_response(proxy_ns, proxy_name, None))).await;

    let client_clone_for_task = mock_comm_client.clone();
    let online_topic_clone_for_task = online_topic.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        log::info!("Test: Simulating property update for {} to true", online_topic_clone_for_task);
        client_clone_for_task.simulate_property_update(&online_topic_clone_for_task, "true".to_string()).await;
    });

    let create_proxy_future = asset_manager.create_asset_proxy(
        proxy_ns,
        proxy_name,
        Some(2)
    );

    match tokio::time::timeout(Duration::from_secs(3), create_proxy_future).await {
        Ok(Ok(_asset_proxy)) => {
            log::info!("Asset proxy created and online as expected.");
        }
        Ok(Err(e)) => {
            panic!("create_asset_proxy failed when it should have succeeded: {:?}", e);
        }
        Err(_) => {
            panic!("create_asset_proxy timed out externally (test logic error)");
        }
    }

    let subscriptions = mock_comm_client.get_subscriptions().await;
    assert!(subscriptions.iter().any(|s| s.topic == online_topic), "Should have subscribed to online topic. Subs: {:?}", subscriptions);
}


#[tokio::test]
async fn test_create_asset_proxy_online_timeout() {
    let mock_comm_client = Arc::new(MockCommunicationClient::new());
    let asset_manager = AssetManager::new(
        "localhost", 1883, "test_timeout_ns", "timeout_mgr_ep"
    );

    let proxy_ns = "proxy_ns_timeout";
    let proxy_name = "proxy_asset_timeout";

    mock_comm_client.set_query_submodels_response(Ok(get_mock_endpoint_meta_response(proxy_ns, proxy_name, Some(false)))).await;

    let result = asset_manager.create_asset_proxy(
        proxy_ns,
        proxy_name,
        Some(1)
    ).await;

    assert!(result.is_err(), "Expected create_asset_proxy to fail with timeout");
    match result.err().unwrap() {
        Error::AssetNotOnlineError { name, namespace, timeout_secs } => {
            assert_eq!(name, proxy_name);
            assert_eq!(namespace, proxy_ns);
            assert_eq!(timeout_secs, 1);
        }
        e => panic!("Expected AssetNotOnlineError, got {:?}", e),
    }
}

#[tokio::test]
async fn test_create_asset_proxy_already_online() {
    let mock_comm_client = Arc::new(MockCommunicationClient::new());
     let asset_manager = AssetManager::new(
        "localhost", 1883, "test_already_ns", "already_mgr_ep"
    );

    let proxy_ns = "proxy_ns_already";
    let proxy_name = "proxy_asset_already";
    let online_topic = format!("{}/{}/_endpoint/online", proxy_ns, proxy_name);

    mock_comm_client.set_query_submodels_response(Ok(get_mock_endpoint_meta_response(proxy_ns, proxy_name, None))).await;
    mock_comm_client.publish(online_topic.clone(), "true".to_string(), true).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;


    let result = asset_manager.create_asset_proxy(
        proxy_ns,
        proxy_name,
        Some(1)
    ).await;

    assert!(result.is_ok(), "create_asset_proxy failed when asset was already online: {:?}", result.err());

    let asset_proxy = result.unwrap();
    let _online_prop = asset_proxy.get_submodel("_endpoint").unwrap().get_property("online").unwrap();
}


#[tokio::test]
async fn test_create_asset_proxy_wait_for_online_zero_timeout_already_online() {
    let mock_comm_client = Arc::new(MockCommunicationClient::new());
    let asset_manager = AssetManager::new("localhost", 1883, "test_zero_ns", "zero_mgr_ep");

    let proxy_ns = "proxy_ns_zero_online";
    let proxy_name = "proxy_asset_zero_online";
    let online_topic = format!("{}/{}/_endpoint/online", proxy_ns, proxy_name);

    mock_comm_client.set_query_submodels_response(Ok(get_mock_endpoint_meta_response(proxy_ns, proxy_name, None))).await;
    mock_comm_client.publish(online_topic.clone(), "true".to_string(), true).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let result = asset_manager.create_asset_proxy(proxy_ns, proxy_name, Some(0)).await;
    assert!(result.is_ok(), "Expected success as asset is 'already online' for zero timeout check: {:?}", result.err());
}

#[tokio::test]
async fn test_create_asset_proxy_wait_for_online_zero_timeout_not_online() {
    let mock_comm_client = Arc::new(MockCommunicationClient::new());
    let asset_manager = AssetManager::new("localhost", 1883, "test_zero_ns", "zero_mgr_ep");

    let proxy_ns = "proxy_ns_zero_offline";
    let proxy_name = "proxy_asset_zero_offline";
     let online_topic = format!("{}/{}/_endpoint/online", proxy_ns, proxy_name);
    mock_comm_client.publish(online_topic.clone(), "false".to_string(), true).await.unwrap();


    mock_comm_client.set_query_submodels_response(Ok(get_mock_endpoint_meta_response(proxy_ns, proxy_name, None))).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let result = asset_manager.create_asset_proxy(proxy_ns, proxy_name, Some(0)).await;
    assert!(result.is_err(), "Expected AssetNotOnlineError for zero timeout check when not online");
    match result.err().unwrap() {
        Error::AssetNotOnlineError { name, namespace, timeout_secs } => {
            assert_eq!(name, proxy_name);
            assert_eq!(namespace, proxy_ns);
            assert_eq!(timeout_secs, 0);
        }
        e => panic!("Expected AssetNotOnlineError, got {:?}", e),
    }
}

// --- Test for Logging Handler ---

#[tokio::test]
async fn test_logging_handler_emits_event() {
    let _mock_comm_client = Arc::new(MockCommunicationClient::new());
    let mut _asset_manager = AssetManager::new(
        "localhost",
        1883,
        "test_log_ns",
        "test_log_mgr_ep",
    );

    let mock_event_comm_client = Arc::new(MockCommunicationClient::new());
    let event_def_json = r#"{
        "parameters": {
            "level": {"type": "string"},
            "message": {"type": "string"}
        }
    }"#;
    let event_def: EventDefinition = serde_json::from_str(event_def_json).unwrap();
    let log_event_mock = Arc::new(Event::new(
        "log_entry",
        event_def,
        mock_event_comm_client.clone() as Arc<dyn CommunicationClient>,
        "test_log_ns/test_log_mgr_ep/_endpoint",
    ));

    let logging_handler = RustLoggingHandler {
        log_event: log_event_mock.clone(),
    };

    if let Err(e) = log::set_boxed_logger(Box::new(logging_handler)) {
        eprintln!("Failed to set logger for test_logging_handler_emits_event, possibly already set: {:?}", e);
    }
    log::set_max_level(log::LevelFilter::Info);

    let test_message = format!("Test log from handler at {}", chrono::Utc::now().to_rfc3339());
    log::info!("{}", test_message);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let publishes = mock_event_comm_client.get_publishes().await;

    assert!(!publishes.is_empty(), "No log event was published.");

    let log_publish = publishes.iter().find(|p| p.topic == "test_log_ns/test_log_mgr_ep/_endpoint/log_entry");
    assert!(log_publish.is_some(), "Log entry event not found in publishes. Publishes: {:?}", publishes);

    let log_payload: Value = serde_json::from_str(&log_publish.unwrap().payload).unwrap();
    assert_eq!(log_payload["parameters"]["level"], "INFO");
    let published_message = log_payload["parameters"]["message"].as_str().unwrap();
    assert!(published_message.contains(&test_message), "Published message content mismatch. Expected to contain '{}', got '{}'", test_message, published_message);

    let current_file_name = std::file!();
    assert!(published_message.contains(current_file_name), "Published message does not contain the file name part of the target. Message: {}", published_message);
}

// --- New/Modified tests for error conditions ---

#[tokio::test]
async fn test_consumer_operation_invoke_timeout() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let op_def_json = r#"{"response": {"type": "string"}}"#;
    let op_def: OperationDefinition = serde_json::from_str(op_def_json).unwrap();

    let operation_base_topic = "remote/asset/sm_timeout";
    let operation_name = "timeoutOp";
    let full_operation_topic = format!("{}/{}", operation_base_topic, operation_name);

    let operation = Operation::new(
        operation_name,
        op_def,
        mock_client.clone() as Arc<dyn CommunicationClient>,
        operation_base_topic,
    );

    let expected_timeout_ms = DEFAULT_OPERATION_TIMEOUT_MS;
    mock_client
        .set_invoke_operation_response(
            &full_operation_topic,
            Err(Error::OperationTimeoutError {
                operation: full_operation_topic.clone(),
                timeout_ms: expected_timeout_ms
            }),
        )
        .await;

    let result = operation.invoke(json!({})).await;
    assert!(result.is_err(), "Expected invoke to return an error.");

    match result.err().unwrap() {
        Error::OperationTimeoutError { operation, timeout_ms } => {
            assert_eq!(operation, full_operation_topic);
            assert_eq!(timeout_ms, expected_timeout_ms);
        }
        e => panic!("Expected OperationTimeoutError, got {:?}", e),
    }
}

#[tokio::test]
async fn test_property_set_value_handles_publish_error() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let prop_def: PropertyDefinition = serde_json::from_str(r#"{"type": "string"}"#).unwrap();
    let parent_topic = "test/prop_pub_err";
    let prop_name = "status";
    let property_topic = format!("{}/{}", parent_topic, prop_name);
    let property = Property::new(prop_name, prop_def, mock_client.clone(), parent_topic);

    let publish_error = Error::PublishFailed {
        topic: property_topic.clone(),
        details: "Simulated publish failure".into()
    };
    mock_client.set_publish_should_fail(Some(publish_error)).await;

    let result = property.set_value(json!("new_value")).await;
    assert!(result.is_err());
    match result.err().unwrap() {
        Error::PublishFailed { topic, details } => {
            assert_eq!(topic, property_topic);
            assert_eq!(details, "Simulated publish failure");
        }
        e => panic!("Expected PublishFailed, got {:?}", e),
    }
}

#[tokio::test]
async fn test_event_trigger_handles_publish_error() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let event_def: EventDefinition = serde_json::from_str(r#"{}"#).unwrap();
    let parent_topic = "test/event_pub_err";
    let event_name = "someEvent";
    let event_topic = format!("{}/{}", parent_topic, event_name);
    let event = Event::new(event_name, event_def, mock_client.clone(), parent_topic);

    let publish_error = Error::PublishFailed {
        topic: event_topic.clone(),
        details: "Simulated event publish failure".into()
    };
    mock_client.set_publish_should_fail(Some(publish_error)).await;

    let result = event.trigger(json!({})).await;
    assert!(result.is_err());
    match result.err().unwrap() {
        Error::PublishFailed { topic, details } => {
            assert_eq!(topic, event_topic);
            assert_eq!(details, "Simulated event publish failure");
        }
        e => panic!("Expected PublishFailed, got {:?}", e),
    }
}

#[tokio::test]
async fn test_operation_bind_handles_subscription_error() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let op_def: OperationDefinition = serde_json::from_str(r#"{}"#).unwrap();
    let operation_base_topic = "test/op_bind_err_base";
    let operation_name = "myBindOp";
    let operation_request_topic = format!("{}/{}/request", operation_base_topic, operation_name);
    let operation = Operation::new(operation_name, op_def, mock_client.clone(), operation_base_topic);

    let sub_error = Error::SubscriptionFailed {
        topic: operation_request_topic.clone(),
        details: "Simulated bind subscribe failure".into()
    };
    mock_client.set_bind_operation_should_fail(Some(sub_error)).await;

    let result = operation.bind(Box::new(|_| Ok(Value::Null))).await;
    assert!(result.is_err());
    match result.err().unwrap() {
        Error::SubscriptionFailed { topic, details } => {
            assert_eq!(topic, operation_request_topic);
            assert_eq!(details, "Simulated bind subscribe failure");
        }
        e => panic!("Expected SubscriptionFailed, got {:?}", e),
    }
}

#[tokio::test]
async fn test_property_on_change_handles_subscription_error() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let prop_def: PropertyDefinition = serde_json::from_str(r#"{"type": "string"}"#).unwrap();
    let parent_topic = "test/prop_sub_err";
    let prop_name = "status";
    let property_topic = format!("{}/{}", parent_topic, prop_name);
    let property = Property::new(prop_name, prop_def, mock_client.clone(), parent_topic);

    let sub_error = Error::SubscriptionFailed {
        topic: property_topic.clone(),
        details: "Simulated property subscribe failure".into()
    };
    mock_client.set_subscribe_should_fail(Some(sub_error)).await;

    let result = property.on_change(Box::new(|_| {})).await;
    assert!(result.is_err());
    match result.err().unwrap() {
        Error::SubscriptionFailed { topic, details } => {
            assert_eq!(topic, property_topic);
            assert_eq!(details, "Simulated property subscribe failure");
        }
        e => panic!("Expected SubscriptionFailed, got {:?}", e),
    }
}

#[tokio::test]
async fn test_create_child_asset_fails_on_setting_belongs_to() {
    let mock_comm_client = Arc::new(MockCommunicationClient::new());
    // Assume AssetManager is created in a way that its internal comm_client is mock_comm_client
    // This is crucial for the _meta publishes of the parent and child to be captured by our mock.
    // For this test, the key is that the child asset uses the mock_comm_client.
    let parent_asset = Asset::new(
        "ParentForChildError",
        "test_rel_err_ns",
        Mode::Owner,
        "parent_ep_rel_err",
        mock_comm_client.clone(),
    );

    let child_name = "ChildAssetBelongsToFail";
    let expected_belongs_to_topic = format!(
        "{}/{}/{}/{}",
        parent_asset.namespace, child_name, "_relations", "belongs_to"
    );

    mock_comm_client.set_conditional_publish_error(Some(Box::new(move |topic: &str| {
        if topic == expected_belongs_to_topic {
            Some(Error::PublishFailed {
                topic: topic.to_string(),
                details: "Simulated failure to set belongs_to".into(),
            })
        } else {
            None // Allow other publishes (like _meta for _relations itself)
        }
    }))).await;

    let child_asset_result = parent_asset
        .create_child_asset(child_name, None, vec![]) // No extra submodels needed for this test
        .await;

    assert!(child_asset_result.is_err(), "Expected create_child_asset to fail.");

    match child_asset_result.err().unwrap() {
        Error::RelationError(details) => {
            assert!(details.contains("Failed to set belongs_to property"));
            assert!(details.contains("Simulated failure to set belongs_to"));
        }
        e => panic!("Expected RelationError, got {:?}", e),
    }

    // Verify that _meta for _relations of the child might have still been published before the belongs_to failure
    let publishes = mock_comm_client.get_publishes().await;
    let relations_meta_publish = publishes.iter().find(|p| {
        p.topic == format!("{}/{}/{}/_meta", parent_asset.namespace, child_name, "_relations")
    });
    assert!(relations_meta_publish.is_some(), "_meta for _relations should have been attempted or published.");
}
