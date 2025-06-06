use assets2036_rs::*;
use async_trait::async_trait;

// Import validate_value if it's public in the crate
use assets2036_rs::*;
use serde_json::{json, Value};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::{boxed::Box, collections::HashMap};
use tokio::sync::Mutex as TokioMutex;
type PropertyCallbackForMock = Box<dyn Fn(String, String) + Send + Sync + 'static>;
type EventCallbackForMock = Box<dyn Fn(HashMap<String, Value>, i64) + Send + Sync + 'static>;
type OperationCallback = Box<dyn Fn(Value) -> Result<Value, Error> + Send + Sync + 'static>;

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
            .field(
                "subscriptions_len",
                &self.subscriptions.blocking_lock().len(),
            )
            .field(
                "event_subscriptions_len",
                &self.event_subscriptions.blocking_lock().len(),
            )
            .field(
                "unsubscriptions_len",
                &self.unsubscriptions.blocking_lock().len(),
            )
            .field(
                "operation_bindings_len",
                &self.operation_bindings.blocking_lock().len(),
            )
            .field(
                "operation_invocations_len",
                &self.operation_invocations.blocking_lock().len(),
            )
            .finish_non_exhaustive()
    }
}

impl MockCommunicationClient {
    pub fn new() -> Self {
        Default::default()
    }

    pub async fn set_invoke_operation_response(&self, topic: &str, response: Result<Value, Error>) {
        let cloned_response = match response {
            Ok(v) => Ok(v.clone()),
            Err(e) => Err(Error::Other(format!("Mocked error: {:?}", e))),
        };
        self.invoke_operation_results
            .lock()
            .await
            .insert(topic.to_string(), cloned_response);
    }

    pub async fn set_query_submodels_response(
        &self,
        response: Result<HashMap<String, Value>, Error>,
    ) {
        let cloned_response = match response {
            Ok(v) => Ok(v.clone()),
            Err(e) => Err(Error::Other(format!("Mocked error: {:?}", e))),
        };
        *self.query_submodels_responses.lock().await = Some(cloned_response);
    }

    pub async fn set_query_asset_names_response(&self, response: Result<Vec<String>, Error>) {
        let cloned_response = match response {
            Ok(v) => Ok(v.clone()),
            Err(e) => Err(Error::Other(format!("Mocked error: {:?}", e))),
        };
        *self.query_asset_names_responses.lock().await = Some(cloned_response);
    }

    pub async fn get_publishes(&self) -> Vec<RecordedPublish> {
        self.publishes.lock().await.clone()
    }
    pub async fn get_subscriptions(&self) -> Vec<RecordedSubscription> {
        self.subscriptions.lock().await.clone()
    }
    pub async fn get_unsubscriptions(&self) -> Vec<String> {
        self.unsubscriptions.lock().await.clone()
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
    pub async fn simulate_event_received(
        &self,
        topic: &str,
        params: HashMap<String, Value>,
        timestamp: i64,
    ) {
        if let Some(callback) = self.event_callbacks.lock().await.get(topic) {
            callback(params, timestamp);
        }
    }
    pub async fn simulate_operation_call(
        &self,
        topic: &str,
        params: Value,
    ) -> Option<Result<Value, Error>> {
        let request_topic = format!("{}/request", topic);
        match self.bound_op_callbacks.lock().await.get(&request_topic) {
            Some(cb) => Some(cb(params)),
            None => None,
        }
    }
}

#[async_trait]
impl CommunicationClient for MockCommunicationClient {
    async fn connect(
        &self,
        _host: &str,
        _port: u16,
        _namespace: &str,
        _endpoint_name: &str,
    ) -> Result<(), Error> {
        *self.connect_count.lock().await += 1;
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), Error> {
        *self.disconnect_count.lock().await += 1;
        Ok(())
    }

    async fn publish(&self, topic: String, payload: String, retain: bool) -> Result<(), Error> {
        self.publishes.lock().await.push(RecordedPublish {
            topic,
            payload,
            retain,
        });
        Ok(())
    }

    async fn subscribe(
        &self,
        topic: String,
        callback: PropertyCallbackForMock,
    ) -> Result<(), Error> {
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

    async fn subscribe_event(
        &self,
        topic: String,
        callback: EventCallbackForMock,
    ) -> Result<(), Error> {
        self.event_subscriptions
            .lock()
            .await
            .push(RecordedEventSubscription {
                topic: topic.clone(),
            });
        self.event_callbacks.lock().await.insert(topic, callback);
        Ok(())
    }

    async fn trigger_event(
        &self,
        topic: String,
        params: HashMap<String, Value>,
    ) -> Result<(), Error> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let event_payload = serde_json::json!({
            "timestamp": now_ms,
            "parameters": params
        });
        let payload_str = serde_json::to_string(&event_payload).map_err(|e| Error::JsonError(e))?;
        self.publish(topic, payload_str, false).await
    }

    async fn invoke_operation(
        &self,
        topic: String,
        params: Value,
        timeout_ms: u64,
    ) -> Result<Value, Error> {
        self.operation_invocations
            .lock()
            .await
            .push(RecordedOperationInvocation {
                topic: topic.clone(),
                params,
                timeout_ms,
            });
        match self.invoke_operation_results.lock().await.get(&topic) {
            Some(Ok(v)) => Ok(v.clone()),
            Some(Err(e)) => Err(Error::Other(format!("Mock Invoke Error: {:?}", e))),
            None => Ok(Value::Null),
        }
    }

    async fn bind_operation(
        &self,
        topic: String,
        callback: OperationCallback,
    ) -> Result<(), Error> {
        let request_topic = format!("{}/request", topic);
        self.operation_bindings
            .lock()
            .await
            .push(RecordedOperationBinding {
                topic: request_topic.clone(),
            });
        self.bound_op_callbacks
            .lock()
            .await
            .insert(request_topic, callback);
        Ok(())
    }

    async fn query_submodels_for_asset(
        &self,
        _namespace: &str,
        _asset_name: &str,
    ) -> Result<HashMap<String, Value>, Error> {
        match self.query_submodels_responses.lock().await.as_ref() {
            Some(Ok(v)) => Ok(v.clone()),
            Some(Err(e)) => Err(Error::Other(format!(
                "Mock query_submodels_for_asset Error: {:?}",
                e
            ))),
            None => Ok(HashMap::new()),
        }
    }

    async fn query_asset_names(
        &self,
        _namespace: Option<&str>,
        _submodel_names: &[&str],
    ) -> Result<Vec<String>, Error> {
        match self.query_asset_names_responses.lock().await.as_ref() {
            Some(Ok(v)) => Ok(v.clone()),
            Some(Err(e)) => Err(Error::Other(format!(
                "Mock query_asset_names Error: {:?}",
                e
            ))),
            None => Ok(Vec::new()),
        }
    }
}

#[test]
fn it_compiles() {
    assert_eq!(2 + 2, 4);
}

#[test]
fn property_type_deserialization() {
    let json_boolean = "\"boolean\"";
    let type_boolean: PropertyType = serde_json::from_str(json_boolean).unwrap();
    assert_eq!(type_boolean, PropertyType::Boolean);
}

#[test]
fn test_deserialize_bool_property() {
    let json_str = r#"{"type": "boolean", "description": "A switch"}"#;
    let prop_def: PropertyDefinition =
        serde_json::from_str(json_str).expect("Failed to deserialize bool property");
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
    let prop_def: PropertyDefinition =
        serde_json::from_str(json_str).expect("Failed to deserialize object property");
    assert_eq!(prop_def.type_of, PropertyType::Object);
    let props_map = prop_def
        .properties
        .as_ref()
        .expect("Properties map should exist");

    let x_def = props_map
        .get("x")
        .expect("Property x should exist")
        .as_ref();
    assert_eq!(x_def.type_of, PropertyType::Integer);

    let y_def = props_map
        .get("y")
        .expect("Property y should exist")
        .as_ref();
    assert_eq!(y_def.type_of, PropertyType::Number);
    assert_eq!(y_def.description.as_deref(), Some("Y coordinate"));
}

#[test]
fn test_deserialize_array_property() {
    let json_str = r#"{"type": "array", "items": {"type": "string"}}"#;
    let prop_def: PropertyDefinition =
        serde_json::from_str(json_str).expect("Failed to deserialize array property");
    assert_eq!(prop_def.type_of, PropertyType::Array);
    let items_def = prop_def
        .items
        .as_ref()
        .expect("Items definition should exist")
        .as_ref();
    assert_eq!(items_def.type_of, PropertyType::String);
}

#[test]
fn test_deserialize_enum_property() {
    let json_str = r#"{"type": "string", "enum": ["ON", "OFF"]}"#;
    let prop_def: PropertyDefinition =
        serde_json::from_str(json_str).expect("Failed to deserialize enum property");
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
    let sm_def: SubModelDefinition =
        serde_json::from_str(json_str).expect("Failed to deserialize submodel");
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
    let p_bool = PropertyDefinition {
        type_of: PropertyType::Boolean,
        ..Default::default()
    };
    assert!(validate_value(&json!(true), &p_bool).is_ok());
    let p_int = PropertyDefinition {
        type_of: PropertyType::Integer,
        ..Default::default()
    };
    assert!(validate_value(&json!(123), &p_int).is_ok());
    let p_num = PropertyDefinition {
        type_of: PropertyType::Number,
        ..Default::default()
    };
    assert!(validate_value(&json!(123.45), &p_num).is_ok());
    assert!(validate_value(&json!(123), &p_num).is_ok());
    let p_str = PropertyDefinition {
        type_of: PropertyType::String,
        ..Default::default()
    };
    assert!(validate_value(&json!("hello"), &p_str).is_ok());
    let p_null = PropertyDefinition {
        type_of: PropertyType::Null,
        ..Default::default()
    };
    assert!(validate_value(&json!(null), &p_null).is_ok());
}

#[test]
fn test_validate_value_simple_types_err() {
    let p_bool = PropertyDefinition {
        type_of: PropertyType::Boolean,
        ..Default::default()
    };
    assert!(validate_value(&json!(123), &p_bool).is_err());
    let p_int = PropertyDefinition {
        type_of: PropertyType::Integer,
        ..Default::default()
    };
    assert!(validate_value(&json!("test"), &p_int).is_err());
    assert!(validate_value(&json!(12.34), &p_int).is_err());
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
    props_map.insert(
        "x".to_string(),
        Box::new(PropertyDefinition {
            type_of: PropertyType::Integer,
            ..Default::default()
        }),
    );
    props_map.insert(
        "s".to_string(),
        Box::new(PropertyDefinition {
            type_of: PropertyType::String,
            ..Default::default()
        }),
    );
    let p_obj = PropertyDefinition {
        type_of: PropertyType::Object,
        properties: Some(props_map),
        ..Default::default()
    };

    assert!(validate_value(&json!({"x": 10, "s": "test"}), &p_obj).is_ok());
    assert!(validate_value(&json!({"x": 10, "s": "test", "y_extra": true}), &p_obj).is_ok());
}

#[test]
fn test_validate_value_object_err_subtype() {
    let mut props_map = HashMap::new();
    props_map.insert(
        "x".to_string(),
        Box::new(PropertyDefinition {
            type_of: PropertyType::Integer,
            ..Default::default()
        }),
    );
    let p_obj = PropertyDefinition {
        type_of: PropertyType::Object,
        properties: Some(props_map),
        ..Default::default()
    };
    assert!(validate_value(&json!({"x": "not-an-int"}), &p_obj).is_err());
}

#[test]
fn test_validate_value_object_nested() {
    let mut nested_props = HashMap::new();
    nested_props.insert(
        "b".to_string(),
        Box::new(PropertyDefinition {
            type_of: PropertyType::Boolean,
            ..Default::default()
        }),
    );
    let mut props_map = HashMap::new();
    props_map.insert(
        "nested".to_string(),
        Box::new(PropertyDefinition {
            type_of: PropertyType::Object,
            properties: Some(nested_props),
            ..Default::default()
        }),
    );
    let p_obj = PropertyDefinition {
        type_of: PropertyType::Object,
        properties: Some(props_map),
        ..Default::default()
    };

    assert!(validate_value(&json!({"nested": {"b": true}}), &p_obj).is_ok());
    assert!(validate_value(&json!({"nested": {"b": "true"}}), &p_obj).is_err());
}

#[test]
fn test_validate_value_array_ok() {
    let p_arr = PropertyDefinition {
        type_of: PropertyType::Array,
        items: Some(Box::new(PropertyDefinition {
            type_of: PropertyType::String,
            ..Default::default()
        })),
        ..Default::default()
    };
    assert!(validate_value(&json!(["a", "b", "c"]), &p_arr).is_ok());
    assert!(validate_value(&json!([]), &p_arr).is_ok());
}

#[test]
fn test_validate_value_array_err_subtype() {
    let p_arr = PropertyDefinition {
        type_of: PropertyType::Array,
        items: Some(Box::new(PropertyDefinition {
            type_of: PropertyType::String,
            ..Default::default()
        })),
        ..Default::default()
    };
    assert!(validate_value(&json!(["a", 123]), &p_arr).is_err());
}

#[test]
fn test_validate_value_array_of_objects() {
    let mut item_props = HashMap::new();
    item_props.insert(
        "id".to_string(),
        Box::new(PropertyDefinition {
            type_of: PropertyType::Integer,
            ..Default::default()
        }),
    );
    let p_arr = PropertyDefinition {
        type_of: PropertyType::Array,
        items: Some(Box::new(PropertyDefinition {
            type_of: PropertyType::Object,
            properties: Some(item_props),
            ..Default::default()
        })),
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
async fn test_mock_client_instantiation_and_publish() {
    let mock_client = MockCommunicationClient::new();
    let result = mock_client
        .publish("test/topic".to_string(), "payload".to_string(), false)
        .await;
    assert!(result.is_ok());
    let publishes = mock_client.get_publishes().await;
    assert_eq!(publishes.len(), 1);
    assert_eq!(publishes[0].topic, "test/topic");
}

#[tokio::test]
async fn test_owner_property_set_value_publishes() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let prop_def_json = r#"{"type": "boolean"}"#;
    let prop_def: PropertyDefinition = serde_json::from_str(prop_def_json).unwrap();

    let property = Property::new(
        "mySwitch".to_string(),
        prop_def,
        mock_client.clone() as Arc<dyn CommunicationClient>,
        "test_namespace/test_asset/test_submodel".to_string(),
    );

    let result = property.set_value(json!(true)).await;
    assert!(result.is_ok());

    let publishes = mock_client.get_publishes().await;
    assert_eq!(publishes.len(), 1);
    assert_eq!(
        publishes[0].topic,
        "test_namespace/test_asset/test_submodel/mySwitch"
    );
    assert_eq!(publishes[0].payload, "true");
    assert_eq!(publishes[0].retain, true);

    let _ = property.set_value(json!(false)).await;
    let publishes_after_second_set = mock_client.get_publishes().await;
    assert_eq!(publishes_after_second_set.len(), 2);
    assert_eq!(publishes_after_second_set[1].payload, "false");
}

#[tokio::test]
async fn test_owner_event_trigger_publishes() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let event_def_json = r#"{
            "parameters": {
                "brightness": {"type": "integer"}
            }
        }"#;
    let event_def: EventDefinition = serde_json::from_str(event_def_json).unwrap();

    let event = Event::new(
        "brightnessChanged".to_string(),
        event_def,
        mock_client.clone() as Arc<dyn CommunicationClient>,
        "test_namespace/test_asset/test_submodel".to_string(),
    );

    let params_map: HashMap<String, Value> = [("brightness".to_string(), json!(75))]
        .into_iter()
        .collect();
    let params_value = serde_json::to_value(params_map).unwrap();
    let result = event.trigger(params_value.clone()).await;
    assert!(result.is_ok());

    let publishes = mock_client.get_publishes().await;
    assert_eq!(publishes.len(), 1);
    assert_eq!(
        publishes[0].topic,
        "test_namespace/test_asset/test_submodel/brightnessChanged"
    );
    assert_eq!(publishes[0].retain, false);

    let published_payload_val: Value = serde_json::from_str(&publishes[0].payload).unwrap();
    assert!(published_payload_val.get("timestamp").is_some());
    assert!(published_payload_val.get("timestamp").unwrap().is_i64());
    assert_eq!(
        published_payload_val.get("parameters").unwrap(),
        &params_value
    );
}

#[tokio::test]
async fn test_owner_operation_bind_registers_with_client() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let op_def_json =
        r#"{"parameters": {"a": {"type": "integer"}}, "response": {"type": "integer"}}"#;
    let op_def: OperationDefinition = serde_json::from_str(op_def_json).unwrap();

    let operation = Operation::new(
        "add".to_string(),
        op_def,
        mock_client.clone() as Arc<dyn CommunicationClient>,
        "test_namespace/test_asset/test_submodel".to_string(),
    );

    let callback: OperationCallback = Box::new(|params_val| {
        // The params_val here is the "params" field from the request payload if using the standard wrapper.
        // For this test, assuming the callback expects the direct parameters object.
        let a_val = params_val
            .get("a")
            .ok_or_else(|| Error::InvalidParameter("Missing 'a'".to_string()))?
            .as_i64()
            .ok_or_else(|| Error::InvalidParameter("'a' not i64".to_string()))?;
        Ok(json!(a_val + 1))
    });

    let result = operation.bind(callback).await;
    assert!(result.is_ok());

    let bound_op_callbacks = mock_client.bound_op_callbacks.lock().await;
    assert!(bound_op_callbacks.contains_key("test_namespace/test_asset/test_submodel/add/request"));
    assert_eq!(bound_op_callbacks.len(), 1);
}

#[tokio::test]
async fn test_owner_submodel_new_publishes_meta() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let sm_def_json = r#"{
            "name": "test_sm",
            "version": "1.0",
            "description": "A test submodel",
            "properties": {"prop1": {"type": "boolean"}}
        }"#;
    let sm_def: SubModelDefinition = serde_json::from_str(sm_def_json).unwrap();

    let _submodel = SubModel::new(
        sm_def.clone(),
        "test_namespace/test_asset".to_string(),
        &Mode::Owner,
        mock_client.clone() as Arc<dyn CommunicationClient>,
        "test_namespace".to_string(),
        "test_asset_endpoint".to_string(),
    )
    .await
    .unwrap();

    let publishes = mock_client.get_publishes().await;
    assert_eq!(
        publishes.len(),
        1,
        "Expected one publish for _meta property but got: {:?}",
        publishes
    );

    let meta_publish = &publishes[0];
    assert_eq!(
        meta_publish.topic,
        "test_namespace/test_asset/test_sm/_meta"
    );
    assert_eq!(meta_publish.retain, true);

    let meta_payload_val: Value = serde_json::from_str(&meta_publish.payload).unwrap();
    assert_eq!(
        meta_payload_val["source"],
        "test_namespace/test_asset_endpoint"
    );
    assert_eq!(meta_payload_val["submodel_url"], "file://localhost");

    let expected_sm_def_val = serde_json::to_value(sm_def.clone()).unwrap();
    assert_eq!(meta_payload_val["submodel_definition"], expected_sm_def_val);
}

#[tokio::test]
async fn test_consumer_property_on_change_updates_value_and_calls_callback() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let prop_def_json = r#"{"type": "integer", "readOnly": true}"#;
    let prop_def: PropertyDefinition = serde_json::from_str(prop_def_json).unwrap();

    let property = Property::new(
        "sensorValue".to_string(),
        prop_def,
        mock_client.clone() as Arc<dyn CommunicationClient>,
        "remote_ns/remote_asset/sensor_submodel".to_string(),
    );

    let callback_fired = Arc::new(AtomicBool::new(false));
    let received_value = Arc::new(TokioMutex::new(json!(null)));

    let callback_fired_clone = callback_fired.clone();
    let received_value_clone = received_value.clone();
    let on_change_callback = Box::new(move |new_val: Value| {
        callback_fired_clone.store(true, AtomicOrdering::SeqCst);
        let mut lock = received_value_clone.try_lock().unwrap();
        *lock = new_val;
    });

    let subscribe_result = property.on_change(on_change_callback).await;
    assert!(subscribe_result.is_ok());

    let subscriptions = mock_client.get_subscriptions().await;
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(
        subscriptions[0].topic,
        "remote_ns/remote_asset/sensor_submodel/sensorValue"
    );

    let new_payload = json!(123).to_string();
    mock_client
        .simulate_property_update(
            "remote_ns/remote_asset/sensor_submodel/sensorValue",
            new_payload,
        )
        .await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert_eq!(
        property.get_value(),
        json!(123),
        "Property's internal value should be updated."
    );
    assert!(
        callback_fired.load(AtomicOrdering::SeqCst),
        "User callback should have been fired."
    );
    assert_eq!(
        *received_value.lock().await,
        json!(123),
        "Value received in callback is incorrect."
    );
}

#[tokio::test]
async fn test_consumer_event_on_event_calls_callback_old() {
    // Name kept from existing to be replaced by new one below
    let mock_client = Arc::new(MockCommunicationClient::new());
    let event_def_json = r#"{"parameters": {"status": {"type": "string"}}}"#;
    let event_def: EventDefinition = serde_json::from_str(event_def_json).unwrap();

    let event = Event::new(
        "statusUpdate".to_string(),
        event_def,
        mock_client.clone() as Arc<dyn CommunicationClient>,
        "remote_ns/remote_asset/status_submodel".to_string(),
    );

    let callback_fired = Arc::new(AtomicBool::new(false));
    let received_params = Arc::new(TokioMutex::new(HashMap::<String, Value>::new()));
    let received_timestamp = Arc::new(TokioMutex::new(0i64));

    let cb_fired_clone = callback_fired.clone();
    let rx_params_clone = received_params.clone();
    let rx_ts_clone = received_timestamp.clone();
    let on_event_callback = Box::new(move |params_map: HashMap<String, Value>, ts: i64| {
        cb_fired_clone.store(true, AtomicOrdering::SeqCst);
        let mut lock_params = rx_params_clone.try_lock().unwrap();
        *lock_params = params_map;
        let mut lock_ts = rx_ts_clone.try_lock().unwrap();
        *lock_ts = ts;
    });

    let subscribe_result = event.on_event(on_event_callback).await;
    assert!(subscribe_result.is_ok());

    let event_subs = mock_client.event_subscriptions.lock().await.clone();
    assert_eq!(event_subs.len(), 1);
    assert_eq!(
        event_subs[0].topic,
        "remote_ns/remote_asset/status_submodel/statusUpdate"
    );

    let simulated_params: HashMap<String, Value> = [("status".to_string(), json!("active"))]
        .into_iter()
        .collect();
    let simulated_timestamp = chrono::Utc::now().timestamp_millis();
    mock_client
        .simulate_event_received(
            "remote_ns/remote_asset/status_submodel/statusUpdate",
            simulated_params.clone(),
            simulated_timestamp,
        )
        .await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert!(
        callback_fired.load(AtomicOrdering::SeqCst),
        "User callback for event should have been fired."
    );
    assert_eq!(
        *received_params.lock().await,
        simulated_params,
        "Parameters received in event callback are incorrect."
    );
    assert_eq!(
        *received_timestamp.lock().await,
        simulated_timestamp,
        "Timestamp received in event callback is incorrect."
    );
}

#[tokio::test]
async fn test_consumer_operation_invoke_calls_client_and_returns_response() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let op_def_json =
        r#"{"parameters": {"a": {"type": "integer"}}, "response": {"type": "integer"}}"#;
    let op_def: OperationDefinition = serde_json::from_str(op_def_json).unwrap();

    let operation = Operation::new(
        "remoteAdd".to_string(),
        op_def,
        mock_client.clone() as Arc<dyn CommunicationClient>,
        "remote_ns/remote_asset/math_submodel".to_string(),
    );

    let expected_response_val = json!(15);
    mock_client
        .set_invoke_operation_response(
            "remote_ns/remote_asset/math_submodel/remoteAdd",
            Ok(expected_response_val.clone()),
        )
        .await;

    let params = json!({"a": 10});
    let result = operation.invoke(params.clone()).await;

    assert!(result.is_ok(), "Invoke failed: {:?}", result.err());
    assert_eq!(result.unwrap(), expected_response_val);

    let invocations = mock_client.get_operation_invocations().await;
    assert_eq!(invocations.len(), 1);
    assert_eq!(
        invocations[0].topic,
        "remote_ns/remote_asset/math_submodel/remoteAdd"
    );
    assert_eq!(invocations[0].params, params);
}

#[tokio::test]
async fn test_consumer_operation_invoke_handles_error_response() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let op_def: OperationDefinition =
        serde_json::from_str(r#"{"response": {"type": "string"}}"#).unwrap();
    let operation = Operation::new(
        "getErrorOp".to_string(),
        op_def,
        mock_client.clone(),
        "remote/asset/sm".to_string(),
    );

    mock_client
        .set_invoke_operation_response(
            "remote/asset/sm/getErrorOp",
            Err(Error::Other("Simulated RPC Error".to_string())),
        )
        .await;

    let result = operation.invoke(json!({})).await;
    assert!(result.is_err());
    if let Err(Error::Other(msg)) = result {
        assert!(msg.contains("Simulated RPC Error") || msg.contains("Mock Invoke Error"));
    } else {
        panic!("Expected Error::Other, got {:?}", result);
    }
}

#[tokio::test]
async fn test_consumer_property_on_change_updates_value_and_calls_callback_new() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let prop_def_json = r#"{"type": "integer", "readOnly": true}"#;
    let prop_def: PropertyDefinition = serde_json::from_str(prop_def_json).unwrap();

    let property = Property::new(
        "sensorValue".to_string(),
        prop_def,
        mock_client.clone() as Arc<dyn CommunicationClient>,
        "remote_ns/remote_asset/sensor_submodel".to_string(),
    );

    assert_eq!(
        property.get_value(),
        json!(null),
        "Initial property value should be null."
    );

    let callback_fired_flag = Arc::new(AtomicBool::new(false));
    let value_in_callback_storage = Arc::new(TokioMutex::new(json!(null)));

    let flag_clone = callback_fired_flag.clone();
    let storage_clone = value_in_callback_storage.clone();

    let on_change_user_cb = Box::new(move |new_val: Value| {
        flag_clone.store(true, AtomicOrdering::SeqCst);
        let mut locked_storage = storage_clone
            .try_lock()
            .expect("Failed to lock storage in user_cb for test");
        *locked_storage = new_val;
    });

    let subscribe_result = property.on_change(on_change_user_cb).await;
    assert!(
        subscribe_result.is_ok(),
        "on_change subscription call failed"
    );

    let subscriptions_recorded = mock_client.get_subscriptions().await;
    assert_eq!(
        subscriptions_recorded.len(),
        1,
        "Expected one subscription to be recorded."
    );
    assert_eq!(
        subscriptions_recorded[0].topic,
        "remote_ns/remote_asset/sensor_submodel/sensorValue"
    );

    let simulated_payload_str = json!(123).to_string();
    mock_client
        .simulate_property_update(
            "remote_ns/remote_asset/sensor_submodel/sensorValue",
            simulated_payload_str,
        )
        .await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert!(
        callback_fired_flag.load(AtomicOrdering::SeqCst),
        "User callback should have been fired."
    );
    assert_eq!(
        *value_in_callback_storage.lock().await,
        json!(123),
        "Value received in user callback is incorrect."
    );
    assert_eq!(
        property.get_value(),
        json!(123),
        "Property's internal value should be updated after simulated update."
    );
}

#[tokio::test]
async fn test_consumer_event_on_event_calls_callback() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let event_def_json = r#"{"parameters": {"status": {"type": "string"}}}"#;
    let event_def: EventDefinition = serde_json::from_str(event_def_json).unwrap();

    let event = Event::new(
        "statusUpdate".to_string(),
        event_def,
        mock_client.clone() as Arc<dyn CommunicationClient>,
        "remote_ns/remote_asset/status_submodel".to_string(),
    );

    let callback_fired_flag = Arc::new(AtomicBool::new(false));
    let received_params_storage = Arc::new(TokioMutex::new(HashMap::<String, Value>::new()));
    let received_timestamp_storage = Arc::new(TokioMutex::new(0i64));

    let flag_clone = callback_fired_flag.clone();
    let params_storage_clone = received_params_storage.clone();
    let timestamp_storage_clone = received_timestamp_storage.clone();

    let on_event_user_cb = Box::new(move |params_map: HashMap<String, Value>, ts: i64| {
        flag_clone.store(true, AtomicOrdering::SeqCst);
        let mut locked_params = params_storage_clone
            .try_lock()
            .expect("Failed to lock params_storage in user_cb");
        *locked_params = params_map;
        let mut locked_ts = timestamp_storage_clone
            .try_lock()
            .expect("Failed to lock timestamp_storage in user_cb");
        *locked_ts = ts;
    });

    let subscribe_result = event.on_event(on_event_user_cb).await;
    assert!(
        subscribe_result.is_ok(),
        "on_event subscription call failed"
    );

    let event_subs_recorded = mock_client.event_subscriptions.lock().await.clone();
    assert_eq!(
        event_subs_recorded.len(),
        1,
        "Expected one event subscription to be recorded."
    );
    assert_eq!(
        event_subs_recorded[0].topic,
        "remote_ns/remote_asset/status_submodel/statusUpdate"
    );

    let simulated_params_map: HashMap<String, Value> = [("status".to_string(), json!("active"))]
        .into_iter()
        .collect();
    let simulated_timestamp = chrono::Utc::now().timestamp_millis();

    mock_client
        .simulate_event_received(
            "remote_ns/remote_asset/status_submodel/statusUpdate",
            simulated_params_map.clone(),
            simulated_timestamp,
        )
        .await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    assert!(
        callback_fired_flag.load(AtomicOrdering::SeqCst),
        "User callback for event should have been fired."
    );

    let locked_params = received_params_storage.lock().await;
    assert_eq!(
        *locked_params, simulated_params_map,
        "Parameters received in event callback are incorrect."
    );

    let locked_timestamp = received_timestamp_storage.lock().await;
    assert_eq!(
        *locked_timestamp, simulated_timestamp,
        "Timestamp received in event callback is incorrect."
    );
}

#[tokio::test]
async fn test_consumer_operation_invoke_success() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let op_def_json = r#"{"parameters": {"a": {"type": "integer"}, "b": {"type": "integer"}}, "response": {"type": "integer"}}"#;
    let op_def: OperationDefinition = serde_json::from_str(op_def_json).unwrap();

    let operation = Operation::new(
        "remoteAdd".to_string(),
        op_def,
        mock_client.clone() as Arc<dyn CommunicationClient>,
        "remote_ns/remote_asset/math_submodel".to_string(),
    );

    let expected_response_val = json!(15);
    mock_client
        .set_invoke_operation_response(
            "remote_ns/remote_asset/math_submodel/remoteAdd",
            Ok(expected_response_val.clone()),
        )
        .await;

    let params_to_send = json!({"a": 10, "b": 5});

    let result = operation.invoke(params_to_send.clone()).await;

    assert!(result.is_ok(), "invoke call failed: {:?}", result.err());
    assert_eq!(
        result.unwrap(),
        expected_response_val,
        "Response value from invoke did not match expected."
    );

    let invocations = mock_client.get_operation_invocations().await;
    assert_eq!(
        invocations.len(),
        1,
        "Expected one operation invocation to be recorded."
    );
    let invocation = &invocations[0];
    assert_eq!(
        invocation.topic,
        "remote_ns/remote_asset/math_submodel/remoteAdd"
    );
    assert_eq!(invocation.params, params_to_send);
    assert_eq!(invocation.timeout_ms, DEFAULT_OPERATION_TIMEOUT_MS);
}

#[tokio::test]
async fn test_consumer_operation_invoke_handles_remote_error() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let op_def_json = r#"{"response": {"type": "string"}}"#;
    let op_def: OperationDefinition = serde_json::from_str(op_def_json).unwrap();

    let operation = Operation::new(
        "getErrorOp".to_string(),
        op_def,
        mock_client.clone() as Arc<dyn CommunicationClient>,
        "remote_ns/remote_asset/error_sm".to_string(),
    );

    let simulated_error = Error::Other("Simulated RPC Error from remote".to_string());
    mock_client
        .set_invoke_operation_response(
            "remote_ns/remote_asset/error_sm/getErrorOp",
            Err(simulated_error),
        )
        .await;

    let params = json!({});
    let result = operation.invoke(params).await;

    assert!(result.is_err(), "Expected invoke to return an error.");
    match result.err().unwrap() {
        Error::Other(msg) => {
            assert!(
                msg.contains("Simulated RPC Error from remote")
                    || msg.contains("Mock Invoke Error"),
                "Error message mismatch: {}",
                msg
            );
        }
        e => panic!("Expected Error::Other, but got {:?}", e),
    }
}

#[tokio::test]
async fn test_consumer_operation_invoke_parameter_validation_fails() {
    let mock_client = Arc::new(MockCommunicationClient::new());
    let op_def_json =
        r#"{"parameters": {"a": {"type": "integer"}}, "response": {"type": "integer"}}"#;
    let op_def: OperationDefinition = serde_json::from_str(op_def_json).unwrap();

    let operation = Operation::new(
        "validationTestOp".to_string(),
        op_def,
        mock_client.clone() as Arc<dyn CommunicationClient>,
        "remote_ns/remote_asset/validation_sm".to_string(),
    );

    let params_invalid_type = json!({"a": "not-an-integer"});
    let result = operation.invoke(params_invalid_type).await;

    assert!(
        result.is_err(),
        "Expected invoke to fail due to parameter validation."
    );
    match result.err().unwrap() {
        Error::InvalidParameter(msg) => {
            assert!(
                msg.contains("Validation failed for parameter 'a'"),
                "Error message mismatch: {}",
                msg
            );
            assert!(
                msg.contains("expected integer, got String")
                    || msg.contains("expected integer, got Object"),
                "Error message mismatch: {}",
                msg
            ); //serde_json may parse "not-an-integer" as String or an Object if it's more complex
        }
        e => panic!("Expected Error::InvalidParameter, but got {:?}", e),
    }

    let invocations = mock_client.get_operation_invocations().await;
    assert_eq!(
        invocations.len(),
        0,
        "Communication client should not have been called if params are invalid."
    );
}
