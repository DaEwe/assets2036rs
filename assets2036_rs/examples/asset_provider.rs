use assets2036_rs::{AssetManager, Mode, Error, Property, Event};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::Mutex; // Using Tokio's Mutex for async state
use tokio::time::Duration;

// Configuration - consider using environment variables or a config file for real applications
const BROKER_HOST: &str = "localhost";
const BROKER_PORT: u16 = 1883;
const NAMESPACE: &str = "org_assets2036_example";
const ASSET_NAME: &str = "lamp_1";
const ENDPOINT_NAME: &str = "rust_provider_endpoint";

// Submodel definition as a JSON string
// In a real scenario, this might be loaded from a file or URL
const LIGHT_SUBMODEL_JSON: &str = r#"
{
    "name": "light",
    "version": "1.0.0",
    "properties": {
        "light_on": {
            "type": "boolean",
            "description": "Current state of the light (on/off)",
            "readOnly": true
        }
    },
    "operations": {
        "switch_light": {
            "description": "Switches the light on or off",
            "parameters": {
                "state": {"type": "boolean"}
            }
        }
    },
    "events": {
        "light_switched": {
            "description": "Emitted when the light state changes",
            "parameters": {
                "new_state": {"type": "boolean"}
            }
        }
    }
}
"#;

// Simple struct to represent our light's state
struct Light {
    is_on: bool,
}

impl Light {
    fn new() -> Self {
        Light { is_on: false }
    }

    fn switch(&mut self, state: bool) -> bool {
        if self.is_on == state {
            return false; // No change
        }
        self.is_on = state;
        println!("Light is now: {}", if self.is_on { "ON" } else { "OFF" });
        true // State changed
    }

    fn get_state(&self) -> bool {
        self.is_on
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Starting asset provider example...");

    let mut manager = AssetManager::new(
        BROKER_HOST,
        BROKER_PORT,
        NAMESPACE,
        ENDPOINT_NAME,
    );

    manager.connect().await.map_err(|e| {
        log::error!("Failed to connect AssetManager: {:?}", e);
        anyhow::anyhow!("AssetManager connection failed: {:?}", e)
    })?;
    log::info!("AssetManager connected to {} at {}:{}", NAMESPACE, BROKER_HOST, BROKER_PORT);

    // Initialize the logging handler for the manager
    match manager.get_logging_handler() {
        Ok(handler) => {
            if let Err(e) = log::set_boxed_logger(Box::new(handler)){
                 log::warn!("Failed to set logger for AssetManager: {:?} (possibly already set)",e);
            }
            log::set_max_level(log::LevelFilter::Info); // Or desired level
            log::info!("AssetManager logging handler initialized.");
        }
        Err(e) => {
            log::error!("Failed to get logging handler: {:?}", e);
        }
    }


    let mut lamp_asset = manager.create_asset(
        ASSET_NAME,
        vec![LIGHT_SUBMODEL_JSON.to_string()],
        Mode::Owner,
    ).await.map_err(|e| {
        log::error!("Failed to create asset '{}': {:?}", ASSET_NAME, e);
        anyhow::anyhow!("Asset creation failed for '{}': {:?}", ASSET_NAME, e)
    })?;
    log::info!("Asset '{}' created successfully.", ASSET_NAME);

    let light_state = Arc::new(Mutex::new(Light::new()));

    // Get handles to property and event for use in the operation callback
    let light_on_prop: Arc<Property> = lamp_asset
        .get_submodel("light").ok_or_else(|| anyhow::anyhow!("Submodel 'light' not found"))?
        .get_property("light_on").ok_or_else(|| anyhow::anyhow!("Property 'light_on' not found"))?;

    // Set initial property state
    light_on_prop.set_value(json!(light_state.lock().await.get_state())).await?;


    let light_switched_event: Arc<Event> = lamp_asset
        .get_submodel("light").ok_or_else(|| anyhow::anyhow!("Submodel 'light' not found"))?
        .get_event("light_switched").ok_or_else(|| anyhow::anyhow!("Event 'light_switched' not found"))?;

    if let Some(switch_light_op_arc) = lamp_asset.get_submodel("light").unwrap().get_operation("switch_light") {
        let light_state_clone = Arc::clone(&light_state);
        let light_on_prop_clone = Arc::clone(&light_on_prop);
        let light_switched_event_clone = Arc::clone(&light_switched_event);

        switch_light_op_arc.bind(Box::new(move |params: Value| {
            log::info!("'switch_light' operation called with params: {:?}", params);
            let state_param = params.get("state").and_then(|v| v.as_bool());

            if let Some(new_state) = state_param {
                let light_state_op = Arc::clone(&light_state_clone);
                let light_on_prop_op = Arc::clone(&light_on_prop_clone);
                let light_switched_event_op = Arc::clone(&light_switched_event_clone);

                // Spawn a task to handle async operations (state update, property set, event trigger)
                tokio::spawn(async move {
                    let mut light = light_state_op.lock().await;
                    if light.switch(new_state) { // If state actually changed
                        if let Err(e) = light_on_prop_op.set_value(json!(new_state)).await {
                            log::error!("Failed to set 'light_on' property: {:?}", e);
                        }
                        let event_params = json!({"new_state": new_state});
                        if let Err(e) = light_switched_event_op.trigger(event_params).await {
                            log::error!("Failed to trigger 'light_switched' event: {:?}", e);
                        }
                    }
                });
                Ok(Value::Null) // Operation acknowledged
            } else {
                Err(Error::InvalidParameter {
                    context: "switch_light".to_string(),
                    reason: "Missing or invalid 'state' parameter (must be boolean)".to_string(),
                })
            }
        })).await?;
        log::info!("Operation 'switch_light' bound successfully.");
    } else {
        log::error!("Operation 'switch_light' not found in submodel 'light'.");
        return Err(anyhow::anyhow!("Operation 'switch_light' not found"));
    }

    log::info!("Asset provider '{}' is running. Press Ctrl+C to exit.", ASSET_NAME);
    tokio::signal::ctrl_c().await?;
    log::info!("Ctrl+C received, shutting down...");

    manager.disconnect().await.map_err(|e| {
        log::error!("Failed to disconnect AssetManager: {:?}", e);
        anyhow::anyhow!("AssetManager disconnection failed: {:?}", e)
    })?;
    log::info!("AssetManager disconnected.");

    Ok(())
}
