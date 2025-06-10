use assets2036_rs::{AssetManager, Error};
use serde_json::{json, Value};
use tokio::time::{sleep, Duration};
use std::collections::HashMap;

// Configuration
const BROKER_HOST: &str = "localhost";
const BROKER_PORT: u16 = 1883;
const NAMESPACE: &str = "org_assets2036_example"; // Same namespace as the provider
const ASSET_NAME: &str = "lamp_1"; // The asset we want to consume
const ENDPOINT_NAME: &str = "rust_consumer_endpoint";

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Starting asset consumer example...");

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
    log::info!("Consumer AssetManager connected to {} at {}:{}", NAMESPACE, BROKER_HOST, BROKER_PORT);

    // Initialize the logging handler for the manager
    match manager.get_logging_handler() {
        Ok(handler) => {
            if let Err(e) = log::set_boxed_logger(Box::new(handler)){
                 log::warn!("Failed to set logger for AssetManager: {:?} (possibly already set)",e);
            }
            log::set_max_level(log::LevelFilter::Info);
            log::info!("Consumer AssetManager logging handler initialized.");
        }
        Err(e) => {
            log::error!("Failed to get logging handler: {:?}", e);
        }
    }

    log::info!("Attempting to create proxy for asset '{}/{}'", NAMESPACE, ASSET_NAME);
    let lamp_proxy = manager.create_asset_proxy(
        NAMESPACE.to_string(),
        ASSET_NAME.to_string(),
        Some(10), // Wait up to 10 seconds for the asset to be online
    ).await.map_err(|e| {
        log::error!("Failed to create asset proxy for '{}/{}': {:?}", NAMESPACE, ASSET_NAME, e);
        anyhow::anyhow!("Asset proxy creation failed for '{}/{}': {:?}", NAMESPACE, ASSET_NAME, e)
    })?;
    log::info!("Asset proxy for '{}/{}' created successfully.", NAMESPACE, ASSET_NAME);

    if let Some(light_sm) = lamp_proxy.get_submodel("light") {
        // Subscribe to the 'light_switched' event
        if let Some(light_switched_event_arc) = light_sm.get_event("light_switched") {
            light_switched_event_arc.on_event(Box::new(|params: HashMap<String, Value>, timestamp: i64| {
                log::info!("[Event Callback] 'light_switched' event received at {}: {:?}", timestamp, params);
            })).await?;
            log::info!("Subscribed to 'light_switched' event.");
        } else {
            log::warn!("Event 'light_switched' not found on proxy.");
        }

        // Subscribe to the 'light_on' property changes
        if let Some(light_on_prop_arc) = light_sm.get_property("light_on") {
            light_on_prop_arc.on_change(Box::new(|new_value: Value| {
                log::info!("[Property Callback] 'light_on' property changed to: {}", new_value);
            })).await?;
            log::info!("Subscribed to 'light_on' property changes.");

            // Initial read after ensuring subscription is active
            sleep(Duration::from_millis(100)).await; // Give a moment for initial state to arrive
            let initial_state = light_on_prop_arc.get_value().await;
            log::info!("Initial 'light_on' state: {}", initial_state);
        } else {
            log::warn!("Property 'light_on' not found on proxy.");
        }

        // Periodically invoke the 'switch_light' operation and read property
        let mut current_target_state = true; // Start by trying to turn it on
        if let Some(switch_light_op_arc) = light_sm.get_operation("switch_light") {
            for i in 0..5 { // Loop a few times
                log::info!("Attempting to invoke 'switch_light' with state: {}", current_target_state);
                match switch_light_op_arc.invoke(json!({"state": current_target_state})).await {
                    Ok(response) => log::info!("'switch_light' invoked successfully. Response: {:?}", response),
                    Err(e) => log::error!("Error invoking 'switch_light': {:?}", e),
                }

                current_target_state = !current_target_state; // Toggle for next iteration

                sleep(Duration::from_secs(2)).await; // Wait before next action

                if let Some(prop_arc) = light_sm.get_property("light_on") {
                     log::info!("Polling 'light_on' property: {}", prop_arc.get_value().await);
                }
                if i == 4 { // On the last iteration, ensure we try to turn it off
                    if current_target_state == true { // if it would be true for next, make it false
                         log::info!("Final attempt: ensuring light is switched OFF.");
                         match switch_light_op_arc.invoke(json!({"state": false})).await {
                            Ok(response) => log::info!("'switch_light' (to off) invoked successfully. Response: {:?}", response),
                            Err(e) => log::error!("Error invoking 'switch_light' (to off): {:?}", e),
                        }
                    }
                }

            }
        } else {
            log::warn!("Operation 'switch_light' not found on proxy.");
        }

    } else {
        log::error!("Submodel 'light' not found on asset proxy '{}/{}'. Cannot interact further.", NAMESPACE, ASSET_NAME);
        return Err(anyhow::anyhow!("Submodel 'light' not found on proxy"));
    }

    log::info!("Consumer example finished its interactions. Waiting for Ctrl+C to exit or program end.");
    // In a real app, might loop indefinitely or handle other inputs.
    // For this example, we'll let it run for a bit then disconnect.
    sleep(Duration::from_secs(5)).await;


    manager.disconnect().await.map_err(|e| {
        log::error!("Failed to disconnect Consumer AssetManager: {:?}", e);
        anyhow::anyhow!("Consumer AssetManager disconnection failed: {:?}", e)
    })?;
    log::info!("Consumer AssetManager disconnected.");

    Ok(())
}
