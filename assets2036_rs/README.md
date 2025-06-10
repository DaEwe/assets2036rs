# assets2036_rs

## Overview

ASSETS2036 is a standardized framework for interoperable, modular, and sustainable automation components. This Rust library, `assets2036_rs`, provides an idiomatic Rust implementation for creating and interacting with ASSETS2036-compliant digital representations of assets.

The primary goal is to offer a robust and efficient way to develop asset providers (owners) and consumers within the ASSETS2036 ecosystem, leveraging Rust's safety and performance features. The library is currently under development, with a focus on implementing core ASSETS2036 functionalities.

## Features

*   **Asset and Submodel Representation:** Defines structures for Assets, Submodels, and their elements: Properties, Events, and Operations, aligning with ASSETS2036 specifications.
*   **Owner and Consumer Modes:** Supports both creating and managing local assets (`Mode::Owner`) and interacting with remote assets (`Mode::Consumer`).
*   **Communication via MQTT:** Utilizes MQTT as the primary communication backend, managed through the `AssetManager`.
*   **Local Asset Creation:** Allows for the creation of owned assets, publishing their structure and state.
*   **Remote Asset Proxies:** Enables creating proxies to remote assets, allowing consumption of their data and services.
*   **Submodel Loading:** Supports loading submodel definitions from:
    *   Direct JSON strings.
    *   `file://` URLs.
    *   `http://` and `https://` URLs.
*   **Asset Discovery:** Provides functionality to query for assets based on implemented submodels (via `AssetManager::query_assets`). (Note: Full discovery mechanisms are still evolving).
*   **Hierarchical Asset Relationships:** Supports creating parent-child relationships between assets using the standard `_relations` submodel and its `belongs_to` property. Includes `Asset::create_child_asset` and `Asset::get_child_assets`.
*   **Logging Integration:** Integrates with the `log` crate via `RustLoggingHandler`, allowing application logs to be emitted as ASSETS2036 `log_entry` events from the `AssetManager`'s `_endpoint`.
*   **Health Monitoring:** `AssetManager` includes a health monitoring mechanism that can periodically update its `_endpoint/healthy` status and optionally exit the application on unhealthy state.
*   **Asynchronous API:** Built on Tokio, providing a fully asynchronous API for non-blocking operations.

## Getting Started / Basic Usage

### Adding to `Cargo.toml`

To use `assets2036_rs` in your project, add it as a dependency in your `Cargo.toml`:

```toml
[dependencies]
assets2036_rs = { git = "URL_TO_THIS_REPO", branch = "main" } # Replace with actual repo URL and branch/tag/version
tokio = { version = "1", features = ["full"] }
log = "0.4"
serde_json = "1.0"
# env_logger = "0.10" # Example for setting up logging
# url = "2" # If you work with URLs directly for submodel sources
```
*(Note: Replace `URL_TO_THIS_REPO` and `branch` with the actual Git repository details or crate version once published.)*

### Simple Owner Example (Asset Provider)

This example demonstrates how to create a simple asset that owns a submodel with a writable property.

```rust
use assets2036_rs::{AssetManager, Asset, Mode, Error};
use serde_json::json;
use std::sync::Arc; // Required if creating assets directly with shared comm client

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Initialize logging (optional, for seeing library logs)
    // env_logger::init(); // Or your preferred logger

    // 1. Create an AssetManager
    let mut manager = AssetManager::new(
        "localhost",      // MQTT broker host
        1883,             // MQTT broker port
        "test_namespace", // Namespace for this manager and its assets
        "my_owner_manager"// Endpoint name for this manager
    );

    // 2. Connect the manager to the broker
    manager.connect().await?;
    println!("AssetManager connected.");

    // 3. Define a simple submodel JSON
    let thermostat_sm_json = r#"
    {
        "name": "Thermostat",
        "properties": {
            "targetTemperature": {
                "type": "number",
                "description": "Desired temperature in Celsius"
            }
        },
        "operations": {
            "getFirmwareVersion": {
                "response": {"type": "string"}
            }
        },
        "events": {
            "temperatureThresholdReached": {
                "parameters": {"level": {"type": "string"}}
            }
        }
    }
    "#;

    // 4. Create an asset in Owner mode
    let mut thermostat_asset = manager.create_asset(
        "MyThermostat1",
        vec![thermostat_sm_json.to_string()],
        Mode::Owner
    ).await?;
    println!("Asset 'MyThermostat1' created.");

    // 5. Get a property and set its value
    if let Some(thermostat_sm) = thermostat_asset.get_submodel("Thermostat") {
        if let Some(temp_prop_arc) = thermostat_sm.get_property("targetTemperature") {
            temp_prop_arc.set_value(json!(22.5)).await?;
            println!("Set targetTemperature to 22.5");
        }

        // 6. Bind an operation handler
        if let Some(fw_op_arc) = thermostat_sm.get_operation("getFirmwareVersion") {
            fw_op_arc.bind(Box::new(|_params| {
                Ok(json!("1.2.3"))
            })).await?;
            println!("Bound getFirmwareVersion operation.");
        }

        // 7. Trigger an event
        if let Some(event_arc) = thermostat_sm.get_event("temperatureThresholdReached") {
            let params = json!({"level": "high"});
            event_arc.trigger(params).await?;
            println!("Triggered temperatureThresholdReached event.");
        }
    }

    println!("Asset 'MyThermostat1' is running. Press Ctrl+C to exit.");
    tokio::signal::ctrl_c().await?; // Keep alive until Ctrl+C

    manager.disconnect().await?;
    println!("AssetManager disconnected.");
    Ok(())
}
```

### Simple Consumer Example (Asset Consumer)

This example shows how to create a proxy to the `MyThermostat1` asset created above.

```rust
use assets2036_rs::{AssetManager, Asset, Error};
use serde_json::json;
use tokio::time::{sleep, Duration};
use std::collections::HashMap; // For event parameters

#[tokio::main]
async fn main() -> Result<(), Error> {
    // env_logger::init();

    // 1. Create an AssetManager
    let mut manager = AssetManager::new(
        "localhost",
        1883,
        "test_namespace", // Same namespace as the owner
        "my_consumer_manager"
    );

    // 2. Connect the manager
    manager.connect().await?;
    println!("Consumer AssetManager connected.");

    // 3. Create a proxy to the remote asset, wait up to 5 seconds for it to be online
    let thermostat_proxy = manager.create_asset_proxy(
        "test_namespace".to_string(), // Namespace of the target asset
        "MyThermostat1".to_string(),   // Name of the target asset
        Some(5)                       // Wait up to 5 seconds for online status
    ).await?;
    println!("Proxy to 'MyThermostat1' created.");

    if let Some(thermostat_sm) = thermostat_proxy.get_submodel("Thermostat") {
        // 4. Get a property and read its value
        if let Some(temp_prop_arc) = thermostat_sm.get_property("targetTemperature") {
            // Subscribe to changes to ensure the property value is populated
            temp_prop_arc.on_change(Box::new(|new_value| {
                println!("Thermostat targetTemperature changed (via on_change): {}", new_value);
            })).await?;

            // Give some time for initial value to arrive via subscription
            sleep(Duration::from_millis(200)).await;
            let current_temp = temp_prop_arc.get_value().await;
            println!("Current targetTemperature (polled): {}", current_temp);
        }

        // 5. Invoke an operation
        if let Some(fw_op_arc) = thermostat_sm.get_operation("getFirmwareVersion") {
            match fw_op_arc.invoke(json!({})).await {
                Ok(response) => println!("Firmware version: {}", response),
                Err(e) => eprintln!("Error invoking getFirmwareVersion: {:?}", e),
            }
        }

        // 6. Subscribe to an event
        if let Some(event_arc) = thermostat_sm.get_event("temperatureThresholdReached") {
            event_arc.on_event(Box::new(|params: HashMap<String, Value>, timestamp: i64| {
                println!("Received temperatureThresholdReached event at {}: {:?}", timestamp, params);
            })).await?;
            println!("Subscribed to temperatureThresholdReached event.");
        }
    }

    println!("Consumer is running. Press Ctrl+C to exit.");
    tokio::signal::ctrl_c().await?;

    manager.disconnect().await?;
    println!("Consumer AssetManager disconnected.");
    Ok(())
}
```

## Key Types

*   [`AssetManager`](src/lib.rs): The main entry point for managing asset connections and creation.
*   [`Asset`](src/lib.rs): Represents a physical or logical asset.
*   [`SubModel`](src/lib.rs): A component of an Asset, grouping related properties, events, and operations.
*   [`Property`](src/lib.rs): A data point of an Asset, can be read, written (if writable), or subscribed to.
*   [`Event`](src/lib.rs): Represents a notification that an Asset can emit.
*   [`Operation`](src/lib.rs): A function that can be invoked on an Asset.

## Error Handling

Most operations in this library that can fail return a `Result<T, assets2036_rs::Error>`. The [`Error`](src/error.rs) enum provides specific variants for different failure conditions, such as communication issues, data validation problems, timeouts, etc. This allows for robust error handling by applications.

## Running Examples

(Placeholder) Check the `examples/` directory in this crate for runnable code showcasing various features. (This directory would need to be created with example files).

## Contributing

Contributions are welcome! Please feel free to open an issue to discuss a feature or bug, or submit a pull request.

## License

This project is licensed under the Apache License 2.0. See the `LICENSE` file (if included, or assume standard Apache 2.0 terms) for details. This is based on the licensing of the related ASSETS2036 Python project.
