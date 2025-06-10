# ASSETS2036 Libraries (Python & Rust)

This repository provides libraries for consuming and providing ASSETS2036 compliant assets in both Python and Rust. ASSETS2036 defines a standardized way for industrial assets to expose their services and data via MQTT.

## Python Library (`assets2036py`)

The Python library, `assets2036py`, offers a flexible way to interact with ASSETS2036 assets in Python environments.

### Quick Start (Python)

```python
from assets2036py import AssetManager

# For local MQTT broker with default port
mgr = AssetManager("localhost", 1883, "my_namespace", "my_python_endpoint")

# Example: Creating an asset with the standard light submodel
my_lamp = mgr.create_asset("my_lamp", "https://raw.githubusercontent.com/boschresearch/assets2036-submodels/master/light.json")

my_lamp.light.light_on.value = True
print(f"Lamp 'my_lamp' light_on property set to: {my_lamp.light.light_on.value}")

# Keep main thread alive to allow MQTT communication
try:
    while True:
        pass
except KeyboardInterrupt:
    print("Disconnecting Python asset manager...")
    mgr.disconnect()
```

For more detailed examples and usage, please refer to the code within the `assets2036py/` directory and the Python examples (typically found in an `examples/` directory or within tests if not standalone).

## Rust Library (`assets2036_rs`)

A new, idiomatic Rust implementation, `assets2036_rs`, is now available for building robust and performant ASSETS2036 applications. It leverages Rust's type system and async capabilities using Tokio.

### Key Features (Rust)
*   Asset and Submodel representation (Properties, Events, Operations).
*   Owner and Consumer modes.
*   Asynchronous communication via MQTT using an `AssetManager`.
*   Creation of local (owned) assets and proxies to remote (consumed) assets.
*   Submodel loading from JSON strings and URLs (`file://`, `http://`, `https://`).
*   Asset discovery (`query_assets`) and hierarchical asset relationships.
*   Integration with the `log` crate via `RustLoggingHandler`.
*   Health monitoring for the `AssetManager`.

### Quick Start (Rust)

For detailed usage, examples, and setup instructions for the Rust library, please see the **[assets2036_rs README](assets2036_rs/README.md)**.

A very brief example to create an asset provider:
```rust,ignore
// Add to your Cargo.toml:
// assets2036_rs = { path = "./assets2036_rs" } # Or appropriate git/version
// tokio = { version = "1", features = ["full"] }
// serde_json = "1.0"
// log = "0.4"
// env_logger = "0.10" // Corrected version from Cargo.toml

use assets2036_rs::{AssetManager, Mode};
use std::error::Error; // Using std::error::Error for Box<dyn Error>

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    let mut mgr = AssetManager::new(
        "localhost",
        1883,
        "my_rust_namespace",
        "my_rust_endpoint"
    );
    mgr.connect().await?;
    log::info!("AssetManager connected.");

    let light_submodel_json = r#"{
        "name": "light",
        "properties": { "light_on": { "type": "boolean", "readOnly": false } }
    }"#;

    let asset = mgr.create_asset(
        "my_rust_lamp",
        vec![light_submodel_json.to_string()],
        Mode::Owner
    ).await?;
    log::info!("Asset 'my_rust_lamp' created.");

    if let Some(sm) = asset.get_submodel("light") {
        if let Some(prop_arc) = sm.get_property("light_on") { // get_property returns Arc<Property>
            prop_arc.set_value(serde_json::json!(true)).await?;
            log::info!("Set light_on to true.");
        }
    }

    // Keep alive or do other work
    tokio::signal::ctrl_c().await?;
    log::info!("Shutting down...");
    mgr.disconnect().await?; // AssetManager::disconnect takes &self
    Ok(())
}
```

## Choosing a Library

*   **Python (`assets2036py`):** Suitable for existing Python projects, rapid prototyping, or scripting.
*   **Rust (`assets2036_rs`):** Recommended for new projects where performance, type safety, and concurrency are primary concerns, or for integration into the Rust ecosystem.

## Development Status

Both libraries are functional. The Rust library (`assets2036_rs`) is a newer addition and is under active development to achieve full feature parity and further enhancements.

## Contributing

Contributions to improve either library are welcome! Please feel free to open an issue or submit a pull request.

## License

This project is licensed under the Apache 2.0 License. (Assuming a `LICENSE` file exists at the root, or this statement needs adjustment per actual license).
