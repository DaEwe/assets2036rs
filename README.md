# Submodel to Python Class Generator

## Overview

This command-line tool generates Python classes from a submodel definition provided in a JSON file. It helps developers create type-safe client (consumer) or server-side (provider) code for interacting with assets based on a defined submodel structure, enabling better code completion and reducing runtime errors.

## Features

*   Generates Python classes for both **consumer** and **provider** roles.
*   Parses submodel definitions including:
    *   Properties (defined as an object/dictionary, with types: string, integer, number, boolean, array, object)
    *   Operations (formerly methods, defined as an object/dictionary, with input parameters and responses)
    *   Events (defined as an object/dictionary, with parameters for payloads)
*   Creates type hints for properties, operation parameters, and return values.
*   Generates distinct nested Python classes for structured object properties.
*   Generates `typing.NamedTuple` classes for event payloads and complex operation responses.
*   Converts naming conventions from camelCase (common in JSON) to PascalCase for classes and snake\_case for methods/attributes in Python.
*   Uses Jinja2 templating for flexible and maintainable code generation.
*   Validates input submodel JSON against an official schema (`assets2036py/resources/submodel_schema.json`).

## Requirements

*   Python 3.7+
*   Jinja2 (for templating)
*   jsonschema (for validating input submodel definitions)
*   requests (for fetching submodels from URLs)

These dependencies are listed in `requirements.txt`.

## Setup & Installation

1.  Ensure Python 3.7+ is installed.
2.  Download (or clone) the `submodel_generator.py` script and the `templates` directory.
3.  (Recommended) Create a virtual environment:
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows: .venv\Scripts\activate
    ```
4.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
5.  Ensure you have your submodel definition file (e.g., the provided `placeholder_submodel.json`) available.

## Usage

The script is run from the command line:

```bash
python submodel_generator.py <submodel_file_path_or_url> --output-dir <directory_path> --role <consumer_or_provider>
```

**Arguments:**

*   `<submodel_file_path_or_url>`: (Required) Path to the local JSON file OR a publicly accessible HTTP/HTTPS URL pointing to the raw JSON submodel definition.
*   `--output-dir <directory_path>`: (Optional) The directory where the generated Python file(s) will be saved. Defaults to `generated_submodels`.
*   `--role <consumer_or_provider>`: (Required) Specifies whether to generate code for a `consumer` or a `provider`.

**Example Commands:**

Using a local file:
```bash
python submodel_generator.py placeholder_submodel.json --output-dir generated_code --role consumer
```

Using a URL:
```bash
python submodel_generator.py https://example.com/path/to/your_submodel.json --output-dir generated_code --role consumer
```
This command will fetch and read `placeholder_submodel.json` (or the specified URL), generate a consumer class, and save it in the `generated_code` directory.

## Input Submodel Definition File

The tool expects a submodel definition as a JSON object, either from a local file or fetched from a URL. **Crucially, this input JSON MUST conform to the schema located at `assets2036py/resources/submodel_schema.json` within this project.** This schema is the authoritative source for the expected structure. The `submodel_generator.py` script will validate your input against this schema before attempting code generation.

If providing a URL, it must be a direct link to the raw JSON content of the submodel definition.

Key top-level fields defined by the schema include:

*   `name` (string, required): The name of the submodel, used for class naming (e.g., "PlaceholderSubmodel").
*   `revision` (string, required): The revision of the submodel (e.g., "1.0.0").
*   `description` (string, optional): A human-readable description of the submodel.
*   `properties` (object, optional): An object where each key is a property name (camelCase recommended for consistency with common JSON practices, though the generator will convert to snake\_case for attributes). The value for each key is a `typeDescription` object.
    *   A `typeDescription` object specifies the `type` (e.g., "string", "integer", "object", "array") and can include other fields like `description`, `items` (for arrays), or nested property definitions (for objects, where keys are sub-property names and values are further `typeDescription` objects).
*   `operations` (object, optional): An object where keys are operation names. Each operation value defines:
    *   `description` (string, optional).
    *   `parameters` (object, optional): An object where keys are parameter names, and values are `typeDescription` objects.
    *   `response` (object, optional): A `typeDescription` object for the operation's response.
*   `events` (object, optional): An object where keys are event names. Each event value defines:
    *   `description` (string, optional).
    *   `parameters` (object, optional): An object where keys are parameter names for the event payload, and values are `typeDescription` objects.

The provided `placeholder_submodel.json` serves as a valid example conforming to this schema.

## Generated Code

*   **File Naming:** Generated files are named `<submodel_name_snake_case>_<role>.py` (e.g., `placeholder_submodel_consumer.py`).
*   **Class Naming:**
    *   The main generated class is named `<SubmodelNamePascalCase><Role>` (e.g., `PlaceholderSubmodelConsumer`).
    *   Properties of type 'object' with a defined structure (i.e., with their own sub-properties listed inline) generate their own nested Python classes (e.g., a property `configuration` of type `object` with fields `host` and `port` will result in a `Configuration` class). This applies recursively for deeply nested objects.
    *   Event payloads are generated as `typing.NamedTuple` classes for strong typing (e.g., an event `errorOccurred` will have an `ErrorOccurredPayload` NamedTuple).
    *   Complex operation responses (i.e., type `object` with defined fields) are also generated as `typing.NamedTuple` (e.g., an operation `get_config` returning an object with `host` and `port` will have a `GetConfigResponse` NamedTuple).
*   **Consumer Classes:**
    *   Properties are initialized with default values (e.g., `""` for strings, `0` for numbers, `False` for booleans, `[]` for lists, and recursively instantiated defaults for complex objects/NamedTuples) and include type hints.
    *   **Note on Type Checking**: You may notice `# type: ignore` comments on the lines where property values are accessed or set (e.g., `return self._prop_xyz.value  # type: ignore`). This is because the `value` attribute in the underlying `assets2036py.assets.Property` class is dynamically typed in a way that static analysis tools like MyPy may not fully resolve. The type hints on the generated accessor methods themselves (e.g., `def status(self) -> Optional[str]:`) provide the intended type safety for users of the generated classes.
    *   Methods for calling submodel operations are named `call_<operation_name_snake_case>(...)` and are `async` (as they typically involve I/O).
    *   Methods for registering event callbacks are named `on_<event_name_snake_case>(callback)`.
*   **Provider Classes:**
    *   Properties are initialized similarly. (This also applies to the "Note on Type Checking" mentioned above for consumer properties, especially for provider setters which might have `# type: ignore[assignment]`.)
    *   Methods directly implement the submodel operations (e.g., `get_config(...)`).
    *   Methods for triggering events are named `trigger_<event_name_snake_case>(...)`.

The generated code includes placeholder comments (`# TODO: ...`) where actual business logic or communication with an asset/service needs to be implemented.

## Running Tests

Unit tests are provided in `test_generated_code.py`. To run them:

1.  Ensure `placeholder_submodel.json` and `submodel_generator.py` are in the project root.
2.  Run the tests from the project root directory:
    ```bash
    python -m unittest test_generated_code.py
    ```

This will generate fresh code into a `test_generated_output` directory and run tests against it.

```
