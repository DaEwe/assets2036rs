# Submodel to Python Class Generator

## Overview

This command-line tool generates Python classes from a submodel definition provided in a JSON file. It helps developers create type-safe client (consumer) or server-side (provider) code for interacting with assets based on a defined submodel structure, enabling better code completion and reducing runtime errors.

## Features

*   Generates Python classes for both **consumer** and **provider** roles.
*   Parses submodel definitions including:
    *   Properties (with types: string, integer, number, boolean, array, object)
    *   Methods (with input and output parameters)
    *   Events (with payloads)
*   Creates type hints for properties, method parameters, and return values.
*   Generates distinct nested classes for complex object properties and event payloads.
*   Converts naming conventions from camelCase (common in JSON) to PascalCase for classes and snake\_case for methods and attributes in Python.

## Requirements

*   Python 3.7+ (due to `typing.NamedTuple` and f-string usage, general modern Python features)
*   No external libraries are required to run the generator itself, beyond standard Python modules.

## Installation

1.  Download the `submodel_generator.py` script.
2.  Ensure you have the `placeholder_submodel.json` (or your own submodel definition file) available.

## Usage

The script is run from the command line:

```bash
python submodel_generator.py <submodel_file_path> --output-dir <directory_path> --role <consumer_or_provider>
```

**Arguments:**

*   `submodel_file_path`: (Required) Path to the JSON file containing the submodel definition.
*   `--output-dir <directory_path>`: (Optional) The directory where the generated Python file(s) will be saved. Defaults to `generated_submodels`.
*   `--role <consumer_or_provider>`: (Required) Specifies whether to generate code for a `consumer` or a `provider`.

**Example Command:**

```bash
python submodel_generator.py placeholder_submodel.json --output-dir generated_code --role consumer
```
This command will read `placeholder_submodel.json`, generate a consumer class, and save it in the `generated_code` directory.

## Input Submodel Definition File

The tool expects a JSON file describing the submodel. Key fields include:

*   `id` (string): A unique identifier for the submodel.
*   `name` (string, optional): The name of the submodel, used for class naming. Falls back to `idShort` or derivation from `id`.
*   `idShort` (string, optional): A short identifier, can be used for class naming if `name` is absent.
*   `description` (string, optional): A description of the submodel.
*   `properties` (array, optional): A list of property definitions. Each property has:
    *   `name` (string): Name of the property.
    *   `type` (string): Data type (e.g., `string`, `integer`, `boolean`, `array`, `object`).
    *   `items` (object, if type is `array`): Defines the type of items in the array.
    *   `properties` (array, if type is `object`): Defines sub-properties for nested objects.
*   `methods` (array, optional): A list of method definitions. Each method has:
    *   `name` (string): Name of the method.
    *   `inputs` (array, optional): List of input parameters.
    *   `outputs` (array, optional): List of output parameters.
*   `events` (array, optional): A list of event definitions. Each event has:
    *   `name` (string): Name of the event.
    *   `payload` (object, optional): Definition of the event's payload structure (typically an object with properties).

Refer to `placeholder_submodel.json` for a concrete example of the expected structure.

## Generated Code

*   **File Naming:** Generated files are named `<submodel_name_snake_case>_<role>.py` (e.g., `placeholder_submodel_consumer.py`).
*   **Class Naming:** The main generated class is named `<SubmodelNamePascalCase><Role>` (e.g., `PlaceholderSubmodelConsumer`). Nested object properties and event payloads also get their own generated classes/NamedTuples.
*   **Consumer Classes:**
    *   Properties are initialized with default values and include type hints.
    *   Methods for calling submodel operations are named `call_<method_name>(...)` and are `async` (as they typically involve I/O).
    *   Methods for registering event callbacks are named `on_<event_name>(callback)`.
*   **Provider Classes:**
    *   Properties are initialized similarly.
    *   Methods directly implement the submodel operations (e.g., `start(...)`).
    *   Methods for triggering events are named `trigger_<event_name>(...)`.

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
