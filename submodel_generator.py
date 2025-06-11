import argparse
import json
import os
import re
from typing import List, Callable, Optional, NamedTuple, Any, Dict # Keep existing typings
import jsonschema
from jinja2 import Environment, FileSystemLoader # Add Jinja2
import requests  # New import
from urllib.parse import urlparse # New import
from assets2036py.utilities import sanitize # New import

# --- Helper functions for name conversion ---
def camel_to_snake(name):
    """Converts camelCase to snake_case."""
    if not name: return ""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def camel_to_pascal(name):
    """Converts camelCase or snake_case to PascalCase."""
    if not name: return "Unnamed"
    if "_" in name: # Likely snake_case
        return "".join(word.capitalize() or "_" for word in name.split('_'))
    return name[0].upper() + name[1:]

# --- Global list to track generated class names for type mapping ---
_generated_classes_registry = set()

# --- Helper function for type mapping ---
def map_type_to_python(type_desc: Dict[str, Any], object_name_hint: Optional[str] = None) -> str:
    if not isinstance(type_desc, dict):
        print(f"Warning: Invalid type_desc passed to map_type_to_python: {type_desc}. Defaulting to 'Any'.")
        return "Any"
    submodel_type = type_desc.get('type')
    if submodel_type == "string": return "str"
    if submodel_type == "integer": return "int"
    if submodel_type == "number": return "float"
    if submodel_type == "boolean": return "bool"
    if submodel_type == "array":
        items_desc = type_desc.get('items')
        item_py_type = map_type_to_python(items_desc) if items_desc else "Any"
        return f"List[{item_py_type}]"
    if submodel_type == "object":
        if object_name_hint:
            potential_class_name = camel_to_pascal(object_name_hint)
            inline_properties = {k: v for k, v in type_desc.items() if k not in ['type', 'description', 'items']}
            if inline_properties and potential_class_name in _generated_classes_registry:
                return potential_class_name
        return "Dict[str, Any]"
    if not submodel_type:
        if len(type_desc.keys() - {'description', 'items'}) > 0 and object_name_hint:
            potential_class_name = camel_to_pascal(object_name_hint)
            if potential_class_name in _generated_classes_registry:
                return potential_class_name
        print(f"Warning: Submodel type key 'type' is missing or not recognized in type_desc for '{object_name_hint}': {type_desc}. Defaulting to Dict or Any.")
        return "Dict[str, Any]" if len(type_desc.keys() - {'description', 'items'}) > 0 else "Any"
    print(f"Warning: Unknown submodel_type '{submodel_type}' in type_desc for '{object_name_hint}'. Defaulting to 'Any'.")
    return "Any"

# --- Helper function for default values ---
def get_default_value_for_type(
    py_type_str: str,
    all_generated_type_definitions: Optional[Dict[str, Dict[str, Any]]] = None
) -> str:
    if py_type_str == "str": return "\"\""
    if py_type_str == "int": return "0"
    if py_type_str == "float": return "0.0"
    if py_type_str == "bool": return "False"
    if py_type_str.startswith("List["): return "[]"
    if py_type_str.startswith("Dict["): return "{}"
    if py_type_str in _generated_classes_registry:
        if all_generated_type_definitions and py_type_str in all_generated_type_definitions:
            type_def = all_generated_type_definitions[py_type_str]
            params_or_props = None
            if 'params' in type_def: params_or_props = type_def['params']
            elif 'properties' in type_def: params_or_props = type_def['properties']
            if params_or_props is not None:
                args = []
                for param in params_or_props:
                    param_default_val = param.get("default_value")
                    if param_default_val is None:
                        param_default_val = get_default_value_for_type(param['type_py'], all_generated_type_definitions)
                    args.append(f"{param['name_snake']}={param_default_val}")
                return f"{py_type_str}({', '.join(args)})"
        return f"{py_type_str}()"
    return "None"

# --- Recursive helper for nested object class definitions ---
def _generate_nested_object_class_definitions(
    name: str, details: Dict[str, Any],
    context_nested_classes: List[Dict[str, Any]], registry: set, depth: int = 0
):
    class_name_pascal = camel_to_pascal(name)
    sub_properties_desc = {k: v for k, v in details.items() if k not in ['type', 'description', 'items']}
    if not sub_properties_desc and details.get('type') == 'object': pass
    elif not sub_properties_desc: return
    if class_name_pascal in registry: return
    registry.add(class_name_pascal)
    class_def = {"name": class_name_pascal, "properties": []}
    if sub_properties_desc:
        for sub_name, sub_details in sub_properties_desc.items():
            if not isinstance(sub_details, dict):
                print(f"Warning: Expected dictionary for sub-property '{sub_name}' in '{class_name_pascal}', got {type(sub_details)}. Skipping.")
                continue
            if sub_details.get('type') == 'object':
                _generate_nested_object_class_definitions(sub_name, sub_details, context_nested_classes, registry, depth + 1)
            mapped_type = map_type_to_python(sub_details, object_name_hint=sub_name)
            # Pass all_generated_type_definitions (which is not available here) or ensure it's handled by simpler default for nested objects' sub-properties
            class_def["properties"].append({
                "name_snake": camel_to_snake(sub_name),
                "type_py": mapped_type,
                "default_value": get_default_value_for_type(mapped_type) # Simplified for this context; enhance if nested defaults need full context
            })
    if not any(nc['name'] == class_name_pascal for nc in context_nested_classes):
        context_nested_classes.append(class_def)

def generate_code(submodel_data, role, output_dir):
    print("DEBUG: Entered generate_code (Corrected Context Version)")
    _generated_classes_registry.clear()
    env = Environment(loader=FileSystemLoader("templates"), trim_blocks=False, lstrip_blocks=False)

    submodel_name_from_data = submodel_data['name']
    submodel_revision = submodel_data.get('revision', "N/A")
    submodel_name_pascal = camel_to_pascal(submodel_name_from_data)

    submodel_name_for_topic_and_init_body = sanitize(submodel_name_from_data)
    # print(f"DEBUG: Original name: '{submodel_name_from_data}', Sanitized for topic/init: '{submodel_name_for_topic_and_init_body}'")

    context = {
        "role": role,
        "parent_asset_param_name_for_init_signature": "parent_asset",
        "submodel_name_param_name_for_init_signature": "submodel_name",
        "submodel_name_for_topic_and_init_body": submodel_name_for_topic_and_init_body,
        "submodel_original_name": submodel_name_from_data,
        "submodel_revision": submodel_revision,
        "submodel_class_name": f"{submodel_name_pascal}{role.capitalize()}",
        "properties": [], "operations": [], "events": [],
        "nested_classes": [], "event_payloads": [], "operation_responses": []
    }

    for prop_name, prop_details in submodel_data.get('properties', {}).items():
        if prop_details.get('type') == 'object':
            try:
                _generate_nested_object_class_definitions(prop_name, prop_details, context["nested_classes"], _generated_classes_registry, depth=0)
            except Exception as e_gnocd_call:
                print(f"ERROR calling _generate_nested_object_class_definitions for '{prop_name}': {e_gnocd_call}")

    for event_name, event_details in submodel_data.get('events', {}).items():
        event_params_desc = event_details.get('parameters', {})
        if event_params_desc:
            payload_name_pascal = f"{camel_to_pascal(event_name)}Payload"
            _generated_classes_registry.add(payload_name_pascal)
            payload_def = {"name": payload_name_pascal, "params": []}
            for param_name, param_details in event_params_desc.items():
                payload_def["params"].append({
                    "name_snake": camel_to_snake(param_name),
                    "type_py": map_type_to_python(param_details, object_name_hint=param_name)
                })
            context["event_payloads"].append(payload_def)

    for op_name, op_details in submodel_data.get('operations', {}).items():
        response_desc = op_details.get('response')
        if response_desc and response_desc.get('type') == 'object':
            response_sub_props = {k:v for k,v in response_desc.items() if k not in ['type', 'description', 'items']}
            if response_sub_props:
                response_name_pascal = f"{camel_to_pascal(op_name)}Response"
                _generated_classes_registry.add(response_name_pascal)
                response_def = {"name": response_name_pascal, "params": [], "is_complex_object": True}
                for param_name, param_details in response_sub_props.items():
                    response_def["params"].append({
                        "name_snake": camel_to_snake(param_name),
                        "type_py": map_type_to_python(param_details, object_name_hint=param_name)
                    })
                context["operation_responses"].append(response_def)

    all_generated_type_definitions = {}
    for item_def in context["nested_classes"]:
        all_generated_type_definitions[item_def['name']] = {"properties": item_def['properties']}
    for item_def in context["event_payloads"]:
        all_generated_type_definitions[item_def['name']] = {"params": item_def['params']}
    for item_def in context["operation_responses"]:
        all_generated_type_definitions[item_def['name']] = {"params": item_def['params']}

    for prop_name, prop_details in submodel_data.get('properties', {}).items():
        prop_name_snake = camel_to_snake(prop_name)
        if not prop_name_snake: continue
        py_type = map_type_to_python(prop_details, object_name_hint=prop_name)
        schema_as_dict_str = json.dumps(prop_details)
        context["properties"].append({
            "name": prop_name,
            "name_snake": prop_name_snake,
            "name_pascal": camel_to_pascal(prop_name),
            "python_type": py_type,
            "default_value": get_default_value_for_type(py_type, all_generated_type_definitions),
            "schema_dict_str": schema_as_dict_str
        })

    print("DEBUG_OPS: --- Start Populating Operations Context ---") # DEBUG
    for op_name, op_details in submodel_data.get('operations', {}).items():
        op_name_snake = camel_to_snake(op_name)
        if not op_name_snake:
            print(f"DEBUG_OPS: Skipping operation with empty snake_case name (original: '{op_name}')") # DEBUG
            continue

        print(f"DEBUG_OPS: Processing operation '{op_name}' (snake_case: '{op_name_snake}')") # DEBUG

        params_list = []
        if op_details.get('parameters'):
            for param_name, param_details_desc in op_details['parameters'].items():
                params_list.append({
                    "name_snake": camel_to_snake(param_name),
                    "type_py": map_type_to_python(param_details_desc, object_name_hint=param_name)
                })
        # print(f"DEBUG_OPS:   Operation '{op_name}', params_list='{params_list}'") # Verbose

        return_type_str = "None"
        return_default_val_str = "None"
        response_desc = op_details.get('response')
        if response_desc:
            return_object_hint = f"{camel_to_pascal(op_name)}Response"
            return_type_str = map_type_to_python(response_desc, object_name_hint=return_object_hint)
            return_default_val_str = get_default_value_for_type(return_type_str, all_generated_type_definitions)
        # print(f"DEBUG_OPS:   Operation '{op_name}', return_type='{return_type_str}', default='{return_default_val_str}'") # Verbose

        current_op_def_str = json.dumps(op_details)
        # print(f"DEBUG_OPS:   Operation '{op_name}', definition_str length='{len(current_op_def_str)}'") # Verbose

        context["operations"].append({
            "name": op_name, # Original name
            "name_snake": op_name_snake,
            "name_pascal": camel_to_pascal(op_name), # PascalCase name
            "operation_definition_str": current_op_def_str, # Full schema of the operation
            "params": params_list, # List of param dicts for Jinja template
            "return_type": return_type_str, # Python type hint for return
            "return_default_value": return_default_val_str # Default value string
        })
    print("DEBUG_OPS: --- Finished Populating Operations Context ---") # DEBUG

    print("DEBUG_EVENTS: --- Start Populating Events Context ---") # DEBUG
    for event_name, event_details in submodel_data.get('events', {}).items():
        event_name_snake = camel_to_snake(event_name)
        if not event_name_snake:
            print(f"DEBUG_EVENTS: Skipping event with empty snake_case name (original: '{event_name}')") # DEBUG
            continue

        print(f"DEBUG_EVENTS: Processing event '{event_name}' (snake_case: '{event_name_snake}')") # DEBUG

        payload_class_name_pascal = f"{camel_to_pascal(event_name)}Payload"
        actual_payload_type = "None"
        payload_params_list = [] # Renamed from trigger_params for context clarity

        if event_details.get('parameters'):
            actual_payload_type = payload_class_name_pascal if payload_class_name_pascal in _generated_classes_registry else "Dict[str, Any]"
            # Only populate payload_params_list if a specific NamedTuple is expected to be used for the payload
            if actual_payload_type == payload_class_name_pascal: # This means it's a known, registered NamedTuple
                for param_name, param_details in event_details['parameters'].items():
                    param_name_snake = camel_to_snake(param_name)
                    param_py_type = map_type_to_python(param_details, object_name_hint=param_name)
                    payload_params_list.append({
                        "name_snake": param_name_snake,
                        "type_py": param_py_type
                        # Default values are not typically part of event trigger signatures this way,
                        # but are handled by NamedTuple creation if needed.
                    })

        print(f"DEBUG_EVENTS:   Event '{event_name}', payload_type='{actual_payload_type}', payload_params_list='{payload_params_list}'") # DEBUG

        context["events"].append({
            "name": event_name, # Original name
            "name_snake": event_name_snake,
            "name_pascal": camel_to_pascal(event_name), # PascalCase name
            "event_definition_str": json.dumps(event_details), # Full schema of the event
            "payload_type": actual_payload_type, # Python type hint for the callback/NamedTuple
            "payload_params": payload_params_list # List of params for provider's trigger method signature
                                                # and for consumer's on_event type hint if specific.
        })
    print("DEBUG_EVENTS: --- Finished Populating Events Context ---") # DEBUG

    template_name = f"{role}.py.j2"
    try:
        template = env.get_template(template_name)
        generated_code_str = template.render(context)
    except Exception as e:
        print(f"ERROR rendering Jinja2 template {template_name}: {e}")
        return

    output_filename_base = camel_to_snake(submodel_name_pascal)
    if not output_filename_base: output_filename_base = "default_generated_submodel_name"
    output_filename = f"{output_filename_base}_{role}.py"
    output_filepath = os.path.join(output_dir, output_filename)
    os.makedirs(output_dir, exist_ok=True)

    try:
        with open(output_filepath, 'w') as f:
            f.write(generated_code_str)
        print(f"Generated code for {role} at {output_filepath} (Corrected Context Version)")
    except IOError as e:
        print(f"ERROR: Could not write generated file. Details: {e}")

def main():
    parser = argparse.ArgumentParser(description='Generate Python classes from a submodel definition.')
    parser.add_argument('submodel_file', type=str, help='Path to the submodel definition JSON file.')
    parser.add_argument('--output-dir', type=str, default='generated_submodels', help='Directory to save generated Python files.')
    parser.add_argument('--role', type=str, choices=['consumer', 'provider'], required=True, help='Role to generate code for (consumer or provider).')
    args = parser.parse_args()

    schema_filepath = "assets2036py/resources/submodel_schema.json"
    submodel_json_schema = None
    try:
        with open(schema_filepath, 'r') as f_schema:
            submodel_json_schema = json.load(f_schema)
    except FileNotFoundError:
        print(f"Error: JSON Schema file not found at {schema_filepath}")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON Schema from {schema_filepath}")
        return
    except Exception as e:
        print(f"Error: An unexpected error occurred while loading the JSON schema: {e}")
        return

    input_source = args.submodel_file
    is_url = False
    submodel_data_content = None
    submodel_data = None

    try:
        parsed_url = urlparse(input_source)
        if parsed_url.scheme in ['http', 'https']:
            is_url = True
            # print(f"DEBUG_URL: Input source '{input_source}' is identified as a URL.")
        # else:
            # print(f"DEBUG_URL: Input source '{input_source}' is identified as a local file path.")
    except Exception as e_urlparse:
        # print(f"DEBUG_URL: Could not parse '{input_source}' as URL (Error: {e_urlparse}), treating as local file path.")
        is_url = False

    if is_url:
        # print(f"DEBUG_URL: Attempting to fetch submodel from URL: {input_source}")
        try:
            response = requests.get(input_source, timeout=10)
            response.raise_for_status()
            submodel_data_content = response.text
            # print(f"DEBUG_URL: Successfully fetched content from URL.")
        except requests.exceptions.HTTPError as e_http:
            print(f"Error: HTTP error fetching submodel from URL '{input_source}'. Status code: {e_http.response.status_code}. Response: {e_http.response.text[:200]}")
            return
        except requests.exceptions.ConnectionError as e_conn:
            print(f"Error: Connection error fetching submodel from URL '{input_source}'. Details: {e_conn}")
            return
        except requests.exceptions.Timeout as e_timeout:
            print(f"Error: Timeout while fetching submodel from URL '{input_source}'. Details: {e_timeout}")
            return
        except requests.exceptions.RequestException as e_req:
            print(f"Error: An error occurred fetching submodel from URL '{input_source}'. Details: {e_req}")
            return
    else:
        try:
            with open(input_source, 'r') as f_data:
                submodel_data_content = f_data.read()
            # print(f"DEBUG_URL: Successfully read content from local file: {input_source}")
        except FileNotFoundError:
            print(f"Error: Submodel file not found at {input_source}")
            return
        except Exception as e:
            print(f"Error: An unexpected error occurred while reading local submodel file: {e}")
            return

    if submodel_data_content is None:
        print("Error: Submodel data content could not be loaded/fetched. Exiting.")
        return

    try:
        submodel_data = json.loads(submodel_data_content)
        # print("DEBUG_URL: Successfully parsed JSON content into submodel_data.")
    except json.JSONDecodeError as e_json:
        print(f"Error: Could not decode JSON from the input source '{input_source}'. Details: {e_json}")
        return

    if submodel_data is None:
        print(f"Error: Failed to load or parse submodel_data from '{input_source}'. Exiting.")
        return

    try:
        validator = jsonschema.Draft7Validator(submodel_json_schema)
        errors = sorted(validator.iter_errors(submodel_data), key=lambda e: e.path)
        if errors:
            print(f"Error: Submodel data from '{input_source}' is not valid according to the schema '{schema_filepath}'.")
            for error in errors:
                print(f"- Validation Error: {'/'.join(map(str, error.path))} - {error.message}")
            return
        else:
            print(f"Submodel data from '{input_source}' successfully validated against the schema.")
    except jsonschema.exceptions.SchemaError as e:
        print(f"Error: The JSON schema '{schema_filepath}' itself is invalid. Details: {e}")
        return
    except Exception as e:
        print(f"Error: An unexpected error occurred during schema validation: {e}")
        return

    generate_code(submodel_data, args.role, args.output_dir)

if __name__ == '__main__':
    main()
