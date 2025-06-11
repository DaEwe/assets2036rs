import argparse
import json
import os
import re
from typing import List, Callable, Optional, NamedTuple, Any, Dict # Keep existing typings
import jsonschema
from jinja2 import Environment, FileSystemLoader # Add Jinja2

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
        return "".join(word.capitalize() or "_" for word in name.split('_')) # Ensure an empty part (e.g. leading/trailing underscore) doesn't cause issues
    return name[0].upper() + name[1:]

# --- Global list to track generated class names for type mapping ---
_generated_classes_registry = set()

# --- Helper function for type mapping ---
def map_type_to_python(type_desc: Dict[str, Any], object_name_hint: Optional[str] = None) -> str:
    if not isinstance(type_desc, dict):
        # This case should ideally not be hit if input is validated by schema correctly
        print(f"Warning: Invalid type_desc passed to map_type_to_python: {type_desc}. Defaulting to 'Any'.")
        return "Any"

    submodel_type = type_desc.get('type')

    if submodel_type == "string":
        return "str"
    elif submodel_type == "integer":
        return "int"
    elif submodel_type == "number":
        return "float"
    elif submodel_type == "boolean":
        return "bool"
    elif submodel_type == "array":
        items_desc = type_desc.get('items')
        item_py_type = map_type_to_python(items_desc) if items_desc else "Any"
        return f"List[{item_py_type}]"
    elif submodel_type == "object":
        if object_name_hint:
            potential_class_name = camel_to_pascal(object_name_hint)
            inline_properties = {k: v for k, v in type_desc.items() if k not in ['type', 'description', 'items']}
            if inline_properties and potential_class_name in _generated_classes_registry:
                return potential_class_name
        return "Dict[str, Any]"
    elif not submodel_type: # Handles cases where "type": "object" might be implicit by presence of properties
        if len(type_desc.keys() - {'description', 'items'}) > 0 and object_name_hint: # check if other keys apart from desc/items exist
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
            # Check if it's a NamedTuple-like structure (has 'params') or class-like (has 'properties')
            params_or_props = None
            if 'params' in type_def: # For NamedTuples (event payloads, operation responses)
                params_or_props = type_def['params']
            elif 'properties' in type_def: # For regular nested classes
                params_or_props = type_def['properties']

            if params_or_props is not None: # Ensure it's a complex type we can destructure
                args = []
                for param in params_or_props:
                    # param is like {"name_snake": "host", "type_py": "str"}
                    # For nested classes, their properties might also have default_value defined directly
                    # For NamedTuple params, they won't have default_value in their definition here
                    param_default_val = param.get("default_value")
                    if param_default_val is None: # Not pre-defined (e.g. for NamedTuple params)
                         # Recursively call for each parameter/property type
                         # Pass along all_generated_type_definitions for further recursive calls
                        param_default_val = get_default_value_for_type(param['type_py'], all_generated_type_definitions)
                    args.append(f"{param['name_snake']}={param_default_val}")
                return f"{py_type_str}({', '.join(args)})"
        # Fallback for other registered classes (e.g., object classes like Configuration if not passed in all_generated_type_definitions,
        # or if the definition doesn't have 'params' or 'properties' which is unlikely for complex types here)
        return f"{py_type_str}()" # Assumes default constructor
    return "None"

# --- Recursive helper for nested object class definitions ---
def _generate_nested_object_class_definitions(
    name: str,
    details: Dict[str, Any],
    context_nested_classes: List[Dict[str, Any]],
    registry: set,
    depth: int = 0
):
    # print(f"{'  '*depth}DEBUG_GNOCD: Called for '{name}'")
    class_name_pascal = camel_to_pascal(name)

    sub_properties_desc = {k: v for k, v in details.items() if k not in ['type', 'description', 'items']}
    if not sub_properties_desc and details.get('type') == 'object':
        # print(f"{'  '*depth}DEBUG_GNOCD: '{name}' is an object type but has no inline sub-properties. Will create empty class {class_name_pascal}.")
        pass
    elif not sub_properties_desc:
        return

    if class_name_pascal in registry:
        # print(f"{'  '*depth}DEBUG_GNOCD: Class '{class_name_pascal}' already in registry. Skipping.")
        return

    # print(f"{'  '*depth}DEBUG_GNOCD: Registering and defining class '{class_name_pascal}'.")
    registry.add(class_name_pascal)
    class_def = {"name": class_name_pascal, "properties": []}

    if sub_properties_desc:
        for sub_name, sub_details in sub_properties_desc.items():
            # print(f"{'  '*(depth+1)}DEBUG_GNOCD: Processing sub-property '{sub_name}' of '{class_name_pascal}'.")
            if not isinstance(sub_details, dict):
                print(f"Warning: Expected dictionary for sub-property '{sub_name}' in '{class_name_pascal}', got {type(sub_details)}. Skipping.")
                continue

            if sub_details.get('type') == 'object':
                _generate_nested_object_class_definitions(sub_name, sub_details, context_nested_classes, registry, depth + 1)

            mapped_type = map_type_to_python(sub_details, object_name_hint=sub_name)
            class_def["properties"].append({
                "name_snake": camel_to_snake(sub_name),
                "type_py": mapped_type,
                "default_value": get_default_value_for_type(mapped_type)
            })
            # print(f"{'  '*(depth+1)}DEBUG_GNOCD: Added sub-property '{camel_to_snake(sub_name)}' to '{class_name_pascal}' with type '{mapped_type}'.")

    if not any(nc['name'] == class_name_pascal for nc in context_nested_classes):
        context_nested_classes.append(class_def)
        # print(f"{'  '*depth}DEBUG_GNOCD: Finished defining class '{class_name_pascal}'. Added to context.")
    # else:
        # print(f"{'  '*depth}DEBUG_GNOCD: Class '{class_name_pascal}' was already added to context (possibly by recursive call).")

def generate_code(submodel_data, role, output_dir):
    print("DEBUG: Entered generate_code (OpResp Debug Version)")
    _generated_classes_registry.clear()

    env = Environment(loader=FileSystemLoader("templates"), trim_blocks=False, lstrip_blocks=False)

    submodel_name = submodel_data['name']
    submodel_revision = submodel_data.get('revision', "N/A")
    submodel_name_pascal = camel_to_pascal(submodel_name)

    context = {
        "submodel_name": submodel_name,
        "submodel_revision": submodel_revision,
        "submodel_class_name": f"{submodel_name_pascal}{role.capitalize()}",
        "properties": [], "operations": [], "events": [],
        "nested_classes": [], "event_payloads": [], "operation_responses": []
    }

    # --- Processing Nested Classes from Properties ---
    for prop_name, prop_details in submodel_data.get('properties', {}).items():
        if prop_details.get('type') == 'object':
            try:
                _generate_nested_object_class_definitions(prop_name, prop_details, context["nested_classes"], _generated_classes_registry, depth=0)
            except Exception as e_gnocd_call: # Should be specific if possible
                print(f"ERROR calling _generate_nested_object_class_definitions for '{prop_name}': {e_gnocd_call}")

    # --- Populate Event Payloads ---
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

    # --- Populate Operation Responses (with DEBUG prints) ---
    print("DEBUG_OP_RESP: --- Start Populating operation_responses ---")
    for op_name, op_details in submodel_data.get('operations', {}).items():
        print(f"DEBUG_OP_RESP: Processing operation '{op_name}' for response.")
        response_desc = op_details.get('response')
        print(f"DEBUG_OP_RESP:   op_name='{op_name}', response_desc='{response_desc}'")
        if response_desc and response_desc.get('type') == 'object':
            response_sub_props = {k:v for k,v in response_desc.items() if k not in ['type', 'description', 'items']}
            print(f"DEBUG_OP_RESP:   response_sub_props='{response_sub_props}' for '{op_name}'")
            if response_sub_props:
                response_name_pascal = f"{camel_to_pascal(op_name)}Response"
                print(f"DEBUG_OP_RESP:   Complex response for '{op_name}'. Generating NamedTuple: '{response_name_pascal}'")
                _generated_classes_registry.add(response_name_pascal)
                print(f"DEBUG_OP_RESP:   Added '{response_name_pascal}' to registry. Registry now: {list(sorted(_generated_classes_registry))}")
                response_def = {"name": response_name_pascal, "params": [], "is_complex_object": True}
                for param_name, param_details in response_sub_props.items():
                    response_def["params"].append({
                        "name_snake": camel_to_snake(param_name),
                        "type_py": map_type_to_python(param_details, object_name_hint=param_name)
                    })
                context["operation_responses"].append(response_def)
                print(f"DEBUG_OP_RESP:   Appended to context['operation_responses']: {response_def}")
            else:
                print(f"DEBUG_OP_RESP:   Response for '{op_name}' is 'object' but no sub_props. Treating as simple dict or registered class.")
        elif response_desc: # Response exists but is not type object
             print(f"DEBUG_OP_RESP:   Response for '{op_name}' is not type 'object'. Type: {response_desc.get('type')}")
        else: # No response description
            print(f"DEBUG_OP_RESP:   No response description for operation '{op_name}'.")
    print("DEBUG_OP_RESP: --- Finished Populating operation_responses ---")

    # Create a flat dictionary of all generated type definitions for get_default_value_for_type
    all_generated_type_definitions = {}
    # Nested classes defined from properties (these have 'properties' key in their definition)
    for item_def in context["nested_classes"]:
        all_generated_type_definitions[item_def['name']] = {"properties": item_def['properties']}
    # Event payloads (NamedTuples, these have 'params' key in their definition)
    for item_def in context["event_payloads"]:
        all_generated_type_definitions[item_def['name']] = {"params": item_def['params']}
    # Operation responses (NamedTuples, these have 'params' key in their definition)
    for item_def in context["operation_responses"]:
        all_generated_type_definitions[item_def['name']] = {"params": item_def['params']}

    # --- Populate Main Class Properties ---
    for prop_name, prop_details in submodel_data.get('properties', {}).items():
        prop_name_snake = camel_to_snake(prop_name)
        if not prop_name_snake: continue
        py_type = map_type_to_python(prop_details, object_name_hint=prop_name)
        context["properties"].append({
            "name_snake": prop_name_snake,
            "type_py": py_type,
            "default_value": get_default_value_for_type(py_type, all_generated_type_definitions)
        })

    # --- Populate Operations ---
    for op_name, op_details in submodel_data.get('operations', {}).items():
        op_name_snake = camel_to_snake(op_name)
        if not op_name_snake: continue
        operation_ctx = {
            "name": op_name, "name_snake": op_name_snake, "params": [],
            "return_type": "None", "return_default_value": "None"
        }
        for param_name, param_details in op_details.get('parameters', {}).items():
            operation_ctx["params"].append({
                "name_snake": camel_to_snake(param_name),
                "type_py": map_type_to_python(param_details, object_name_hint=param_name)
            })
        response_desc = op_details.get('response')
        if response_desc:
            return_object_hint = f"{camel_to_pascal(op_name)}Response"
            operation_ctx["return_type"] = map_type_to_python(response_desc, object_name_hint=return_object_hint)
            operation_ctx["return_default_value"] = get_default_value_for_type(
                operation_ctx["return_type"],
                all_generated_type_definitions
            )
        context["operations"].append(operation_ctx)

    # --- Populate Events ---
    for event_name, event_details in submodel_data.get('events', {}).items():
        event_name_snake = camel_to_snake(event_name)
        if not event_name_snake: continue
        payload_name_pascal = f"{camel_to_pascal(event_name)}Payload"
        actual_payload_type = "None"
        if event_details.get('parameters'):
            actual_payload_type = payload_name_pascal if payload_name_pascal in _generated_classes_registry else "Dict[str, Any]"
        event_ctx = {
            "name": event_name, "name_snake": event_name_snake,
            "payload_type": actual_payload_type, "trigger_params": []
        }
        if actual_payload_type == payload_name_pascal :
            for param_name, param_details in event_details.get('parameters', {}).items():
                event_ctx["trigger_params"].append({
                    "name_snake": camel_to_snake(param_name),
                    "type_py": map_type_to_python(param_details, object_name_hint=param_name)
                })
        context["events"].append(event_ctx)

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
        print(f"Generated code for {role} at {output_filepath} (OpResp Debug Version)")
    except IOError as e:
        print(f"ERROR: Could not write generated file. Details: {e}")

# --- Main function (as before) ---
def main():
    parser = argparse.ArgumentParser(description='Generate Python classes from a submodel definition.')
    parser.add_argument('submodel_file', type=str, help='Path to the submodel definition JSON file.')
    parser.add_argument('--output-dir', type=str, default='generated_submodels', help='Directory to save generated Python files.')
    parser.add_argument('--role', type=str, choices=['consumer', 'provider'], required=True, help='Role to generate code for (consumer or provider).')
    args = parser.parse_args()

    schema_filepath = "assets2036py/resources/submodel_schema.json"
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

    try:
        with open(args.submodel_file, 'r') as f_data:
            submodel_data = json.load(f_data)
    except FileNotFoundError:
        print(f"Error: Submodel file not found at {args.submodel_file}")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {args.submodel_file}")
        return
    except Exception as e:
        print(f"Error: An unexpected error occurred while loading submodel file: {e}")
        return

    try:
        validator = jsonschema.Draft7Validator(submodel_json_schema)
        errors = sorted(validator.iter_errors(submodel_data), key=lambda e: e.path)
        if errors:
            print(f"Error: Submodel file '{args.submodel_file}' is not valid according to the schema '{schema_filepath}'.")
            for error in errors:
                print(f"- Validation Error: {'/'.join(map(str, error.path))} - {error.message}")
            return
        else:
            print(f"Submodel file '{args.submodel_file}' successfully validated against the schema.")
    except jsonschema.exceptions.SchemaError as e:
        print(f"Error: The JSON schema '{schema_filepath}' itself is invalid. Details: {e}")
        return
    except Exception as e:
        print(f"Error: An unexpected error occurred during schema validation: {e}")
        return

    generate_code(submodel_data, args.role, args.output_dir)

if __name__ == '__main__':
    main()
