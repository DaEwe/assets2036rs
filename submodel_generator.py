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
    """Converts camelCase to PascalCase."""
    if not name: return "Unnamed"
    return name[0].upper() + name[1:]

# --- Global list to track generated class names for type mapping ---
# This is a simplification. A more robust solution might involve a dedicated registry.
_generated_classes_registry = set()

# --- Helper function for type mapping ---
# Expects type_desc to be a dictionary like: {"type": "string"} or {"type": "object", "host": {"type": "string"}, ...}
def map_type_to_python(type_desc: Dict[str, Any], object_name_hint: Optional[str] = None) -> str:
    if not isinstance(type_desc, dict):
        print(f"Warning: Invalid type_desc passed to map_type_to_python: {type_desc}. Defaulting to 'Any'.")
        return "Any"

    submodel_type = type_desc.get('type')

    if submodel_type == "string":
        return "str"
    elif submodel_type == "integer":
        return "int"
    elif submodel_type == "number": # Often means float in JSON schema contexts
        return "float"
    elif submodel_type == "boolean":
        return "bool"
    elif submodel_type == "array":
        items_desc = type_desc.get('items')
        item_py_type = map_type_to_python(items_desc) if items_desc else "Any"
        return f"List[{item_py_type}]"
    elif submodel_type == "object":
        # Check if this object is a reference to a generated class or an inline definition
        # For inline definitions, object_name_hint is crucial for class naming.
        if object_name_hint:
            potential_class_name = camel_to_pascal(object_name_hint)
            # Check if the object has its own properties defined inline, making it a specific class
            # These are keys in type_desc other than 'type', 'description', 'items'.
            inline_properties = {k: v for k, v in type_desc.items() if k not in ['type', 'description', 'items']}
            if inline_properties and potential_class_name in _generated_classes_registry:
                return potential_class_name
        return "Dict[str, Any]" # Fallback for generic objects or if class not found/defined
    elif not submodel_type:
        # This case might occur if type_desc is something like {"$ref": "..."} which is not yet supported
        # Or if it's an object type description that omits the "type": "object" pair,
        # which can be valid if properties are listed directly.
        # For now, if 'type' is missing, but other keys (potential properties) exist, assume object.
        # This is a heuristic and might need refinement based on actual schema usage.
        if len(type_desc.keys()) > 0 and object_name_hint: # It has keys, could be an implicit object
            potential_class_name = camel_to_pascal(object_name_hint)
            if potential_class_name in _generated_classes_registry:
                return potential_class_name
            # It could also be a direct reference to a simple type if schema allows, but our current schema does not.
            # For now, fallback to Dict if it seems like an object without a pre-registered class.
            # This part is tricky because 'type_desc' could be a property definition itself if 'type' is missing.
            # Example: "configuration": { "host": {"type": "string"}, "port": {"type": "integer"} }
            # In this case, 'configuration' is the object_name_hint.
            # The `generate_code` function must ensure Configuration is in `_generated_classes_registry` *before* this is called for the type.
            # This map_type_to_python is called when processing the *main* properties list, and also for sub-properties.
            # If an object_name_hint-based class is registered, it should be used.
            # Otherwise, it's a generic dictionary.
            # The pre-registration of nested classes in generate_code is key.
            print(f"Warning: Submodel type key 'type' is missing in type_desc for '{object_name_hint}': {type_desc}. Assuming generic Dict or registered class if hint matches.")
            return "Dict[str, Any]" # Fallback, safer than "Any" if it has structure

    elif submodel_type not in ["string", "integer", "number", "boolean", "array", "object"]:
        print(f"Warning: Unknown submodel_type '{submodel_type}' in type_desc for '{object_name_hint}': {type_desc}. Defaulting to 'Any'.")

    return "Any" # Default fallback for unhandled cases

# --- Helper function for default values ---
def get_default_value_for_type(py_type_str: str) -> str:
    if py_type_str == "str": return "\"\""
    if py_type_str == "int": return "0"
    if py_type_str == "float": return "0.0"
    if py_type_str == "bool": return "False"
    if py_type_str.startswith("List["): return "[]"
    if py_type_str.startswith("Dict["): return "{}"
    # For custom classes (like Configuration or Payloads), assume they have default constructors
    if py_type_str in _generated_classes_registry : return f"{py_type_str}()"
    return "None"

def generate_code(submodel_data, role, output_dir):
    """Generates Python code from submodel data."""
    _generated_classes_registry.clear() # Clear for each generation run

    if not submodel_data:
        print("Error: Submodel data is empty.")
        return

    # Schema validation now guarantees 'name' exists.
    submodel_name_pascal = camel_to_pascal(submodel_data['name'])
    # The 'id' is optional according to schema, but we might still want it for SUBMODEL_ID if present.
    # For now, the schema doesn't define 'id' or 'idShort' at the top level.
    # We'll use a fixed string or derive from filename if needed, or remove SUBMODEL_ID class var.
    # For this iteration, let's remove the SUBMODEL_ID class variable as it's not in the new schema.
    # submodel_id = submodel_data.get('id', f"urn:generated:{submodel_name_pascal}")

    # --- Initialize Jinja2 Environment ---
    env = Environment(loader=FileSystemLoader("templates"), trim_blocks=False, lstrip_blocks=False)

    # --- Prepare data for Jinja2 context ---
    submodel_name = submodel_data['name'] # Schema validation guarantees 'name'
    submodel_revision = submodel_data.get('revision', "N/A") # Revision is optional in schema

    context = {
        "submodel_name": submodel_name,
        "submodel_revision": submodel_revision,
        "submodel_class_name": f"{submodel_name_pascal}{role.capitalize()}",
        "properties": [],
        "operations": [],
        "events": [],
        "nested_classes": [],
        "event_payloads": [],
        "operation_responses": []
    }

    # 1. Process and Register Nested Classes from Properties
    for prop_name, prop_details in submodel_data.get('properties', {}).items():
        if not isinstance(prop_details, dict): continue
        if prop_details.get('type') == 'object':
            sub_properties_desc = {k: v for k, v in prop_details.items() if k not in ['type', 'description', 'items']}
            if sub_properties_desc:
                nested_class_name_pascal = camel_to_pascal(prop_name)
                if not nested_class_name_pascal or nested_class_name_pascal == "Unnamed": continue

                _generated_classes_registry.add(nested_class_name_pascal)
                class_def = {"name": nested_class_name_pascal, "properties": []}
                for sub_name, sub_details in sub_properties_desc.items():
                    sub_name_snake = camel_to_snake(sub_name)
                    sub_py_type = map_type_to_python(sub_details, object_name_hint=sub_name)
                    class_def["properties"].append({
                        "name_snake": sub_name_snake,
                        "type_py": sub_py_type,
                        "default_value": get_default_value_for_type(sub_py_type)
                    })
                context["nested_classes"].append(class_def)

    # 2. Process and Register Event Payloads (as NamedTuples)
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

    # 3. Process and Register complex Operation Responses (as NamedTuples)
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

    # --- Populate context for main class properties ---
    for prop_name, prop_details in submodel_data.get('properties', {}).items():
        prop_name_snake = camel_to_snake(prop_name)
        if not prop_name_snake: continue
        py_type = map_type_to_python(prop_details, object_name_hint=prop_name)
        context["properties"].append({
            "name_snake": prop_name_snake,
            "type_py": py_type,
            "default_value": get_default_value_for_type(py_type)
        })

    # --- Populate context for operations ---
    for op_name, op_details in submodel_data.get('operations', {}).items():
        op_name_snake = camel_to_snake(op_name)
        if not op_name_snake: continue
        operation_ctx = {
            "name": op_name,
            "name_snake": op_name_snake,
            "params": [],
            "return_type": "None",
            "return_default_value": "None"
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
            operation_ctx["return_default_value"] = get_default_value_for_type(operation_ctx["return_type"])
        context["operations"].append(operation_ctx)

    # --- Populate context for events ---
    for event_name, event_details in submodel_data.get('events', {}).items():
        event_name_snake = camel_to_snake(event_name)
        if not event_name_snake: continue

        payload_name_pascal = f"{camel_to_pascal(event_name)}Payload"
        actual_payload_type = "None" # Default if no params
        if event_details.get('parameters'):
            actual_payload_type = payload_name_pascal if payload_name_pascal in _generated_classes_registry else "Dict[str, Any]"

        event_ctx = {
            "name": event_name,
            "name_snake": event_name_snake,
            "payload_type": actual_payload_type,
            "trigger_params": []
        }
        if actual_payload_type == payload_name_pascal :
            for param_name, param_details in event_details.get('parameters', {}).items():
                event_ctx["trigger_params"].append({
                    "name_snake": camel_to_snake(param_name),
                    "type_py": map_type_to_python(param_details, object_name_hint=param_name)
                })
        context["events"].append(event_ctx)

    # --- Render the template ---
    template_name = f"{role}.py.j2"
    try:
        template = env.get_template(template_name)
        generated_code_str = template.render(context)
    except Exception as e:
        print(f"Error rendering Jinja2 template {template_name}: {e}")
        # Consider printing context for debugging: print(json.dumps(context, indent=2))
        return

    # --- Write to file ---
    output_filename_base = camel_to_snake(submodel_name_pascal)
    if not output_filename_base: output_filename_base = "default_generated_submodel_name"
    output_filename = f"{output_filename_base}_{role}.py"
    output_filepath = os.path.join(output_dir, output_filename)
    os.makedirs(output_dir, exist_ok=True)

    try:
        with open(output_filepath, 'w') as f:
            f.write(generated_code_str)
        print(f"Generated code for {role} at {output_filepath} using Jinja2 template.")
        print(f"Main class: {context['submodel_class_name']}")
    except IOError as e:
        print(f"Error: Could not write generated file to {output_filepath}. Details: {e}")


def main():
    parser = argparse.ArgumentParser(description='Generate Python classes from a submodel definition.')
    parser.add_argument('submodel_file', type=str, help='Path to the submodel definition JSON file.')
    parser.add_argument('--output-dir', type=str, default='generated_submodels', help='Directory to save generated Python files.')
    parser.add_argument('--role', type=str, choices=['consumer', 'provider'], required=True, help='Role to generate code for (consumer or provider).')

    args = parser.parse_args()

    # --- Load the JSON Schema ---
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

    # --- Load the Submodel Data File ---
    try:
        with open(args.submodel_file, 'r') as f_data:
            submodel_data = json.load(f_data)
    except FileNotFoundError:
        print(f"Error: Submodel file not found at {args.submodel_file}")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {args.submodel_file}")
        return
    except Exception as e: # Generic catch for other load issues
        print(f"Error: An unexpected error occurred while loading submodel file: {e}")
        return

    # --- Validate Submodel Data against Schema ---
    try:
        validator = jsonschema.Draft7Validator(submodel_json_schema) # Or appropriate version
        errors = sorted(validator.iter_errors(submodel_data), key=lambda e: e.path)
        if errors:
            print(f"Error: Submodel file '{args.submodel_file}' is not valid according to the schema '{schema_filepath}'.")
            for error in errors:
                print(f"- Validation Error: {'/'.join(map(str, error.path))} - {error.message}")
            # Optionally print more details from `error` if needed
            return # Exit if validation fails
        else:
            print(f"Submodel file '{args.submodel_file}' successfully validated against the schema.")

    except jsonschema.exceptions.SchemaError as e:
        print(f"Error: The JSON schema '{schema_filepath}' itself is invalid. Details: {e}")
        return
    except Exception as e: # Catch other potential jsonschema related errors
        print(f"Error: An unexpected error occurred during schema validation: {e}")
        return

    # If validation passes, proceed to generate_code
    generate_code(submodel_data, args.role, args.output_dir)

if __name__ == '__main__':
    main()

# Step 2: Ensure placeholder_submodel.json exists from the previous plan step.
# The subtask will run:
# python submodel_generator.py placeholder_submodel.json --output-dir generated --role consumer
# python submodel_generator.py placeholder_submodel.json --output-dir generated --role provider
