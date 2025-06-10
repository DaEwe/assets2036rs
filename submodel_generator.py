import argparse
import json
import os
import re
from typing import List, Callable, Optional, NamedTuple, Any, Dict

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
def map_type_to_python(submodel_type, items_type=None, object_name_hint=None):
    if submodel_type == "string":
        return "str"
    elif submodel_type == "integer":
        return "int"
    elif submodel_type == "number": # Often means float in JSON schema contexts
        return "float"
    elif submodel_type == "boolean":
        return "bool"
    elif submodel_type == "array":
        item_py_type = map_type_to_python(items_type) if items_type else "Any"
        return f"List[{item_py_type}]"
    elif submodel_type == "object":
        # Try to use object_name_hint if provided and a class for it was registered
        if object_name_hint:
            potential_class_name = camel_to_pascal(object_name_hint)
            if potential_class_name in _generated_classes_registry:
                return potential_class_name
        return "Dict[str, Any]" # Fallback
    elif submodel_type not in ["string", "integer", "number", "boolean", "array", "object"]:
        print(f"Warning: Unknown submodel type '{submodel_type}' encountered. Defaulting to 'Any'.")
    return "Any" # Default fallback

# --- Helper function for default values ---
def get_default_value_for_type(py_type_str):
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

    submodel_id = submodel_data.get('id')
    if not submodel_id:
        print("Error: Submodel definition must contain an 'id'.")
        return

    submodel_name_from_data = submodel_data.get('name', submodel_data.get('idShort'))
    if not submodel_name_from_data:
        derived_name = submodel_id.split(':')[-1] if ':' in submodel_id else submodel_id
        submodel_name_from_data = derived_name
        print(f"Warning: Submodel 'name' or 'idShort' not found. Using derived name '{derived_name}' from 'id'.")

    submodel_name_pascal = camel_to_pascal(submodel_name_from_data)
    if not submodel_name_pascal or submodel_name_pascal == "Unnamed" or submodel_name_pascal == "GeneratedSubmodel": # Check against camel_to_pascal's default for empty
        # Attempt to create a more generic name if derivation failed badly
        generic_name_base = submodel_id.replace(':','_').replace('/','_') # Sanitize id for use as class name base
        submodel_name_pascal = camel_to_pascal(generic_name_base)
        if not submodel_name_pascal or submodel_name_pascal == "Unnamed": # if still bad
             submodel_name_pascal = "DefaultGeneratedSubmodelName" # Absolute fallback
        print(f"Warning: Could not derive a good submodel name. Using '{submodel_name_pascal}'.")


    output_filename_base = camel_to_snake(submodel_name_pascal)
    if not output_filename_base: output_filename_base = "default_generated_submodel_name" # Ensure filename is valid
    output_filename = f"{output_filename_base}_{role}.py"
    output_filepath = os.path.join(output_dir, output_filename)

    os.makedirs(output_dir, exist_ok=True)

    code_lines = [
        "from typing import List, Callable, Optional, NamedTuple, Any, Dict",
        "import abc",
        "",
    ]

    # --- Generate nested object classes (from properties) ---
    for prop_spec in submodel_data.get('properties', []):
        if prop_spec.get('type') == 'object' and prop_spec.get('properties'):
            # Use the property name as the basis for the nested class name
            nested_class_name = camel_to_pascal(prop_spec['name'])
            if not nested_class_name: continue # Skip if name is invalid

            _generated_classes_registry.add(nested_class_name)
            code_lines.append(f"class {nested_class_name}:")
            if not prop_spec['properties']:
                code_lines.append("    pass # No sub-properties defined")
            else:
                constructor_params = []
                constructor_body = []
                for sub_prop in prop_spec['properties']:
                    sub_prop_name_snake = camel_to_snake(sub_prop['name'])
                    sub_prop_type_py = map_type_to_python(sub_prop['type'], sub_prop.get('items', {}).get('type'))
                    default_val = get_default_value_for_type(sub_prop_type_py)
                    constructor_params.append(f"{sub_prop_name_snake}: {sub_prop_type_py} = {default_val}")
                    constructor_body.append(f"        self.{sub_prop_name_snake} = {sub_prop_name_snake}")

                code_lines.append(f"    def __init__(self, {', '.join(constructor_params)}):")
                if constructor_body:
                    code_lines.extend(constructor_body)
                else:
                    code_lines.append("        pass")

            code_lines.append("")


    # --- Generate NamedTuple for event payloads ---
    for event in submodel_data.get('events', []):
        if event.get('payload') and event['payload'].get('type') == 'object' and event['payload'].get('properties'):
            event_name_pascal = camel_to_pascal(event['name'])
            payload_class_name = f"{event_name_pascal}Payload"
            _generated_classes_registry.add(payload_class_name)
            code_lines.append(f"class {payload_class_name}(NamedTuple):")
            for prop in event['payload']['properties']:
                prop_name_snake = camel_to_snake(prop['name'])
                prop_type_py = map_type_to_python(prop['type'], prop.get('items', {}).get('type'))
                code_lines.append(f"    {prop_name_snake}: {prop_type_py}")
            code_lines.append("")

    # --- Main Submodel Class ---
    class_name = f"{submodel_name_pascal}{role.capitalize()}"
    code_lines.append(f"class {class_name}:")
    code_lines.append(f"    SUBMODEL_ID = \"{submodel_id}\"")
    code_lines.append("")
    code_lines.append("    def __init__(self):")

    if not submodel_data.get('properties'):
        code_lines.append("        pass # No properties to initialize")
    else:
        for prop in submodel_data.get('properties', []):
            prop_name_snake = camel_to_snake(prop['name'])
            if not prop_name_snake: continue # Skip if name is invalid

            prop_type = prop['type']
            py_type = map_type_to_python(prop_type,
                                         prop.get('items', {}).get('type') if prop_type == 'array' else None,
                                         prop.get('name') if prop_type == 'object' else None) # Pass name hint for objects

            default_value = get_default_value_for_type(py_type)
            code_lines.append(f"        self.{prop_name_snake}: {py_type} = {default_value}")
    code_lines.append("")

    # Methods generation
    if role == 'consumer':
        for method in submodel_data.get('methods', []):
            method_name_snake = camel_to_snake(method['name'])
            if not method_name_snake: continue

            params = []
            for inp in method.get('inputs', []):
                param_name_snake = camel_to_snake(inp['name'])
                param_type_py = map_type_to_python(inp['type'], inp.get('items', {}).get('type'))
                params.append(f"{param_name_snake}: {param_type_py}")

            return_type = "None"
            if method.get('outputs'):
                # Assuming single output or first one for simplicity
                first_output = method['outputs'][0]
                return_type = map_type_to_python(first_output['type'], first_output.get('items', {}).get('type'))

            code_lines.append(f"    async def call_{method_name_snake}(self, {', '.join(params) if params else ''}) -> {return_type}:")
            code_lines.append(f"        # TODO: Implement actual call to provider for '{method['name']}'")
            code_lines.append(f"        print(f\"Consumer: Calling method '{method['name']}'\")")
            if return_type != "None":
                 code_lines.append(f"        return {get_default_value_for_type(return_type)} # Placeholder return")
            else:
                code_lines.append("        pass")
            code_lines.append("")

    elif role == 'provider':
        for method in submodel_data.get('methods', []):
            method_name_snake = camel_to_snake(method['name'])
            if not method_name_snake: continue

            params = []
            for inp in method.get('inputs', []):
                param_name_snake = camel_to_snake(inp['name'])
                param_type_py = map_type_to_python(inp['type'], inp.get('items', {}).get('type'))
                params.append(f"{param_name_snake}: {param_type_py}")

            return_type = "None"
            if method.get('outputs'):
                first_output = method['outputs'][0]
                return_type = map_type_to_python(first_output['type'], first_output.get('items', {}).get('type'))

            code_lines.append(f"    def {method_name_snake}(self, {', '.join(params) if params else ''}) -> {return_type}:")
            code_lines.append(f"        # TODO: Implement actual logic for method '{method['name']}'")
            code_lines.append(f"        print(f\"Provider: Method '{method['name']}' called\")")
            if return_type != "None":
                 code_lines.append(f"        return {get_default_value_for_type(return_type)} # Placeholder return")
            else:
                code_lines.append("        pass")
            code_lines.append("")

    # Event handling generation
    if role == 'consumer':
        for event in submodel_data.get('events', []):
            event_name_snake = camel_to_snake(event['name'])
            if not event_name_snake: continue

            event_name_pascal = camel_to_pascal(event['name'])
            payload_class_name = f"{event_name_pascal}Payload"

            callback_param_type = payload_class_name if payload_class_name in _generated_classes_registry else "Dict[str, Any]"

            code_lines.append(f"    def on_{event_name_snake}(self, callback: Callable[[{callback_param_type}], None]) -> None:")
            code_lines.append(f"        # TODO: Implement event callback registration for '{event['name']}'")
            code_lines.append(f"        print(f\"Consumer: Registering callback for event '{event['name']}'\")")
            code_lines.append("        pass")
            code_lines.append("")

    elif role == 'provider':
        for event in submodel_data.get('events', []):
            event_name_snake = camel_to_snake(event['name'])
            if not event_name_snake: continue

            event_name_pascal = camel_to_pascal(event['name'])
            payload_class_name = f"{event_name_pascal}Payload"

            params = []
            param_names_for_payload_constructor = []
            if event.get('payload') and event['payload'].get('type') == 'object' and event['payload'].get('properties'):
                 for prop in event['payload']['properties']:
                    param_name_snake = camel_to_snake(prop['name'])
                    param_type_py = map_type_to_python(prop['type'], prop.get('items', {}).get('type'))
                    params.append(f"{param_name_snake}: {param_type_py}")
                    param_names_for_payload_constructor.append(f"{param_name_snake}={param_name_snake}")

            code_lines.append(f"    def trigger_{event_name_snake}(self, {', '.join(params) if params else ''}) -> None:")
            code_lines.append(f"        # TODO: Implement actual event triggering for '{event['name']}'")
            if payload_class_name in _generated_classes_registry and params:
                payload_args_str = ", ".join(param_names_for_payload_constructor)
                code_lines.append(f"        payload = {payload_class_name}({payload_args_str})")
                code_lines.append(f"        print(f\"Provider: Triggering event '{event['name']}' with payload {{payload}}\")")
            else:
                code_lines.append(f"        print(f\"Provider: Triggering event '{event['name']}'\")")
            code_lines.append("        pass")
            code_lines.append("")

    try:
        with open(output_filepath, 'w') as f:
            f.write("\n".join(code_lines))
        print(f"Generated code for {role} at {output_filepath}")
        print(f"Main class: {class_name}")
    except IOError as e:
        print(f"Error: Could not write generated file to {output_filepath}. Details: {e}")


def main():
    parser = argparse.ArgumentParser(description='Generate Python classes from a submodel definition.')
    parser.add_argument('submodel_file', type=str, help='Path to the submodel definition JSON file.')
    parser.add_argument('--output-dir', type=str, default='generated_submodels', help='Directory to save generated Python files.')
    parser.add_argument('--role', type=str, choices=['consumer', 'provider'], required=True, help='Role to generate code for (consumer or provider).')

    args = parser.parse_args()

    try:
        with open(args.submodel_file, 'r') as f:
            submodel_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Submodel file not found at {args.submodel_file}")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {args.submodel_file}")
        return
    except Exception as e: # Generic catch for other load issues
        print(f"Error: An unexpected error occurred while loading submodel file: {e}")
        return

    # Check if submodel_data was successfully loaded and processed by initial checks in generate_code
    # generate_code now returns None if there's an early exit due to bad data.
    # However, the primary check for submodel_data being None (e.g. from file read issues) is already handled by `return` in main.
    # The call to generate_code itself will handle internal validation.
    # If generate_code had a return value indicating success/failure, we might check it here.
    # For now, if file read failed, main exits. If generate_code fails internally, it prints errors and exits.
    # A simple check like this could be redundant if generate_code always exits on its own errors.
    # However, if generate_code could return None AND we want to stop `main` explicitly:
    # result = generate_code(submodel_data, args.role, args.output_dir)
    # if result is None: # Assuming generate_code returns None on handled error
    #     print("Halting due to errors in submodel data processing reported by generate_code.")
    #     return
    # For this iteration, direct call is fine as generate_code handles its own errors/returns.
    # And critical file errors already cause main to return.

    generate_code(submodel_data, args.role, args.output_dir)
    # No explicit check like `if not submodel_data:` is needed here if generate_code handles its own print and return for bad data.
    # The `return` statements in the try-except block for file loading already prevent generate_code from being called with invalid submodel_data.

if __name__ == '__main__':
    main()

# Step 2: Ensure placeholder_submodel.json exists from the previous plan step.
# The subtask will run:
# python submodel_generator.py placeholder_submodel.json --output-dir generated --role consumer
# python submodel_generator.py placeholder_submodel.json --output-dir generated --role provider
