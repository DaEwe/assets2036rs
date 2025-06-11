from typing import List, Callable, Optional, NamedTuple, Any, Dict
import abc
from assets2036py.assets import Asset, ReadOnlyProperty, WritableProperty, SubscribableEvent, TriggerableEvent, CallableOperation, BindableOperation # Added Operation classes
import json

# --- Generated Nested Classes & NamedTuples ---

class Configuration:

    def __init__(self, host: str = "", port: int = 0):

        self.host = host

        self.port = port




class LevelOne:

    def __init__(self, name: str = "", value: float = 0.0):

        self.name = name

        self.value = value




class ComplexNested:

    def __init__(self, level_one: LevelOne = LevelOne(), active: bool = False):

        self.level_one = level_one

        self.active = active





class ErrorOccurredPayload(NamedTuple):


    error_code: int

    error_message: str







class GetConfigResponse(NamedTuple):

    host: str

    port: int




# --- Main Submodel Class ---
class PlaceholderSubmodelConsumer:
    # BASE_PY_J2 __INIT__ VERSION_PROPERTIES_FIX
    # Removed SUBMODEL_NAME and SUBMODEL_REVISION class attributes from here

    def __init__(self, parent_asset: Asset, submodel_name: str):
        # print(f"DEBUG: Initializing PlaceholderSubmodelConsumer for parent_asset.name / submodel_name")
        self.parent_asset = parent_asset
        self.submodel_name = submodel_name
        self.communication_client = self.parent_asset.communication_client
        self.access_mode = self.parent_asset.access_mode


        # prop.schema_dict_str is a JSON string. json.loads converts it to a Python dict.
        # The |tojson filter ensures the string prop.schema_dict_str is correctly escaped
        # to be a valid Python string literal when injected into json.loads(...).
        prop_schema_dict_for_status = json.loads("{\"type\": \"string\", \"description\": \"The current status of the asset.\"}")
        self._prop_status = ReadOnlyProperty(
            name="status",  # Use original name from submodel definition
            parent=self,
            property_definition=prop_schema_dict_for_status
        )
        # print(f"DEBUG: Initialized property _status for status")

        # prop.schema_dict_str is a JSON string. json.loads converts it to a Python dict.
        # The |tojson filter ensures the string prop.schema_dict_str is correctly escaped
        # to be a valid Python string literal when injected into json.loads(...).
        prop_schema_dict_for_count = json.loads("{\"type\": \"integer\", \"description\": \"A counter value.\"}")
        self._prop_count = ReadOnlyProperty(
            name="count",  # Use original name from submodel definition
            parent=self,
            property_definition=prop_schema_dict_for_count
        )
        # print(f"DEBUG: Initialized property _count for count")

        # prop.schema_dict_str is a JSON string. json.loads converts it to a Python dict.
        # The |tojson filter ensures the string prop.schema_dict_str is correctly escaped
        # to be a valid Python string literal when injected into json.loads(...).
        prop_schema_dict_for_is_enabled = json.loads("{\"type\": \"boolean\", \"description\": \"Indicates if the asset is enabled.\"}")
        self._prop_is_enabled = ReadOnlyProperty(
            name="isEnabled",  # Use original name from submodel definition
            parent=self,
            property_definition=prop_schema_dict_for_is_enabled
        )
        # print(f"DEBUG: Initialized property _is_enabled for isEnabled")

        # prop.schema_dict_str is a JSON string. json.loads converts it to a Python dict.
        # The |tojson filter ensures the string prop.schema_dict_str is correctly escaped
        # to be a valid Python string literal when injected into json.loads(...).
        prop_schema_dict_for_measurements = json.loads("{\"type\": \"array\", \"description\": \"A list of measurements.\", \"items\": {\"type\": \"number\"}}")
        self._prop_measurements = ReadOnlyProperty(
            name="measurements",  # Use original name from submodel definition
            parent=self,
            property_definition=prop_schema_dict_for_measurements
        )
        # print(f"DEBUG: Initialized property _measurements for measurements")

        # prop.schema_dict_str is a JSON string. json.loads converts it to a Python dict.
        # The |tojson filter ensures the string prop.schema_dict_str is correctly escaped
        # to be a valid Python string literal when injected into json.loads(...).
        prop_schema_dict_for_configuration = json.loads("{\"type\": \"object\", \"description\": \"Asset configuration parameters.\", \"host\": {\"type\": \"string\", \"description\": \"Hostname of the asset.\"}, \"port\": {\"type\": \"integer\", \"description\": \"Port number for communication.\"}}")
        self._prop_configuration = ReadOnlyProperty(
            name="configuration",  # Use original name from submodel definition
            parent=self,
            property_definition=prop_schema_dict_for_configuration
        )
        # print(f"DEBUG: Initialized property _configuration for configuration")

        # prop.schema_dict_str is a JSON string. json.loads converts it to a Python dict.
        # The |tojson filter ensures the string prop.schema_dict_str is correctly escaped
        # to be a valid Python string literal when injected into json.loads(...).
        prop_schema_dict_for_complex_nested = json.loads("{\"type\": \"object\", \"description\": \"A more complex nested object.\", \"levelOne\": {\"type\": \"object\", \"description\": \"First level of nesting.\", \"name\": {\"type\": \"string\"}, \"value\": {\"type\": \"number\"}}, \"active\": {\"type\": \"boolean\"}}")
        self._prop_complex_nested = ReadOnlyProperty(
            name="complexNested",  # Use original name from submodel definition
            parent=self,
            property_definition=prop_schema_dict_for_complex_nested
        )
        # print(f"DEBUG: Initialized property _complex_nested for complexNested")



        event_def_dict_for_error_occurred = json.loads("{\"description\": \"Fired when an error occurs.\", \"parameters\": {\"errorCode\": {\"type\": \"integer\", \"description\": \"The code of the error.\"}, \"errorMessage\": {\"type\": \"string\", \"description\": \"A human-readable error message.\"}}}")
        self._event_error_occurred = SubscribableEvent(
            name="errorOccurred",  # Original event name
            parent=self,
            event_definition=event_def_dict_for_error_occurred
        )
        # print(f"DEBUG: Initialized event _error_occurred for errorOccurred")



        op_def_dict_for_start = json.loads("{\"description\": \"Starts the asset.\", \"parameters\": {\"delay\": {\"type\": \"integer\", \"description\": \"Delay in seconds before starting.\"}}, \"response\": {\"type\": \"string\", \"description\": \"The result of the start operation.\"}}")
        self._op_start = CallableOperation(
            name="start",
            parent=self,
            operation_definition=op_def_dict_for_start
        )
        # print(f"DEBUG: Initialized operation _op_start for start")

        op_def_dict_for_stop = json.loads("{\"description\": \"Stops the asset.\"}")
        self._op_stop = CallableOperation(
            name="stop",
            parent=self,
            operation_definition=op_def_dict_for_stop
        )
        # print(f"DEBUG: Initialized operation _op_stop for stop")

        op_def_dict_for_get_config = json.loads("{\"description\": \"Returns the current configuration object.\", \"response\": {\"type\": \"object\", \"host\": {\"type\": \"string\"}, \"port\": {\"type\": \"integer\"}}}")
        self._op_get_config = CallableOperation(
            name="get_config",
            parent=self,
            operation_definition=op_def_dict_for_get_config
        )
        # print(f"DEBUG: Initialized operation _op_get_config for get_config")


    def _get_topic(self) -> str:
        # The self.submodel_name is expected to be pre-sanitized by the generator.
        return f"{self.parent_asset.namespace}/{self.parent_asset.name}/{self.submodel_name}"




    @property
    def status(self) -> Optional[str]:
        if hasattr(self, '_prop_status'):
            return self._prop_status.value  # type: ignore
        # print(f"Warning: Accessing 'status' but underlying '_prop_status' not found.") # Optional debug
        return None

    def on_status_change(self, callback: Callable[[Optional[str]], None]) -> None:
        if hasattr(self, '_prop_status'):
            self._prop_status.on_change(callback)
        else:
            print(f"Warning: Property 'status' not fully initialized for on_change subscription.")

    @property
    def count(self) -> Optional[int]:
        if hasattr(self, '_prop_count'):
            return self._prop_count.value  # type: ignore
        # print(f"Warning: Accessing 'count' but underlying '_prop_count' not found.") # Optional debug
        return None

    def on_count_change(self, callback: Callable[[Optional[int]], None]) -> None:
        if hasattr(self, '_prop_count'):
            self._prop_count.on_change(callback)
        else:
            print(f"Warning: Property 'count' not fully initialized for on_change subscription.")

    @property
    def is_enabled(self) -> Optional[bool]:
        if hasattr(self, '_prop_is_enabled'):
            return self._prop_is_enabled.value  # type: ignore
        # print(f"Warning: Accessing 'is_enabled' but underlying '_prop_is_enabled' not found.") # Optional debug
        return None

    def on_is_enabled_change(self, callback: Callable[[Optional[bool]], None]) -> None:
        if hasattr(self, '_prop_is_enabled'):
            self._prop_is_enabled.on_change(callback)
        else:
            print(f"Warning: Property 'is_enabled' not fully initialized for on_change subscription.")

    @property
    def measurements(self) -> Optional[List[float]]:
        if hasattr(self, '_prop_measurements'):
            return self._prop_measurements.value  # type: ignore
        # print(f"Warning: Accessing 'measurements' but underlying '_prop_measurements' not found.") # Optional debug
        return None

    def on_measurements_change(self, callback: Callable[[Optional[List[float]]], None]) -> None:
        if hasattr(self, '_prop_measurements'):
            self._prop_measurements.on_change(callback)
        else:
            print(f"Warning: Property 'measurements' not fully initialized for on_change subscription.")

    @property
    def configuration(self) -> Optional[Configuration]:
        if hasattr(self, '_prop_configuration'):
            return self._prop_configuration.value  # type: ignore
        # print(f"Warning: Accessing 'configuration' but underlying '_prop_configuration' not found.") # Optional debug
        return None

    def on_configuration_change(self, callback: Callable[[Optional[Configuration]], None]) -> None:
        if hasattr(self, '_prop_configuration'):
            self._prop_configuration.on_change(callback)
        else:
            print(f"Warning: Property 'configuration' not fully initialized for on_change subscription.")

    @property
    def complex_nested(self) -> Optional[ComplexNested]:
        if hasattr(self, '_prop_complex_nested'):
            return self._prop_complex_nested.value  # type: ignore
        # print(f"Warning: Accessing 'complex_nested' but underlying '_prop_complex_nested' not found.") # Optional debug
        return None

    def on_complex_nested_change(self, callback: Callable[[Optional[ComplexNested]], None]) -> None:
        if hasattr(self, '_prop_complex_nested'):
            self._prop_complex_nested.on_change(callback)
        else:
            print(f"Warning: Property 'complex_nested' not fully initialized for on_change subscription.")




    async def call_start(self, delay: int) -> str:
        if hasattr(self, '_op_start'): # Corrected attribute name
            params_dict = {
                "delay": delay
            }
            # Not exposing timeout in the generated method signature for now. Default will be used by .invoke()
            return await self._op_start.invoke(**params_dict)
        else:
            # This case implies an issue with __init__ if the instance isn't there.
            print(f"Warning: Operation 'start' not initialized.") # Changed to warning for now

            return ""  # Return default on error


    async def call_stop(self) -> None:
        if hasattr(self, '_op_stop'): # Corrected attribute name
            params_dict = {
            }
            # Not exposing timeout in the generated method signature for now. Default will be used by .invoke()
            return await self._op_stop.invoke(**params_dict)
        else:
            # This case implies an issue with __init__ if the instance isn't there.
            print(f"Warning: Operation 'stop' not initialized.") # Changed to warning for now

            return None # Or pass


    async def call_get_config(self) -> GetConfigResponse:
        if hasattr(self, '_op_get_config'): # Corrected attribute name
            params_dict = {
            }
            # Not exposing timeout in the generated method signature for now. Default will be used by .invoke()
            return await self._op_get_config.invoke(**params_dict)
        else:
            # This case implies an issue with __init__ if the instance isn't there.
            print(f"Warning: Operation 'get_config' not initialized.") # Changed to warning for now

            return GetConfigResponse(host="", port=0)  # Return default on error




    def on_error_occurred(self, callback: Callable[[ErrorOccurredPayload, float], None]) -> None:
        # assets.py SubscribableEvent.on_event expects callback_func(parameters: Dict[str, Any], timestamp: float)
        # This wrapper adapts it to a user-friendly callback: (PayloadType, timestamp) or (Dict[str,Any], timestamp) or (timestamp)
        def internal_callback_wrapper(parameters: Dict[str, Any], timestamp: float):

            try:
                # Ensure all required keys for the NamedTuple are present in parameters, otherwise it will raise TypeError
                # This is a simple way to filter; more sophisticated logic might be needed if parameters can have extra fields
                # that are not part of the NamedTuple, or if some NamedTuple fields are Optional and might be missing.
                # For now, assume 'parameters' from the event exactly matches the NamedTuple fields.
                payload_instance = ErrorOccurredPayload(**parameters)
                callback(payload_instance, timestamp)
            except TypeError as e:
                print(f"Error creating payload for event 'errorOccurred': {e}. Raw params: {parameters}")
                # Fallback: pass raw dictionary and timestamp if payload creation fails
                # Or, users can make their callback accept Dict[str, Any] if they expect potential errors
                # callback(parameters, timestamp) # Option 1: Pass raw dict
                # Option 2: Do nothing or log, as type contract is broken


        if hasattr(self, '_event_error_occurred'):
            self._event_error_occurred.subscribe(internal_callback_wrapper) # Changed on_event to subscribe
        else:
            print(f"Warning: Event 'error_occurred' not fully initialized for on_event subscription.")
