import unittest
import os
import sys
import importlib.util
import subprocess
from unittest.mock import patch, MagicMock, call, ANY
import asyncio

# Add the submodel_generator directory to the Python path
# to allow importing submodel_generator
script_dir = os.path.dirname(os.path.abspath(__file__))
# Assuming submodel_generator.py is in the parent directory of the tests directory
# If test_generated_code.py is in /app and submodel_generator.py is in /app, this is not needed.
# If test_generated_code.py is in /app/tests and submodel_generator.py is in /app, then:
# sys.path.insert(0, os.path.dirname(script_dir))
# For now, assume they are in the same directory or submodel_generator is in PYTHONPATH

# Import the generator script
try:
    import submodel_generator
except ModuleNotFoundError:
    # Fallback if the script_dir adjustment isn't enough (e.g. running from a different CWD)
    # This assumes submodel_generator.py is in the same directory as this test script
    # or directly in the /app directory.
    if os.path.exists("submodel_generator.py"):
        sys.path.insert(0, os.getcwd())
        import submodel_generator
    else: # Try /app if running from /app/tests
        sys.path.insert(0, "/app")
        import submodel_generator


from assets2036py.assets import Asset, Mode
from assets2036py.communication import CommunicationClient

# These will be the actual names of the generated files
consumer_module_name = "placeholder_submodel_consumer"
provider_module_name = "placeholder_submodel_provider"

# No more class-level patch decorators
class TestGeneratedCode(unittest.IsolatedAsyncioTestCase):
    PlaceholderSubmodelConsumer = None
    PlaceholderSubmodelProvider = None
    # Store module names for patching
    consumer_module_name_prop = consumer_module_name # Renamed to avoid clash with module-level var
    provider_module_name_prop = provider_module_name # Renamed to avoid clash with module-level var


    @classmethod
    def setUpClass(cls):
        cls.patchers = [] # Initialize patchers list at class level

        # Start patchers before any modules that might import these are loaded
        patcher_ro_prop = patch("assets2036py.assets.ReadOnlyProperty")
        cls.MockReadOnlyProperty = patcher_ro_prop.start()
        cls.patchers.append(patcher_ro_prop)

        patcher_w_prop = patch("assets2036py.assets.WritableProperty")
        cls.MockWritableProperty = patcher_w_prop.start()
        cls.patchers.append(patcher_w_prop)

        patcher_sub_event = patch("assets2036py.assets.SubscribableEvent")
        cls.MockSubscribableEvent = patcher_sub_event.start()
        cls.patchers.append(patcher_sub_event)

        patcher_trig_event = patch("assets2036py.assets.TriggerableEvent")
        cls.MockTriggerableEvent = patcher_trig_event.start()
        cls.patchers.append(patcher_trig_event)

        patcher_call_op = patch("assets2036py.assets.CallableOperation")
        cls.MockCallableOperation = patcher_call_op.start()
        cls.patchers.append(patcher_call_op)

        patcher_bind_op = patch("assets2036py.assets.BindableOperation")
        cls.MockBindableOperation = patcher_bind_op.start()
        cls.patchers.append(patcher_bind_op)

        # Define paths
        cls.submodel_json_path = "placeholder_submodel.json"
        cls.test_output_dir = "generated_test_submodels" # New directory for test outputs
        os.makedirs(cls.test_output_dir, exist_ok=True)

        # Define expected output paths based on generator's naming convention
        # Submodel name "PlaceholderSubmodel" -> snake_case "placeholder_submodel"
        base_filename = "placeholder_submodel"
        cls.consumer_output_filename = f"{base_filename}_consumer.py"
        cls.provider_output_filename = f"{base_filename}_provider.py"
        cls.consumer_output_path = os.path.join(cls.test_output_dir, cls.consumer_output_filename)
        cls.provider_output_path = os.path.join(cls.test_output_dir, cls.provider_output_filename)

        # Run the generator script for consumer role
        result_consumer = subprocess.run(
            [sys.executable, "submodel_generator.py", cls.submodel_json_path, "--output-dir", cls.test_output_dir, "--role", "consumer"],
            capture_output=True, text=True
        )
        print("Generator (Consumer) STDOUT:", result_consumer.stdout)
        print("Generator (Consumer) STDERR:", result_consumer.stderr)
        assert result_consumer.returncode == 0, f"Generator script failed for consumer: {result_consumer.stderr}"
        assert os.path.exists(cls.consumer_output_path), f"Consumer output file not generated at {cls.consumer_output_path}"

        # Run the generator script for provider role
        result_provider = subprocess.run(
            [sys.executable, "submodel_generator.py", cls.submodel_json_path, "--output-dir", cls.test_output_dir, "--role", "provider"],
            capture_output=True, text=True
        )
        print("Generator (Provider) STDOUT:", result_provider.stdout)
        print("Generator (Provider) STDERR:", result_provider.stderr)
        assert result_provider.returncode == 0, f"Generator script failed for provider: {result_provider.stderr}"
        assert os.path.exists(cls.provider_output_path), f"Provider output file not generated at {cls.provider_output_path}"

        # Dynamically import the generated modules
        # The module name for import should not have .py and should be unique if files are in different dirs.
        # Since they are in a subdirectory, we need to add that to sys.path temporarily if not using full path for name.
        # For importlib, the name parameter should be the module's future __name__.

        # Use the base filename as the module name for import, this is what the patch decorators will use.
        cls.consumer_module_name_prop = consumer_module_name # Used by patchers
        cls.provider_module_name_prop = provider_module_name # Used by patchers

        consumer_spec = importlib.util.spec_from_file_location(cls.consumer_module_name_prop, cls.consumer_output_path)
        consumer_module = importlib.util.module_from_spec(consumer_spec)
        sys.modules[cls.consumer_module_name_prop] = consumer_module # Add to sys.modules before exec
        consumer_spec.loader.exec_module(consumer_module)
        cls.PlaceholderSubmodelConsumer = getattr(consumer_module, "PlaceholderSubmodelConsumer")

        provider_spec = importlib.util.spec_from_file_location(cls.provider_module_name_prop, cls.provider_output_path)
        provider_module = importlib.util.module_from_spec(provider_spec)
        sys.modules[cls.provider_module_name_prop] = provider_module # Add to sys.modules before exec
        provider_spec.loader.exec_module(provider_module)
        cls.PlaceholderSubmodelProvider = getattr(provider_module, "PlaceholderSubmodelProvider")


    @classmethod
    def tearDownClass(cls):
        # Stop all patchers
        for p in cls.patchers:
            p.stop()
        # Or use patch.stopall() if preferred and appropriate
        # patch.stopall()

        # Clean up generated files
        if os.path.exists(cls.consumer_output_path):
            os.remove(cls.consumer_output_path)
        if os.path.exists(cls.provider_output_path):
            os.remove(cls.provider_output_path)

        pycache_dir = os.path.join(cls.test_output_dir, "__pycache__")
        if os.path.exists(pycache_dir):
            try:
                for f_name in os.listdir(pycache_dir):
                    os.remove(os.path.join(pycache_dir, f_name))
                os.rmdir(pycache_dir)
            except OSError as e:
                print(f"Warning: Could not fully clean up {pycache_dir}. Error: {e}")

        if os.path.exists(cls.test_output_dir) and not os.listdir(cls.test_output_dir):
            try:
                os.rmdir(cls.test_output_dir)
            except OSError as e:
                print(f"Warning: Could not remove {cls.test_output_dir}. Error: {e}")

        # Remove modules from sys.modules
        if cls.consumer_module_name_prop in sys.modules:
            del sys.modules[cls.consumer_module_name_prop]
        if cls.provider_module_name_prop in sys.modules:
            del sys.modules[cls.provider_module_name_prop]


    def setUp(self): # Signature back to just self
        # Mocks are now started in setUpClass and stored on cls
        # self.MockReadOnlyProperty etc. are available via self if needed for assertions on the mock object itself
        # e.g. self.MockReadOnlyProperty.call_args_list

        self.mock_comm_client = MagicMock(spec=CommunicationClient)

        self.mock_consumer_asset = MagicMock(spec=Asset)
        self.mock_consumer_asset.name = "TestAssetConsumer"
        self.mock_consumer_asset.namespace = "test_namespace"
        self.mock_consumer_asset.access_mode = Mode.CONSUMER
        self.mock_consumer_asset.communication_client = self.mock_comm_client

        self.mock_provider_asset = MagicMock(spec=Asset)
        self.mock_provider_asset.name = "TestAssetProvider"
        self.mock_provider_asset.namespace = "test_namespace"
        self.mock_provider_asset.access_mode = Mode.OWNER
        self.mock_provider_asset.communication_client = self.mock_comm_client

        self.consumer = self.PlaceholderSubmodelConsumer(self.mock_consumer_asset, "PlaceholderSubmodel")
        self.provider = self.PlaceholderSubmodelProvider(self.mock_provider_asset, "PlaceholderSubmodel")

    # --- Consumer Property Tests ---
    # Test method signatures are now back to just 'self'
    def test_consumer_property_status_get(self):
        mock_status_instance = self.consumer._prop_status
        self.assertIsInstance(mock_status_instance, MagicMock, "Consumer's _prop_status is not the expected mock")

        mock_status_instance.value = "active_mock_value"
        self.assertEqual(self.consumer.status, "active_mock_value")

        # Check that ReadOnlyProperty was called correctly when self.consumer was initialized
        # The mock to check is now self.MockReadOnlyProperty (from setUp)
        found_call = False
        for call_item in self.MockReadOnlyProperty.call_args_list: # Use the correct mock attribute
            if call_item.kwargs.get('name') == 'status' and call_item.kwargs.get('parent') == self.consumer:
                found_call = True
                break
        self.assertTrue(found_call, "ReadOnlyProperty for 'status' was not instantiated as expected.")

    def test_consumer_property_status_on_change(self):
        mock_status_instance = self.consumer._prop_status
        self.assertIsInstance(mock_status_instance, MagicMock)

        def my_cb(val): pass
        self.consumer.on_status_change(my_cb)
        mock_status_instance.on_change.assert_called_once_with(my_cb)

    # --- Provider Property Tests ---
    def test_provider_property_status_set(self):
        mock_status_instance = self.provider._prop_status # This is an instance of self.MockWritableProperty
        self.assertIsInstance(mock_status_instance, MagicMock)

        new_value_to_set = "new_status_for_test"
        self.provider.status = new_value_to_set  # This calls the generated setter

        # The generated setter does: self._prop_status.value = new_value_to_set
        # Assert that the 'value' attribute of our mock '_prop_status' was set.
        self.assertEqual(mock_status_instance.value, new_value_to_set)

        # The getter should also return this new value.
        # This also implicitly tests that mock_status_instance.value was indeed set correctly by the setter.
        self.assertEqual(self.provider.status, new_value_to_set)

    def test_provider_property_status_on_change(self):
        mock_status_instance = self.provider._prop_status
        self.assertIsInstance(mock_status_instance, MagicMock)

        def my_cb(val): pass
        self.provider.on_status_change(my_cb)
        mock_status_instance.on_change.assert_called_once_with(my_cb)

    # --- Provider Event Tests ---
    def test_provider_event_trigger_error_occurred(self):
        mock_event_instance = self.provider._event_error_occurred
        self.assertIsInstance(mock_event_instance, MagicMock)

        # Use snake_case for keyword arguments as per generated method signature
        self.provider.trigger_error_occurred(error_code=123, error_message="Test Error")
        mock_event_instance.trigger.assert_called_once_with(error_code=123, error_message="Test Error")

    # --- Consumer Event Tests ---
    def test_consumer_event_subscribe_error_occurred(self):
        mock_event_instance = self.consumer._event_error_occurred
        self.assertIsInstance(mock_event_instance, MagicMock)

        def my_event_cb(payload): pass
        self.consumer.on_error_occurred(my_event_cb)

        mock_event_instance.subscribe.assert_called_once()
        internal_callback = mock_event_instance.subscribe.call_args[0][0]
        self.assertTrue(callable(internal_callback))

    # --- Consumer Operation Tests (Async) ---
    async def test_consumer_operation_call_start_async(self):
        mock_op_instance = self.consumer._op_start
        self.assertIsInstance(mock_op_instance, MagicMock)

        mock_op_instance.invoke = MagicMock(return_value=asyncio.Future())
        mock_op_instance.invoke.return_value.set_result("operation_started_async")

        result = await self.consumer.call_start(delay=5)
        self.assertEqual(result, "operation_started_async")
        mock_op_instance.invoke.assert_called_once_with(delay=5)

    # --- Provider Operation Tests ---
    def test_provider_operation_bind_start(self):
        mock_op_instance = self.provider._op_start
        self.assertIsInstance(mock_op_instance, MagicMock)

        async def my_op_handler(delay): return f"started with {delay}"

        self.provider.bind_start(my_op_handler)

        mock_op_instance.bind.assert_called_once()
        internal_bound_handler = mock_op_instance.bind.call_args[0][0]
        self.assertTrue(callable(internal_bound_handler))

    # Test for complex nested object property - consumer get
    def test_consumer_property_complex_nested_get(self):
        mock_prop_instance = self.consumer._prop_complex_nested
        self.assertIsInstance(mock_prop_instance, MagicMock)

        ConsumerModule = sys.modules[self.consumer_module_name_prop]
        LevelOneType = getattr(ConsumerModule, "LevelOne", None)
        ComplexNestedType = getattr(ConsumerModule, "ComplexNested", None)

        self.assertIsNotNone(LevelOneType, "LevelOne type not found in consumer module")
        self.assertIsNotNone(ComplexNestedType, "ComplexNested type not found in consumer module")

        # Use snake_case for keyword argument as per generated __init__
        mock_level_one = LevelOneType(name="test_name", value=123)
        mock_complex_nested = ComplexNestedType(level_one=mock_level_one, active=True)

        mock_prop_instance.value = mock_complex_nested

        retrieved_complex = self.consumer.complex_nested # Changed to snake_case
        self.assertEqual(retrieved_complex.level_one.name, "test_name") # Also change here for consistency
        self.assertEqual(retrieved_complex.level_one.value, 123)    # And here
        self.assertTrue(retrieved_complex.active)

        found_call = False
        # Check against the class-level mock self.MockReadOnlyProperty
        # The 'name' kwarg in ReadOnlyProperty init is still original camelCase "complexNested"
        for call_item in self.MockReadOnlyProperty.call_args_list:
            if call_item.kwargs.get('name') == 'complexNested' and call_item.kwargs.get('parent') == self.consumer:
                found_call = True
                break
        self.assertTrue(found_call, "ReadOnlyProperty for 'complexNested' was not instantiated as expected.")

    # Test for operation with complex response type - consumer call
    async def test_consumer_operation_call_get_config_async(self):
        mock_op_instance = self.consumer._op_get_config
        self.assertIsInstance(mock_op_instance, MagicMock)

        ConsumerModule = sys.modules[self.consumer_module_name_prop]
        GetConfigResponseType = getattr(ConsumerModule, "GetConfigResponse", None)
        self.assertIsNotNone(GetConfigResponseType, "GetConfigResponse type not found in consumer module")

        expected_response_obj = GetConfigResponseType(host="localhost", port=8080)

        mock_op_instance.invoke = MagicMock(return_value=asyncio.Future())
        mock_op_instance.invoke.return_value.set_result(expected_response_obj)

        result = await self.consumer.call_get_config()
        self.assertIsInstance(result, GetConfigResponseType)
        self.assertEqual(result.host, "localhost")
        self.assertEqual(result.port, 8080)
        mock_op_instance.invoke.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
