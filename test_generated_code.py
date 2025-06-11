import unittest
import os
import sys
import shutil
import subprocess
import importlib.util
import asyncio
from unittest.mock import patch, MagicMock
import requests # For requests.exceptions
import json
from io import StringIO
import submodel_generator # To call submodel_generator.main()

TEST_OUTPUT_DIR = "test_generated_output"
PLACEHOLDER_SUBMODEL_FILE = "placeholder_submodel.json"
GENERATOR_SCRIPT = "submodel_generator.py" # Used by setUpClass, but not URL tests directly

class TestGeneratedCode(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if os.path.exists(TEST_OUTPUT_DIR):
            shutil.rmtree(TEST_OUTPUT_DIR)
        os.makedirs(TEST_OUTPUT_DIR)
        if os.path.abspath(TEST_OUTPUT_DIR) not in sys.path:
            sys.path.insert(0, os.path.abspath(TEST_OUTPUT_DIR))

        # Generate code using subprocess for existing tests
        try:
            subprocess.run([
                sys.executable, GENERATOR_SCRIPT, PLACEHOLDER_SUBMODEL_FILE,
                "--output-dir", TEST_OUTPUT_DIR, "--role", "consumer"
            ], check=True, capture_output=True, text=True)
            subprocess.run([
                sys.executable, GENERATOR_SCRIPT, PLACEHOLDER_SUBMODEL_FILE,
                "--output-dir", TEST_OUTPUT_DIR, "--role", "provider"
            ], check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            print(f"Failed to generate code for testing in setUpClass. Stdout: {e.stdout}, Stderr: {e.stderr}")
            raise

        try:
            consumer_module_name = "placeholder_submodel_consumer"
            provider_module_name = "placeholder_submodel_provider"

            consumer_spec = importlib.util.spec_from_file_location(consumer_module_name,
                                                                    os.path.join(TEST_OUTPUT_DIR, consumer_module_name + ".py"))
            cls.consumer_module = importlib.util.module_from_spec(consumer_spec)
            consumer_spec.loader.exec_module(cls.consumer_module)

            provider_spec = importlib.util.spec_from_file_location(provider_module_name,
                                                                     os.path.join(TEST_OUTPUT_DIR, provider_module_name + ".py"))
            cls.provider_module = importlib.util.module_from_spec(provider_spec)
            provider_spec.loader.exec_module(cls.provider_module)

            cls.PlaceholderSubmodelConsumer = getattr(cls.consumer_module, "PlaceholderSubmodelConsumer")
            cls.PlaceholderSubmodelProvider = getattr(cls.provider_module, "PlaceholderSubmodelProvider")

            cls.ConsumerConfiguration = getattr(cls.consumer_module, "Configuration", None)
            cls.ProviderConfiguration = getattr(cls.provider_module, "Configuration", None)
            cls.ConsumerComplexNested = getattr(cls.consumer_module, "ComplexNested", None)
            cls.ProviderComplexNested = getattr(cls.provider_module, "ComplexNested", None)
            cls.ConsumerLevelOne = getattr(cls.consumer_module, "LevelOne", None)
            cls.ProviderLevelOne = getattr(cls.provider_module, "LevelOne", None)
            cls.ConsumerErrorOccurredPayload = getattr(cls.consumer_module, "ErrorOccurredPayload", None)
            cls.ProviderErrorOccurredPayload = getattr(cls.provider_module, "ErrorOccurredPayload", None)
            cls.ConsumerGetConfigResponse = getattr(cls.consumer_module, "GetConfigResponse", None)
            cls.ProviderGetConfigResponse = getattr(cls.provider_module, "GetConfigResponse", None)

            # print(f"DEBUG_TEST_SETUP: ConsumerGetConfigResponse loaded: {cls.ConsumerGetConfigResponse is not None}")
            # print(f"DEBUG_TEST_SETUP: ConsumerLevelOne loaded: {cls.ConsumerLevelOne is not None}")

        except Exception as e:
            print(f"Failed to import generated modules in setUpClass: {e}")
            raise

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(TEST_OUTPUT_DIR):
            shutil.rmtree(TEST_OUTPUT_DIR)
        if os.path.abspath(TEST_OUTPUT_DIR) in sys.path:
            sys.path.remove(os.path.abspath(TEST_OUTPUT_DIR))

    def test_consumer_instantiation(self):
        self.assertIsNotNone(self.PlaceholderSubmodelConsumer, "Consumer class not loaded")
        consumer = self.PlaceholderSubmodelConsumer()
        self.assertIsNotNone(consumer)
        self.assertEqual(consumer.SUBMODEL_NAME, "PlaceholderSubmodel")
        self.assertEqual(consumer.SUBMODEL_REVISION, "1.0.0")
        self.assertFalse(hasattr(consumer, "SUBMODEL_ID"), "SUBMODEL_ID should not exist")

    def test_provider_instantiation(self):
        self.assertIsNotNone(self.PlaceholderSubmodelProvider, "Provider class not loaded")
        provider = self.PlaceholderSubmodelProvider()
        self.assertIsNotNone(provider)
        self.assertEqual(provider.SUBMODEL_NAME, "PlaceholderSubmodel")
        self.assertEqual(provider.SUBMODEL_REVISION, "1.0.0")
        self.assertFalse(hasattr(provider, "SUBMODEL_ID"), "SUBMODEL_ID should not exist")

    def test_consumer_properties(self):
        self.assertIsNotNone(self.PlaceholderSubmodelConsumer, "Consumer class not loaded")
        consumer = self.PlaceholderSubmodelConsumer()
        self.assertTrue(hasattr(consumer, "status") and isinstance(consumer.status, str))
        self.assertTrue(hasattr(consumer, "count") and isinstance(consumer.count, int))
        # ... (rest of the property assertions as they were) ...
        self.assertTrue(hasattr(consumer, "is_enabled") and isinstance(consumer.is_enabled, bool))
        self.assertTrue(hasattr(consumer, "measurements") and isinstance(consumer.measurements, list))

        self.assertTrue(hasattr(consumer, "configuration"))
        self.assertIsNotNone(self.ConsumerConfiguration, "ConsumerConfiguration class not loaded")
        self.assertIsInstance(consumer.configuration, self.ConsumerConfiguration)
        self.assertTrue(hasattr(consumer.configuration, "host") and isinstance(consumer.configuration.host, str))
        self.assertTrue(hasattr(consumer.configuration, "port") and isinstance(consumer.configuration.port, int))

        self.assertTrue(hasattr(consumer, "complex_nested"))
        self.assertIsNotNone(self.ConsumerComplexNested, "ConsumerComplexNested class not loaded")
        self.assertIsInstance(consumer.complex_nested, self.ConsumerComplexNested)
        self.assertTrue(hasattr(consumer.complex_nested, "active") and isinstance(consumer.complex_nested.active, bool))

        self.assertTrue(hasattr(consumer.complex_nested, "level_one"))
        self.assertIsNotNone(self.ConsumerLevelOne, "ConsumerLevelOne class not loaded")
        self.assertIsInstance(consumer.complex_nested.level_one, self.ConsumerLevelOne)
        self.assertTrue(hasattr(consumer.complex_nested.level_one, "name") and isinstance(consumer.complex_nested.level_one.name, str))
        self.assertTrue(hasattr(consumer.complex_nested.level_one, "value") and isinstance(consumer.complex_nested.level_one.value, float))


    def test_provider_properties(self):
        self.assertIsNotNone(self.PlaceholderSubmodelProvider, "Provider class not loaded")
        provider = self.PlaceholderSubmodelProvider()
        self.assertTrue(hasattr(provider, "status"))
        self.assertTrue(hasattr(provider, "configuration"))
        self.assertIsNotNone(self.ProviderConfiguration, "ProviderConfiguration class not loaded")
        self.assertIsInstance(provider.configuration, self.ProviderConfiguration)
        self.assertTrue(hasattr(provider, "complex_nested"))
        self.assertIsNotNone(self.ProviderComplexNested, "ProviderComplexNested class not loaded")
        self.assertIsInstance(provider.complex_nested, self.ProviderComplexNested)
        self.assertIsNotNone(self.ProviderLevelOne, "ProviderLevelOne class not loaded")
        self.assertIsInstance(provider.complex_nested.level_one, self.ProviderLevelOne)

    def test_consumer_methods(self):
        self.assertIsNotNone(self.PlaceholderSubmodelConsumer, "Consumer class not loaded")
        consumer = self.PlaceholderSubmodelConsumer()
        self.assertTrue(callable(getattr(consumer, "call_start", None)))
        self.assertTrue(callable(getattr(consumer, "call_stop", None)))
        self.assertTrue(callable(getattr(consumer, "call_get_config", None)))

        import asyncio # Moved import here as it's only used here now
        async def get_conf():
            return await consumer.call_get_config()
        result = asyncio.run(get_conf())
        self.assertIsNotNone(self.ConsumerGetConfigResponse, "ConsumerGetConfigResponse class not loaded")
        self.assertIsInstance(result, self.ConsumerGetConfigResponse)
        self.assertEqual(result.host, "")
        self.assertEqual(result.port, 0)

    def test_provider_methods(self):
        self.assertIsNotNone(self.PlaceholderSubmodelProvider, "Provider class not loaded")
        provider = self.PlaceholderSubmodelProvider()
        self.assertTrue(callable(getattr(provider, "start", None)))
        self.assertTrue(callable(getattr(provider, "stop", None)))
        self.assertTrue(callable(getattr(provider, "get_config", None)))
        result = provider.get_config()
        self.assertIsNotNone(self.ProviderGetConfigResponse, "ProviderGetConfigResponse class not loaded")
        self.assertIsInstance(result, self.ProviderGetConfigResponse)
        self.assertEqual(result.host, "")
        self.assertEqual(result.port, 0)

    def test_consumer_event_handlers(self):
        self.assertIsNotNone(self.PlaceholderSubmodelConsumer, "Consumer class not loaded")
        consumer = self.PlaceholderSubmodelConsumer()
        self.assertTrue(callable(getattr(consumer, "on_error_occurred", None)))
        self.assertIsNotNone(self.ConsumerErrorOccurredPayload, "ConsumerErrorOccurredPayload not loaded")
        def cb(payload: self.ConsumerErrorOccurredPayload): pass
        consumer.on_error_occurred(cb)

    def test_provider_event_triggers(self):
        self.assertIsNotNone(self.PlaceholderSubmodelProvider, "Provider class not loaded")
        provider = self.PlaceholderSubmodelProvider()
        self.assertTrue(callable(getattr(provider, "trigger_error_occurred", None)))
        provider.trigger_error_occurred(error_code=0, error_message="")

    def test_nested_classes_exist_and_work(self):
        self.assertIsNotNone(self.ConsumerConfiguration)
        self.assertIsNotNone(self.ConsumerComplexNested)
        self.assertIsNotNone(self.ConsumerLevelOne)
        self.assertIsNotNone(self.ConsumerErrorOccurredPayload)
        self.assertIsNotNone(self.ConsumerGetConfigResponse)

        conf = self.ConsumerConfiguration()
        conf.host = "test"
        self.assertEqual(conf.host, "test")

        err_payload = self.ConsumerErrorOccurredPayload(error_code=1, error_message="msg")
        self.assertEqual(err_payload.error_code, 1)

    # --- New Test Methods for URL Functionality ---

    def test_url_input_success(self):
        """Test successful code generation from a URL input."""
        # print("\\nDEBUG_TEST_URL: Running test_url_input_success")
        mock_response = MagicMock()
        mock_response.status_code = 200
        with open(PLACEHOLDER_SUBMODEL_FILE, 'r') as f:
            placeholder_content = f.read()
        mock_response.text = placeholder_content
        mock_response.json = MagicMock(side_effect=requests.exceptions.JSONDecodeError("Simulated error", "doc", 0))

        test_url = "http://fake-url.com/submodel.json"
        output_dir_url_test = os.path.join(TEST_OUTPUT_DIR, "url_test_success_output")
        if os.path.exists(output_dir_url_test):
            shutil.rmtree(output_dir_url_test)

        with patch('submodel_generator.requests.get', return_value=mock_response) as mock_get:
            original_argv = sys.argv
            sys.argv = [GENERATOR_SCRIPT, test_url, "--output-dir", output_dir_url_test, "--role", "consumer"]
            captured_output = StringIO()
            sys.stdout = captured_output
            try:
                submodel_generator.main()
            except Exception as e:
                sys.stdout = sys.__stdout__ # Restore stdout before failing
                self.fail(f"Submodel generator script failed for URL input: {e}. Output: {captured_output.getvalue()}")
            finally:
                sys.argv = original_argv
                sys.stdout = sys.__stdout__

            mock_get.assert_called_once_with(test_url, timeout=10)
            expected_file = os.path.join(output_dir_url_test, "placeholder_submodel_consumer.py")
            self.assertTrue(os.path.exists(expected_file), f"Expected output file {expected_file} not found for URL input. Output: {captured_output.getvalue()}")
        if os.path.exists(output_dir_url_test):
            shutil.rmtree(output_dir_url_test)

    def test_url_input_http_error(self):
        """Test URL input with a simulated HTTP error."""
        # print("\\nDEBUG_TEST_URL: Running test_url_input_http_error")
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"
        mock_response.raise_for_status = MagicMock(side_effect=requests.exceptions.HTTPError(response=mock_response))

        test_url = "http://fake-url.com/notfound.json"
        output_dir_url_test = os.path.join(TEST_OUTPUT_DIR, "url_test_http_error_output")
        if os.path.exists(output_dir_url_test):
            shutil.rmtree(output_dir_url_test)

        with patch('submodel_generator.requests.get', return_value=mock_response) as mock_get:
            original_argv = sys.argv
            sys.argv = [GENERATOR_SCRIPT, test_url, "--output-dir", output_dir_url_test, "--role", "consumer"]
            captured_output = StringIO()
            sys.stdout = captured_output
            try:
                submodel_generator.main()
            finally: # Ensure cleanup even if main() exits or errors before assert
                sys.argv = original_argv
                sys.stdout = sys.__stdout__

            output = captured_output.getvalue()
            mock_get.assert_called_once_with(test_url, timeout=10)
            self.assertIn(f"Error: HTTP error fetching submodel from URL '{test_url}'", output)
            expected_file = os.path.join(output_dir_url_test, "placeholder_submodel_consumer.py")
            self.assertFalse(os.path.exists(expected_file), f"Output file {expected_file} should not be created on HTTP error.")

        if os.path.exists(output_dir_url_test):
            shutil.rmtree(output_dir_url_test)

    def test_url_input_connection_error(self):
        """Test URL input with a simulated connection error."""
        # print("\\nDEBUG_TEST_URL: Running test_url_input_connection_error")
        test_url = "http://fake-url.com/connect-error.json"
        output_dir_url_test = os.path.join(TEST_OUTPUT_DIR, "url_test_conn_error_output")
        if os.path.exists(output_dir_url_test):
            shutil.rmtree(output_dir_url_test)

        with patch('submodel_generator.requests.get', side_effect=requests.exceptions.ConnectionError("Simulated connection error")) as mock_get:
            original_argv = sys.argv
            sys.argv = [GENERATOR_SCRIPT, test_url, "--output-dir", output_dir_url_test, "--role", "consumer"]
            captured_output = StringIO()
            sys.stdout = captured_output
            try:
                submodel_generator.main()
            finally:
                sys.argv = original_argv
                sys.stdout = sys.__stdout__

            output = captured_output.getvalue()
            mock_get.assert_called_once_with(test_url, timeout=10)
            self.assertIn(f"Error: Connection error fetching submodel from URL '{test_url}'", output)

        if os.path.exists(output_dir_url_test):
            shutil.rmtree(output_dir_url_test)

    def test_url_input_invalid_json(self):
        """Test URL input that returns non-JSON content."""
        # print("\\nDEBUG_TEST_URL: Running test_url_input_invalid_json")
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "This is not valid JSON content."
        mock_response.json = MagicMock(side_effect=requests.exceptions.JSONDecodeError("Simulated error", "doc", 0))

        test_url = "http://fake-url.com/not-json.txt"
        output_dir_url_test = os.path.join(TEST_OUTPUT_DIR, "url_test_invalid_json_output")
        if os.path.exists(output_dir_url_test):
            shutil.rmtree(output_dir_url_test)

        with patch('submodel_generator.requests.get', return_value=mock_response) as mock_get:
            original_argv = sys.argv
            sys.argv = [GENERATOR_SCRIPT, test_url, "--output-dir", output_dir_url_test, "--role", "consumer"]
            captured_output = StringIO()
            sys.stdout = captured_output
            try:
                submodel_generator.main()
            finally:
                sys.argv = original_argv
                sys.stdout = sys.__stdout__

            output = captured_output.getvalue()
            mock_get.assert_called_once_with(test_url, timeout=10)
            self.assertIn(f"Error: Could not decode JSON from the input source '{test_url}'", output)

        if os.path.exists(output_dir_url_test):
            shutil.rmtree(output_dir_url_test)

if __name__ == '__main__':
    unittest.main()
