import unittest
import os
import sys
import shutil
import subprocess
import importlib.util
import asyncio # Added for async method testing if needed later

# Define the directory for generated test files and ensure it's in Python's path
TEST_OUTPUT_DIR = "test_generated_output"
PLACEHOLDER_SUBMODEL_FILE = "placeholder_submodel.json" # Assumed to be in root
GENERATOR_SCRIPT = "submodel_generator.py" # Assumed to be in root

# Ensure placeholder_submodel.json exists (it should from previous steps)
# For this subtask, we assume it's present. If not, the generator will fail, caught by tests.

class TestGeneratedCode(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Generate code once before all tests in this class."""
        if os.path.exists(TEST_OUTPUT_DIR):
            shutil.rmtree(TEST_OUTPUT_DIR)
        os.makedirs(TEST_OUTPUT_DIR)
        # Add to sys.path to allow direct import of generated modules
        sys.path.insert(0, os.path.abspath(TEST_OUTPUT_DIR))

        cls.consumer_module = None
        cls.provider_module = None
        cls.PlaceholderSubmodelConsumer = None
        cls.PlaceholderSubmodelProvider = None
        cls.ConsumerConfiguration = None
        cls.ConsumerErrorOccurredPayload = None
        cls.ProviderConfiguration = None
        cls.ProviderErrorOccurredPayload = None
        # For new nested classes and operation responses
        cls.ConsumerComplexNested = None
        cls.ConsumerLevelOne = None # Part of ComplexNested
        cls.ConsumerGetConfigResponse = None
        cls.ProviderComplexNested = None
        cls.ProviderLevelOne = None
        cls.ProviderGetConfigResponse = None

        try:
            # Generate consumer code
            consumer_gen_result = subprocess.run([
                sys.executable, GENERATOR_SCRIPT, PLACEHOLDER_SUBMODEL_FILE,
                "--output-dir", TEST_OUTPUT_DIR, "--role", "consumer"
            ], check=False, capture_output=True, text=True)
            if consumer_gen_result.returncode != 0:
                print("Failed to generate consumer code for testing.")
                print("Stdout:", consumer_gen_result.stdout)
                print("Stderr:", consumer_gen_result.stderr)
                raise subprocess.CalledProcessError(consumer_gen_result.returncode, consumer_gen_result.args,
                                                    output=consumer_gen_result.stdout, stderr=consumer_gen_result.stderr)


            # Generate provider code
            provider_gen_result = subprocess.run([
                sys.executable, GENERATOR_SCRIPT, PLACEHOLDER_SUBMODEL_FILE,
                "--output-dir", TEST_OUTPUT_DIR, "--role", "provider"
            ], check=False, capture_output=True, text=True)
            if provider_gen_result.returncode != 0:
                print("Failed to generate provider code for testing.")
                print("Stdout:", provider_gen_result.stdout)
                print("Stderr:", provider_gen_result.stderr)
                raise subprocess.CalledProcessError(provider_gen_result.returncode, provider_gen_result.args,
                                                    output=provider_gen_result.stdout, stderr=provider_gen_result.stderr)


        except subprocess.CalledProcessError as e:
            # This block will now catch the explicitly raised error if returncode is not 0
            print(f"Code generation failed with exit code {e.returncode}.")
            # No need to print stdout/stderr again as it's done above
            raise RuntimeError("Code generation failed, cannot proceed with tests.") from e
        except FileNotFoundError:
            print(f"Error: Ensure '{GENERATOR_SCRIPT}' and '{PLACEHOLDER_SUBMODEL_FILE}' are in the root directory.")
            raise RuntimeError(f"Missing script or submodel file, cannot proceed with tests.")


        # Dynamically import the generated modules
        try:
            consumer_module_name = "placeholder_submodel_consumer" # Filename is snake_case
            provider_module_name = "placeholder_submodel_provider" # Filename is snake_case

            consumer_module_path = os.path.join(TEST_OUTPUT_DIR, consumer_module_name + ".py")
            provider_module_path = os.path.join(TEST_OUTPUT_DIR, provider_module_name + ".py")

            if not os.path.exists(consumer_module_path):
                 raise FileNotFoundError(f"Generated consumer module not found at {consumer_module_path}")
            if not os.path.exists(provider_module_path):
                 raise FileNotFoundError(f"Generated provider module not found at {provider_module_path}")

            consumer_spec = importlib.util.spec_from_file_location(consumer_module_name, consumer_module_path)
            cls.consumer_module = importlib.util.module_from_spec(consumer_spec)
            consumer_spec.loader.exec_module(cls.consumer_module)

            provider_spec = importlib.util.spec_from_file_location(provider_module_name, provider_module_path)
            cls.provider_module = importlib.util.module_from_spec(provider_spec)
            provider_spec.loader.exec_module(cls.provider_module)

            # Corrected class names based on "name": "PlaceholderSubmodel"
            cls.PlaceholderSubmodelConsumer = getattr(cls.consumer_module, "PlaceholderSubmodelConsumer")
            cls.PlaceholderSubmodelProvider = getattr(cls.provider_module, "PlaceholderSubmodelProvider")

            # Nested classes (property 'configuration' -> class 'Configuration')
            cls.ConsumerConfiguration = getattr(cls.consumer_module, "Configuration", None)
            cls.ProviderConfiguration = getattr(cls.provider_module, "Configuration", None)

            # Event payload classes (event 'errorOccurred' -> class 'ErrorOccurredPayload')
            cls.ConsumerErrorOccurredPayload = getattr(cls.consumer_module, "ErrorOccurredPayload", None)
            cls.ProviderErrorOccurredPayload = getattr(cls.provider_module, "ErrorOccurredPayload", None)

            # New nested property classes
            cls.ConsumerComplexNested = getattr(cls.consumer_module, "ComplexNested", None)
            cls.ProviderComplexNested = getattr(cls.provider_module, "ComplexNested", None)
            cls.ConsumerLevelOne = getattr(cls.consumer_module, "LevelOne", None) # Generated for complexNested.levelOne
            cls.ProviderLevelOne = getattr(cls.provider_module, "LevelOne", None)

            # New operation response NamedTuples
            cls.ConsumerGetConfigResponse = getattr(cls.consumer_module, "GetConfigResponse", None)
            cls.ProviderGetConfigResponse = getattr(cls.provider_module, "GetConfigResponse", None)

        except Exception as e:
            print(f"Failed to import generated modules: {e}")
            # Print files in TEST_OUTPUT_DIR to help debug
            if os.path.exists(TEST_OUTPUT_DIR):
                print(f"Files in {TEST_OUTPUT_DIR}: {os.listdir(TEST_OUTPUT_DIR)}")
            raise RuntimeError(f"Module import failed: {e}")


    @classmethod
    def tearDownClass(cls):
        """Clean up generated files and path modifications after tests."""
        if os.path.abspath(TEST_OUTPUT_DIR) in sys.path:
            sys.path.remove(os.path.abspath(TEST_OUTPUT_DIR))
        if os.path.exists(TEST_OUTPUT_DIR):
            shutil.rmtree(TEST_OUTPUT_DIR)


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
        self.assertTrue(hasattr(consumer, "status"))
        self.assertIsInstance(consumer.status, str)
        self.assertEqual(consumer.status, "") # Check default value

        self.assertTrue(hasattr(consumer, "count"))
        self.assertIsInstance(consumer.count, int)
        self.assertEqual(consumer.count, 0)

        self.assertTrue(hasattr(consumer, "is_enabled"))
        self.assertIsInstance(consumer.is_enabled, bool)
        self.assertEqual(consumer.is_enabled, False)

        self.assertTrue(hasattr(consumer, "measurements"))
        self.assertIsInstance(consumer.measurements, list)
        self.assertEqual(consumer.measurements, [])

        self.assertTrue(hasattr(consumer, "configuration"))
        self.assertIsNotNone(self.ConsumerConfiguration, "Consumer Configuration class should be generated and loaded.")
        self.assertIsInstance(consumer.configuration, self.ConsumerConfiguration)
        config = consumer.configuration
        self.assertTrue(hasattr(config, "host"))
        self.assertIsInstance(config.host, str)
        self.assertEqual(config.host, "")
        self.assertTrue(hasattr(config, "port"))
        self.assertIsInstance(config.port, int)
        self.assertEqual(config.port, 0)

        # Test new complexNested property
        self.assertTrue(hasattr(consumer, "complex_nested"))
        self.assertIsNotNone(self.ConsumerComplexNested, "Consumer ComplexNested class not loaded")
        self.assertIsInstance(consumer.complex_nested, self.ConsumerComplexNested)

        cn_instance = consumer.complex_nested
        self.assertTrue(hasattr(cn_instance, "active"))
        self.assertIsInstance(cn_instance.active, bool)
        self.assertEqual(cn_instance.active, False) # Default

        self.assertTrue(hasattr(cn_instance, "level_one"))
        self.assertIsNotNone(self.ConsumerLevelOne, "Consumer LevelOne class not loaded")
        self.assertIsInstance(cn_instance.level_one, self.ConsumerLevelOne)

        lo_instance = cn_instance.level_one
        self.assertTrue(hasattr(lo_instance, "name"))
        self.assertIsInstance(lo_instance.name, str)
        self.assertEqual(lo_instance.name, "")
        self.assertTrue(hasattr(lo_instance, "value"))
        self.assertIsInstance(lo_instance.value, float) # 'number' maps to float
        self.assertEqual(lo_instance.value, 0.0)


    def test_provider_properties(self):
        self.assertIsNotNone(self.PlaceholderSubmodelProvider, "Provider class not loaded")
        provider = self.PlaceholderSubmodelProvider()
        self.assertTrue(hasattr(provider, "status"))
        self.assertIsInstance(provider.status, str)
        self.assertEqual(provider.status, "")

        self.assertTrue(hasattr(provider, "count"))
        self.assertIsInstance(provider.count, int)
        self.assertEqual(provider.count, 0)

        self.assertTrue(hasattr(provider, "is_enabled"))
        self.assertIsInstance(provider.is_enabled, bool)
        self.assertEqual(provider.is_enabled, False)

        self.assertTrue(hasattr(provider, "measurements"))
        self.assertIsInstance(provider.measurements, list)
        self.assertEqual(provider.measurements, [])

        self.assertTrue(hasattr(provider, "configuration"))
        self.assertIsNotNone(self.ProviderConfiguration, "Provider Configuration class should be generated and loaded.")
        self.assertIsInstance(provider.configuration, self.ProviderConfiguration)
        config = provider.configuration
        self.assertTrue(hasattr(config, "host"))
        self.assertIsInstance(config.host, str)
        self.assertEqual(config.host, "")
        self.assertTrue(hasattr(config, "port"))
        self.assertIsInstance(config.port, int)
        self.assertEqual(config.port, 0)

        # Test new complexNested property for provider
        self.assertTrue(hasattr(provider, "complex_nested"))
        self.assertIsNotNone(self.ProviderComplexNested, "Provider ComplexNested class not loaded")
        self.assertIsInstance(provider.complex_nested, self.ProviderComplexNested)
        # ... (similar detailed checks as for consumer if desired)


    async def _test_consumer_calls(self, consumer):
        # Test call_start
        start_result = await consumer.call_start(delay=5)
        self.assertEqual(start_result, "") # Default for string return type

        # Test call_get_config
        self.assertIsNotNone(self.ConsumerGetConfigResponse, "Consumer GetConfigResponse class not loaded")
        get_config_result = await consumer.call_get_config()
        self.assertIsInstance(get_config_result, self.ConsumerGetConfigResponse)
        self.assertEqual(get_config_result.host, "") # Default value
        self.assertEqual(get_config_result.port, 0)  # Default value

    def test_consumer_methods(self):
        self.assertIsNotNone(self.PlaceholderSubmodelConsumer, "Consumer class not loaded")
        consumer = self.PlaceholderSubmodelConsumer()
        self.assertTrue(callable(getattr(consumer, "call_start", None)))
        self.assertTrue(callable(getattr(consumer, "call_stop", None)))
        self.assertTrue(callable(getattr(consumer, "call_get_config", None))) # New method

        asyncio.run(self._test_consumer_calls(consumer))


    def test_provider_methods(self):
        self.assertIsNotNone(self.PlaceholderSubmodelProvider, "Provider class not loaded")
        provider = self.PlaceholderSubmodelProvider()
        self.assertTrue(callable(getattr(provider, "start", None)))
        self.assertTrue(callable(getattr(provider, "stop", None)))
        self.assertTrue(callable(getattr(provider, "get_config", None))) # New method

        # Test placeholder return values
        self.assertEqual(provider.start(delay=0), "") # Default for string
        # provider.stop() has no return

        self.assertIsNotNone(self.ProviderGetConfigResponse, "Provider GetConfigResponse class not loaded")
        get_config_result = provider.get_config()
        self.assertIsInstance(get_config_result, self.ProviderGetConfigResponse)
        self.assertEqual(get_config_result.host, "") # Default value
        self.assertEqual(get_config_result.port, 0)  # Default value


    def test_consumer_event_handlers(self):
        self.assertIsNotNone(self.PlaceholderSubmodelConsumer, "Consumer class not loaded")
        consumer = self.PlaceholderSubmodelConsumer()
        self.assertTrue(callable(getattr(consumer, "on_error_occurred", None)))

        self.assertIsNotNone(self.ConsumerErrorOccurredPayload, "ConsumerErrorOccurredPayload not loaded")
        def my_callback(payload: self.ConsumerErrorOccurredPayload):
            pass
        consumer.on_error_occurred(my_callback) # Test registration (placeholder)


    def test_provider_event_triggers(self):
        self.assertIsNotNone(self.PlaceholderSubmodelProvider, "Provider class not loaded")
        provider = self.PlaceholderSubmodelProvider()
        self.assertTrue(callable(getattr(provider, "trigger_error_occurred", None)))

        self.assertIsNotNone(self.ProviderErrorOccurredPayload, "ProviderErrorOccurredPayload not loaded")
        # Test calling the trigger method (placeholder)
        # The event has parameters, so they must be provided if the payload class is specific.
        if self.ProviderErrorOccurredPayload.__name__ != "Dict": # Check if it's not the fallback Dict
             provider.trigger_error_occurred(error_code=100, error_message="Test Error")
        else: # Fallback if it's Dict[str, Any] - this shouldn't happen with current setup
             provider.trigger_error_occurred() # Or pass a dict if the template expects it


    def test_nested_classes_exist_and_work(self):
        # Existing classes
        self.assertIsNotNone(self.ConsumerConfiguration, "Consumer Configuration class should be generated.")
        self.assertIsNotNone(self.ProviderConfiguration, "Provider Configuration class should be generated.")
        self.assertIsNotNone(self.ConsumerErrorOccurredPayload, "Consumer ErrorOccurredPayload (NamedTuple) should be generated.")
        self.assertIsNotNone(self.ProviderErrorOccurredPayload, "Provider ErrorOccurredPayload (NamedTuple) should be generated.")

        # New classes based on updated placeholder
        self.assertIsNotNone(self.ConsumerComplexNested, "Consumer ComplexNested class should be generated.")
        self.assertIsNotNone(self.ProviderComplexNested, "Provider ComplexNested class should be generated.")
        self.assertIsNotNone(self.ConsumerLevelOne, "Consumer LevelOne class (for ComplexNested.levelOne) should be generated.")
        self.assertIsNotNone(self.ProviderLevelOne, "Provider LevelOne class (for ComplexNested.levelOne) should be generated.")
        self.assertIsNotNone(self.ConsumerGetConfigResponse, "Consumer GetConfigResponse NamedTuple should be generated.")
        self.assertIsNotNone(self.ProviderGetConfigResponse, "Provider GetConfigResponse NamedTuple should be generated.")

        # Test Consumer Configuration
        consumer_config_instance = self.ConsumerConfiguration(host="ch", port=12)
        self.assertEqual(consumer_config_instance.host, "ch")
        self.assertEqual(consumer_config_instance.port, 12)

        # Test Consumer ComplexNested and LevelOne
        level_one = self.ConsumerLevelOne(name="L1", value=123.45)
        complex_n = self.ConsumerComplexNested(level_one=level_one, active=True)
        self.assertIsInstance(complex_n.level_one, self.ConsumerLevelOne)
        self.assertEqual(complex_n.level_one.name, "L1")
        self.assertEqual(complex_n.level_one.value, 123.45)
        self.assertEqual(complex_n.active, True)

        # Test Consumer ErrorOccurredPayload (NamedTuple)
        consumer_payload = self.ConsumerErrorOccurredPayload(error_code=1, error_message="consumer test")
        self.assertEqual(consumer_payload.error_code, 1)

        # Test Consumer GetConfigResponse (NamedTuple)
        get_config_resp = self.ConsumerGetConfigResponse(host="localhost", port=8080)
        self.assertEqual(get_config_resp.host, "localhost")
        self.assertEqual(get_config_resp.port, 8080)


if __name__ == '__main__':
    # This allows running the tests directly from the script.
    # The environment might also pick it up via `python -m unittest discover`.
    unittest.main()
