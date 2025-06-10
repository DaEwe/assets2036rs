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
        self.assertEqual(consumer.SUBMODEL_ID, "urn:example:submodel:placeholder")

    def test_provider_instantiation(self):
        self.assertIsNotNone(self.PlaceholderSubmodelProvider, "Provider class not loaded")
        provider = self.PlaceholderSubmodelProvider()
        self.assertIsNotNone(provider)
        self.assertEqual(provider.SUBMODEL_ID, "urn:example:submodel:placeholder")

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


    async def _test_consumer_call_start(self, consumer): # Helper for async
        # This is a placeholder, actual implementation would require a running provider
        # or more complex mocking of the consumer's call mechanism.
        # For now, we test if it returns the default value.
        result = await consumer.call_start(delay=5)
        self.assertEqual(result, "") # Default for string return type

    def test_consumer_methods(self):
        self.assertIsNotNone(self.PlaceholderSubmodelConsumer, "Consumer class not loaded")
        consumer = self.PlaceholderSubmodelConsumer()
        self.assertTrue(callable(getattr(consumer, "call_start", None)))
        self.assertTrue(callable(getattr(consumer, "call_stop", None)))

        # Example of testing an async method's placeholder behavior
        asyncio.run(self._test_consumer_call_start(consumer))
        # call_stop has no return, so just check existence and callability
        # For a real test, one might check for logs or side effects if any.


    def test_provider_methods(self):
        self.assertIsNotNone(self.PlaceholderSubmodelProvider, "Provider class not loaded")
        provider = self.PlaceholderSubmodelProvider()
        self.assertTrue(callable(getattr(provider, "start", None)))
        self.assertTrue(callable(getattr(provider, "stop", None)))

        # Test placeholder return values
        self.assertEqual(provider.start(delay=0), "") # Default for string
        # provider.stop() # No return value


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
        provider.trigger_error_occurred(error_code=100, error_message="Test Error")


    def test_nested_classes_exist_and_work(self):
        self.assertIsNotNone(self.ConsumerConfiguration, "Consumer Configuration class should be generated.")
        self.assertIsNotNone(self.ProviderConfiguration, "Provider Configuration class should be generated.")
        self.assertIsNotNone(self.ConsumerErrorOccurredPayload, "Consumer ErrorOccurredPayload (NamedTuple) should be generated.")
        self.assertIsNotNone(self.ProviderErrorOccurredPayload, "Provider ErrorOccurredPayload (NamedTuple) should be generated.")

        # Test Consumer Configuration
        consumer_config_instance = self.ConsumerConfiguration() # Uses defaults from __init__
        self.assertEqual(consumer_config_instance.host, "")
        self.assertEqual(consumer_config_instance.port, 0)
        consumer_config_instance_custom = self.ConsumerConfiguration(host="testhost", port=1234)
        self.assertEqual(consumer_config_instance_custom.host, "testhost")
        self.assertEqual(consumer_config_instance_custom.port, 1234)

        # Test Provider Configuration
        provider_config_instance = self.ProviderConfiguration(host="anotherhost", port=5678)
        self.assertEqual(provider_config_instance.host, "anotherhost")
        self.assertEqual(provider_config_instance.port, 5678)

        # Test Consumer ErrorOccurredPayload (NamedTuple)
        consumer_payload = self.ConsumerErrorOccurredPayload(error_code=1, error_message="consumer test")
        self.assertEqual(consumer_payload.error_code, 1)
        self.assertEqual(consumer_payload.error_message, "consumer test")

        # Test Provider ErrorOccurredPayload (NamedTuple)
        provider_payload = self.ProviderErrorOccurredPayload(error_code=2, error_message="provider test")
        self.assertEqual(provider_payload.error_code, 2)
        self.assertEqual(provider_payload.error_message, "provider test")


if __name__ == '__main__':
    # This allows running the tests directly from the script.
    # The environment might also pick it up via `python -m unittest discover`.
    unittest.main()
