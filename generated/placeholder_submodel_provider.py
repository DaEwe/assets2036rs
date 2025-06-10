from typing import List, Callable, Optional, NamedTuple, Any, Dict
import abc

class Configuration:
    def __init__(self, host: str = "", port: int = 0):
        self.host = host
        self.port = port

class ErrorOccurredPayload(NamedTuple):
    error_code: int
    error_message: str

class PlaceholderSubmodelProvider:
    SUBMODEL_ID = "urn:example:submodel:placeholder"

    def __init__(self):
        self.status: str = ""
        self.count: int = 0
        self.is_enabled: bool = False
        self.measurements: List[float] = []
        self.configuration: Configuration = Configuration()

    def start(self, delay: int) -> str:
        # TODO: Implement actual logic for method 'start'
        print(f"Provider: Method 'start' called")
        return "" # Placeholder return

    def stop(self, ) -> None:
        # TODO: Implement actual logic for method 'stop'
        print(f"Provider: Method 'stop' called")
        pass

    def trigger_error_occurred(self, error_code: int, error_message: str) -> None:
        # TODO: Implement actual event triggering for 'errorOccurred'
        payload = ErrorOccurredPayload(error_code=error_code, error_message=error_message)
        print(f"Provider: Triggering event 'errorOccurred' with payload {payload}")
        pass
