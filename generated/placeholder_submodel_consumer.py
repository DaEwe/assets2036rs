from typing import List, Callable, Optional, NamedTuple, Any, Dict
import abc

class Configuration:
    def __init__(self, host: str = "", port: int = 0):
        self.host = host
        self.port = port

class ErrorOccurredPayload(NamedTuple):
    error_code: int
    error_message: str

class PlaceholderSubmodelConsumer:
    SUBMODEL_ID = "urn:example:submodel:placeholder"

    def __init__(self):
        self.status: str = ""
        self.count: int = 0
        self.is_enabled: bool = False
        self.measurements: List[float] = []
        self.configuration: Configuration = Configuration()

    async def call_start(self, delay: int) -> str:
        # TODO: Implement actual call to provider for 'start'
        print(f"Consumer: Calling method 'start'")
        return "" # Placeholder return

    async def call_stop(self, ) -> None:
        # TODO: Implement actual call to provider for 'stop'
        print(f"Consumer: Calling method 'stop'")
        pass

    def on_error_occurred(self, callback: Callable[[ErrorOccurredPayload], None]) -> None:
        # TODO: Implement event callback registration for 'errorOccurred'
        print(f"Consumer: Registering callback for event 'errorOccurred'")
        pass
