class DetectionError(Exception):
    def __init__(self, value, message) -> None:
        self.value = value
        self.message = message
        super().__init__(message)


class PrevalenceError(Exception):
    def __init__(self, value, message) -> None:
        self.value = value
        self.message = message
        super().__init__(message)


class AccuracyError(Exception):
    def __init__(self, values, message) -> None:
        self.values = values
        self.message = message
        super().__init__(message)
