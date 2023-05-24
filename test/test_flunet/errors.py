class ZeroError(Exception):
    def __init__(self, value, message) -> None:
        self.value = value
        self.message = message
        super().__init__(message)


class AccuracyError(Exception):
    def __init__(self, values, message) -> None:
        self.values = values
        self.message = message
        super().__init__(message)


class DiscrepancyError(Exception):
    def __init__(self, values, message) -> None:
        self.values = values
        self.message = message
        super().__init__(message)


class TerritoryError(Exception):
    def __init__(self, values, message) -> None:
        self.values = values
        self.message = message
        super().__init__(message)


class StrainError(Exception):
    def __init__(self, values, message) -> None:
        self.values = values
        self.message = message
        super().__init__(message)


class DuplicationError(Exception):
    def __init__(self, values, message) -> None:
        self.values = values
        self.message = message
        super().__init__(message)
