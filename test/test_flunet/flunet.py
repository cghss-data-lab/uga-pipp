from datetime import datetime
from typing import Optional, Dict
import pydantic
from driver.create_query import create_query_line_data


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


class FluNet(pydantic.BaseModel):
    collected: int
    dataSource: str
    dataSourceRow: int
    duration: str
    negative: int
    positive: int
    processed: int
    # start: datetime

    @pydantic.validator("collected")
    @classmethod
    def collected_null(cls, value):
        if value <= 0:
            raise ZeroError(
                value=value, message="Collected is less than or equal to zero."
            )
        return value

    @pydantic.validator("negative")
    @classmethod
    def negative_null(cls, value):
        if value <= 0:
            raise ZeroError(
                value=value, message="Negative is less than or equal to zero."
            )
        return value

    @pydantic.validator("positive")
    @classmethod
    def positive_null(cls, value):
        if value <= 0:
            raise ZeroError(
                value=value, message="Positive is less than or equal to zero."
            )
        return value

    @pydantic.validator("processed")
    @classmethod
    def processed_null(cls, value):
        if value <= 0:
            raise ZeroError(
                value=value, message="Processed is less than or equal to zero."
            )
        return value


class FluNetReport(pydantic.BaseModel):
    neo4j_point: FluNet
    row_point: dict

    @pydantic.root_validator(pre=True)
    @classmethod
    def flunet_node_accurate(cls, values):
        if values["neo4j_point"] != values["row_point"]:
            raise AccuracyError(
                values=values, message="FluNet node and data are not equal."
            )
