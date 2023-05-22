from datetime import datetime
from typing import Optional, Dict
import pydantic
from errors import ZeroError, AccuracyError, DiscrepancyError


class FluNet(pydantic.BaseModel):
    collected: int
    dataSource: str
    dataSourceRow: int
    duration: str
    negative: int
    positive: int
    processed: int
    start: datetime

    @pydantic.root_validator(pre=True)
    @classmethod
    def consistent_collection_processing(cls, values):
        if values["collected"] != values["processed"]:
            raise DiscrepancyError(
                values=values,
                message="Number of collected samples differs from processed samples.",
            )
        return values

    @pydantic.root_validator(pre=True)
    @classmethod
    def consistent_collection_processing(cls, values):
        if values["processed"] != values["positive"] + values["negative"]:
            raise DiscrepancyError(
                values=values,
                message="Number of negative and positive samples differ from processed samples.",
            )
        return values

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
