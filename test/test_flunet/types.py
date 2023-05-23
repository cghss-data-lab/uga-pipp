from datetime import datetime
import pydantic
from .errors import ZeroError, AccuracyError, DiscrepancyError, TerritoryError


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
    def consistent_collection(cls, values):
        """
        Check number of collected and processed samples are equal.
        """
        if values["collected"] != values["processed"]:
            raise DiscrepancyError(
                values=values,
                message="Number of collected samples differs from processed samples.",
            )
        return values

    @pydantic.root_validator(pre=True)
    @classmethod
    def consistent_processing(cls, values):
        """
        Check number of processed samples is equal to the sym of positive and negative samples.
        """
        if values["processed"] != values["positive"] + values["negative"]:
            raise DiscrepancyError(
                values=values,
                message="Number of negative and positive samples differ from processed samples.",
            )
        return values

    @pydantic.validator("collected")
    @classmethod
    def collected_nonpositive(cls, value):
        if value <= 0:
            raise ZeroError(
                value=value, message="Collected is less than or equal to zero."
            )
        return value

    @pydantic.validator("negative")
    @classmethod
    def negative_nonpositive(cls, value):
        if value <= 0:
            raise ZeroError(
                value=value, message="Negative is less than or equal to zero."
            )
        return value

    @pydantic.validator("positive")
    @classmethod
    def positive_nonpositive(cls, value):
        if value <= 0:
            raise ZeroError(
                value=value, message="Positive is less than or equal to zero."
            )
        return value

    @pydantic.validator("processed")
    @classmethod
    def processed_nonpositive(cls, value):
        if value <= 0:
            raise ZeroError(
                value=value, message="Processed is less than or equal to zero."
            )
        return value


class FluNetReport(pydantic.BaseModel):
    row_data: dict
    neo4j_point: FluNet
    adjacent_nodes: list[tuple[dict, str]]

    @pydantic.root_validator(pre=True)
    @classmethod
    def report_node_accuracy(cls, values):
        if values["neo4j_point"] != values["row_data"]:
            raise AccuracyError(
                values=values, message="FluNet node and data are not equal."
            )

    @pydantic.root_validator(pre=True)
    @classmethod
    def adjacent_node_accuracy(cls, values):
        for node, node_type in values["adjacent_nodes"]:
            if node_type == "REPORT":
                pass
            if node_type == "IN":
                node_territory = node["name"]
                data_territory = values["row_data"]["Territory"]
                territory = node_territory == data_territory
                if not territory:
                    raise TerritoryError(
                        values=values, message="Territory does not match data."
                    )

        return values
