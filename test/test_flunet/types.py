from datetime import datetime
import pydantic
from driver.errors import (
    ZeroError,
    AccuracyError,
    DiscrepancyError,
    TerritoryError,
    StrainError,
)

flunet_to_ncbi = {}
with open("./flunet/data/flunet_to_ncbi.csv", "r") as flunet_ncbi:
    for record in flunet_ncbi:
        key, value = record.split(",")
        value = value.strip()
        flunet_to_ncbi[value] = key


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
    adjacent_nodes: list[tuple[dict, str, str]]

    @pydantic.root_validator(pre=True)
    @classmethod
    def report_node_accuracy(cls, values):
        """
        Check main flunet report node.
        """
        if values["neo4j_point"] != values["row_data"]:
            raise AccuracyError(
                values=values, message="FluNet node and data are not equal."
            )
        return values

    @pydantic.root_validator(pre=True)
    @classmethod
    def adjacent_node_accuracy(cls, values):
        """
        Check strain and territory node accuracy.
        """
        for node, edge_type, relationship in values["adjacent_nodes"]:
            if edge_type == "REPORT" and "host" not in relationship:
                name = node["name"]
                node_strain = flunet_ncbi[name]
                data_strain = values["row_data"][node_strain]
                if data_strain == 0:
                    raise StrainError(
                        values=values, message="Strain does not match data."
                    )

            if edge_type == "IN":
                node_territory = node["name"]
                data_territory = values["row_data"]["Territory"]
                territory = node_territory == data_territory
                if not territory:
                    raise TerritoryError(
                        values=values, message="Territory does not match data."
                    )
        return values
