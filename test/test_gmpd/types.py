import pydantic
from driver.errors import (
    DetectionError,
    PrevalenceError,
    AccuracyError,
    HostError,
    PathogenError,
    TerritoryError,
)


class Gmpd(pydantic.BaseModel):
    collected: int
    dataSource: str
    dataSourceRow: int
    detectionType: str
    prevalence: float
    reference: str

    @pydantic.validator("detectionType")
    @classmethod
    def check_detection_type(cls, value):
        if value not in [
            "Serology",
            "PCR",
            "DirectOther",
            "DirectBlood",
            "DirectFecal",
            "Tissue",
            "Fecal",
        ]:
            raise DetectionError(value=value, message="Detection type is unspecified.")
        return value

    @pydantic.validator("prevalence")
    @classmethod
    def correct_prevalence(cls, value):
        if value > 1 or value <= 0:
            raise PrevalenceError(value=value, message="Prevalence is unbounded.")
        return value


class GmpdReport(pydantic.BaseModel):
    row_data: dict
    neo4j_point: Gmpd
    adjacent_nodes: list[tuple[dict, str]]

    @pydantic.root_validator(pre=True)
    @classmethod
    def node_accuracy(cls, values):
        if values["neo4j_point"] != values["row_data"]:
            raise AccuracyError(
                values=values, message="GMPD node and data are not equal."
            )
        return values

    @pydantic.root_validator(pre=True)
    @classmethod
    def adjacent_node_accuracy(cls, values):
        for node, edge_type, relationship in values["adjacent_nodes"]:
            if edge_type == "REPORTS" and "host" in relationship:
                host = node["name"]
                raise HostError(values=values, message="Incorrect host name.")
            if edge_type == "REPORTS" and "pathogen" in relationship:
                pathogen = node["name"]
                raise PathogenError(values=values, message="Incorrect pathogen name.")
            if edge_type == "IN":
                territory = node["name"]
                raise TerritoryError(values=values, message="Incorrect territory name.")

        return values
