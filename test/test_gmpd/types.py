import pydantic
from .errors import DetectionError, PrevalenceError


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
