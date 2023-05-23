import pydantic
from .errors import DetectionError


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
            "Other",
            "Tissue",
            "Fecal",
        ]:
            raise DetectionError(value=value, message="Detection type is null")
