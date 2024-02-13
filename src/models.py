from dataclasses import dataclass


@dataclass
class Sample:
    sample_id: str
    sample_date: str
    collector_institution: str
    collector_name: str
