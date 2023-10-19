from loguru import logger
from virion.valid_virion import valid_virion


async def ingest_virion(session):
    with open("virion/data/Virion.csv", "r", encoding="utf-8") as virion:
        virion = valid_virion()
