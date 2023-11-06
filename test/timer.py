import time
from functools import wraps
from loguru import logger


def timer(function):
    @wraps(function)
    async def quantize(*args, **kwargs):
        start = time.monotonic()
        result = await function(*args, **kwargs)
        end = time.monotonic()

        logger.debug(f"Function: {function} Seconds: {end - start}")
        return result

    return quantize
