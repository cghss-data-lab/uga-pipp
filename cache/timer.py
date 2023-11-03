import time
from functools import wraps
from loguru import logger


def timer(function):
    @wraps(function)
    def quantize(*args):
        start = time.time()
        result = function(*args)
        end = time.time()

        logger.debug(f"Function: {function} Seconds: {end - start}")
        return result

    return quantize
