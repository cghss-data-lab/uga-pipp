import os
import pickle
from functools import wraps
from typing import Callable

FILE = os.environ["FILE"]


def cache(function: Callable) -> Callable:
    """
    Simple cache implementation.
    It caches results and allows O(1) access to cache.
    """
    function.cache = {}

    @wraps(function)
    def wrapper(*args):
        try:
            return function.cache[args]
        except KeyError:
            function.cache[args] = result = function(*args)
            return result

    return wrapper


def save_cache(cache: dict, file: str = FILE) -> None:
    with open(FILE, "w") as c:
        pickle.dump(cache, c)


def load_cache(file: str = FILE) -> dict:
    with open(FILE, "r") as c:
        cache = pickle.load(c)
