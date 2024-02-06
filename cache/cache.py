import pickle
from functools import wraps
from typing import Callable

def cache(file: str, is_class=False) -> Callable:
    """
    Simple cache implementation.
    It caches results and allows O(1) access to cache.
    """

    def wrapper(function: Callable):
        try:
            function.cache = load_cache(file)
        except FileNotFoundError:
            function.cache = {}

        @wraps(function)
        async def memoize(*args):
            key = args
            if is_class:
                key = args[1:]
            try:
                return function.cache[key[0]]
            except KeyError:
                function.cache[key[0]] = result = await function(*args)
                await save_cache(function.cache, file)
                return result

        return memoize

    return wrapper


async def save_cache(cache_file: dict, file: str) -> None:
    with open(file, "wb") as c:
        pickle.dump(cache_file, c)


def load_cache(file: str) -> dict:
    with open(file, "rb") as c:
        cache_file = pickle.load(c)
    return cache_file
