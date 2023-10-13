import asyncio


async def handle_concurrency(*tasks, n_semaphore: int = 10):
    semaphore = asyncio.Semaphore(n_semaphore)

    async def sem_coro(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_coro(c) for c in tasks))
