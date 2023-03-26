import aiohttp
from typing import Coroutine

async def get_response_data(session: aiohttp.ClientSession, url: str) -> Coroutine:
    """Get json response from given URL."""

    try:
        async with session.get(url) as response:
            return await response.json(content_type=None)
    except Exception as error:
        raise ConnectionError(f"Error while fetching data from url due to {error=}.")