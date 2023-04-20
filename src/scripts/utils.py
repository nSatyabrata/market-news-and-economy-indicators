import aiohttp
from typing import Coroutine


async def get_response_data(session: aiohttp.ClientSession, url: str) -> Coroutine:
    """
    Asynchronously fetches data from the specified URL using the provided aiohttp.ClientSession object.

    Args:
        session: An aiohttp.ClientSession object used to establish the connection and send the request.
        url: A string representing the URL to fetch data from.

    Returns:
        A coroutine that will return the JSON data retrieved from the specified URL.

    Raises:
        ConnectionError: If an error occurs while fetching data from the URL.
    """
    try:
        async with session.get(url) as response:
            return await response.json(content_type=None)
    except Exception as error:
        raise ConnectionError(f"Error while fetching data from url: {error}.")
