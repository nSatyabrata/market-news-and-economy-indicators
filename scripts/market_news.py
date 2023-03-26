import os
import aiohttp
from scripts.utils import get_response_data
from dotenv import load_dotenv


load_dotenv()
API_KEY = os.environ['API_KEY']


class S3UploadError(Exception):
    '''Custom exception for s3 data upload'''

    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)


async def get_news_data() -> tuple:
    """Return news data."""

    url = f"http://api.mediastack.com/v1/news?access_key={API_KEY}"\
            "&categories=science,technology,entertainment,business,health,sports"\
            "&limit=100&sort=published_desc"
    
    try:
        async with aiohttp.ClientSession() as session:
            try:
                response = await get_response_data(session, url)
                news = response['data']
                data = [
                    (
                    n['author'],
                    n['title'],
                    n['description'],
                    n['url'],
                    n['source'],
                    n['image'],
                    n['category'],
                    n['language'],
                    n['country'],
                    )
                    for n in news
                ]
                return tuple(data)
            except Exception as error:
                raise error
    except Exception as error:
        raise ConnectionError(error)