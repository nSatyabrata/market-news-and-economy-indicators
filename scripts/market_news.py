import os
import aiohttp
import asyncio
from scripts.utils import get_response_data
from dotenv import load_dotenv


load_dotenv()
API_KEY = os.environ['API_KEY']


class S3UploadError(Exception):
    '''Custom exception for s3 data upload'''

    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)

# change this methodology similar to economy data.
async def get_news_data() -> tuple:
    """Return news data."""

    categories = ['technology', 'science', 'business', 'health']
    
    news_urls = ["http://api.mediastack.com/v1/news"
                + f"?access_key={API_KEY}"
                + f"&categories={category}"
                + "&limit=50&sort=published_desc" 
                for category in categories]
    
    async with aiohttp.ClientSession() as session:
        all_news = []

        try:
            responses = await asyncio.gather(*[get_response_data(session, news_url) for news_url in news_urls], return_exceptions=True)

            error_count = 0
            for response in responses:
                if isinstance(response, ConnectionError) == False:
                    data = response['data']
                    news = [
                        (n['title'],
                        n['description'],
                        n['url'],
                        n['source'],
                        n['image'],
                        n['category'],
                        n['language'],
                        n['country'],)
                        for n in data
                    ]
                    all_news += news
                else:
                    error_count += 1

            if error_count != 0:
                raise ConnectionError("Error in connection in one of the urls.")
        except Exception as error:
            raise ConnectionError(error)
        
        return tuple(all_news)