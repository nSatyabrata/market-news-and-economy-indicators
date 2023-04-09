import os
import aiohttp
import asyncio
from datetime import datetime
from src.scripts.utils import get_response_data
from dotenv import load_dotenv


load_dotenv()
API_KEY = os.environ['API_KEY']


async def get_news_data() -> tuple:
    """Get news data for all categories."""

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
            today = datetime.today().date()
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
                        n['country'],
                        today)
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