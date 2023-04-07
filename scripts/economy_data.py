import aiohttp
import asyncio
from scripts.utils import get_response_data
from config.urls import INDICATORS
from datetime import datetime


async def get_all_indicators_data() -> tuple:
    """Get economy indicator data for indicators."""

    indicators = INDICATORS

    async with aiohttp.ClientSession() as session:
        all_records = []

        try:
            responses = await asyncio.gather(*[get_response_data(session, indicator['url']) for indicator in indicators], return_exceptions=True)

            error_count = 0
            for response in responses:
                if isinstance(response, ConnectionError) == False:
                    records = [( response['ticker'], datetime.strptime(record[0],'%Y-%m-%d').date() , record[1]) 
                                        for record in zip(response['data']['dates'], response['data']['values'])]
                    all_records += records
                else:
                    error_count += 1

            if error_count != 0:
                raise ConnectionError("Error in connection in one of the urls.")
        except Exception as error:
            raise ConnectionError(error)

        return tuple(all_records)