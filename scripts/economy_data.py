import aiohttp
import asyncio
from scripts.utils import get_response_data
from datetime import datetime
from database.db_utils import insert_data, delete_old_data


async def get_all_indicators_data(indicators: dict) -> tuple:
    """Get all the data from all available urls asynchronously."""

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