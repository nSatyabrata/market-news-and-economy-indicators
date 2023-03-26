import os
import sys
import asyncio
import logging
from dotenv import load_dotenv
from psycopg2.extensions import connection
from config.urls import INDICATORS
from database.db import Database
from database.db_utils import create_tables, insert_data, delete_old_data, OldDataNotFoundError
from scripts.economy_data import get_all_indicators_data
from scripts.market_news import get_news_data


load_dotenv()

DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_PORT = os.environ['DB_PORT']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']


# logging setup
logger = logging.getLogger("News-Logger")
logger.setLevel(logging.INFO)

# checking if logger is already available(AWS lambda)
if len(logging.getLogger().handlers) > 0:
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().handlers[0].setFormatter(
        logging.Formatter('%(asctime)s :: %(name)s :: %(levelname)s :: %(message)s')
    )
# if not available creating a new logger
else:
    s_handler = logging.StreamHandler(sys.stdout)
    s_handler.setLevel(logging.INFO)
    s_handler.setFormatter(
        logging.Formatter('%(asctime)s :: %(name)s :: %(levelname)s :: %(message)s')
    )
    logging.getLogger().addHandler(s_handler)


async def tasks():
    try:
        db = Database(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        conn = db.get_connection()
        logger.info("Database connection successful.")

        # create table if not exists
        create_tables(conn)

        try:
            task1 = asyncio.create_task(get_all_indicators_data(indicators=INDICATORS))
            task2 = asyncio.create_task(get_news_data())

            economy_data = await task1
            logger.info("Got all economy indicators data successfully.")

            news_data = await task2
            logger.info("Got all news data successfully.")

            #insert economy data
            if economy_data:
                try:
                    sql_query = 'INSERT INTO economy_data (ticker_name, dates, values) VALUES (%s, %s, %s)'
                    insert_data(economy_data, conn, sql_query)
                    logger.info("Inserted new economy indicators data.")

                    # delete old economy data
                    delete_old_data(conn, table_name='economy_data', date_column='date_created')
                    logger.info("Deleted old economy indicators data.")
                except Exception as error:
                    logger.error(error)

            # insert news data
            if news_data:
                try:
                    sql_query = '''
                        INSERT INTO market_news (author, title, description, url, source, image, category, language, country)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    '''
                    insert_data(news_data, conn, sql_query)
                    logger.info("Inserted new news data.")

                    # delete old news data
                    delete_old_data(conn, table_name='market_news', date_column='date_created')
                    logger.info("Deleted old news data.")
                except Exception as error:
                    logger.error(error)  
                    
        except Exception as error:
            logger.error(error)
        finally:
            conn.commit()
            db.disconnect()
            logger.info("Diconnected database.")

    except Exception as error:
        logger.error(error)


def main():
    asyncio.run(tasks())