import os
import sys
import asyncio
import logging
from datetime import datetime
from dotenv import load_dotenv
from src.database.db import Database
from src.database.db_utils import create_tables, insert_data, delete_old_data, get_latest_date_created
from src.scripts.economy_data import get_all_indicators_data
from src.scripts.market_news import get_news_data


load_dotenv()

DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_PORT = os.environ['DB_PORT']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']


# logging setup
logger = logging.getLogger("NewsAndInfo")
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
            task1 = asyncio.create_task(get_all_indicators_data())
            task2 = asyncio.create_task(get_news_data())

            economy_data = await task1
            logger.info("Got all economy indicators data successfully.")

            news_data = await task2
            logger.info("Got all news data successfully.")

            today = datetime.today().date()

            # get latest date available in economy data table then check and insert new data
            latest_date_economy_table = get_latest_date_created(conn=conn, table_name='economy_data', date_column='date_created')
            if latest_date_economy_table != today:
                # insert economy data
                economy_sql_query = 'INSERT INTO economy_data (ticker_name, dates, values, date_created) VALUES (%s, %s, %s ,%s)'
                insert_data(economy_data, conn, economy_sql_query)
                logger.info("Inserted new economy indicators data.")

                # delete old economy data
                delete_old_data(conn, table_name='economy_data', date_column='date_created')
                logger.info("Deleted old economy indicators data.")
            else:
                logger.warning("Latest economy data already exists.")
            
            # get latest date available in market data table then check and insert new data
            latest_date_news_table = get_latest_date_created(conn=conn, table_name='market_news', date_column='date_created')
            if latest_date_news_table != today:
                # insert news data
                news_sql_query = '''
                    INSERT INTO market_news (title, description, url, source, image, category, language, country, date_created)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                '''
                insert_data(news_data, conn, news_sql_query)
                logger.info("Inserted new news data.")

                # delete old news data
                delete_old_data(conn, table_name='market_news', date_column='date_created')
                logger.info("Deleted old news data.")
            else:
                logger.warning("Latest news data already exists.")

        except Exception as error:
            logger.error(error)

    except Exception as error:
        logger.error(f"Error connecting to database: {error}")

    finally:
        conn.commit()
        db.disconnect()
        logger.info("Disconnected database.")


def main(event, context):
    asyncio.run(tasks())

if __name__ == '__main__':
    main(None, None)