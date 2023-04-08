import pytest
import os
from datetime import datetime
from src.database.db import Database
from src.database.db_utils import insert_data, delete_old_data
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_PORT = os.environ['DB_PORT']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']


@pytest.fixture(scope='module')
def db_connection():
    """
    Creates a temporary database connection for testing purposes.

    Returns:
        psycopg2.extensions.connection: A PostgreSQL connection object.
    """

    db = Database(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
            
    conn = db.get_connection()
    cur = conn.cursor()

    test_economy_data_table = '''
        CREATE TABLE IF NOT EXISTS test_economy_data (
            ticker_name VARCHAR(40) NOT NULL,
            dates DATE NOT NULL,
            values FLOAT,
            date_created DATE NOT NULL DEFAULT CURRENT_DATE,
            PRIMARY KEY (ticker_name, dates, date_created)
        );
    '''

    test_market_news_table = '''
        CREATE TABLE IF NOT EXISTS test_market_news (
            id SERIAL,
            title TEXT,
            description TEXT,
            url TEXT,
            source TEXT,
            image TEXT,
            category VARCHAR(20),
            language VARCHAR(10),
            country VARCHAR(5),
            date_created DATE NOT NULL DEFAULT CURRENT_DATE,
            PRIMARY KEY (id)
        );
    '''

    cur.execute(test_economy_data_table)
    cur.execute(test_market_news_table)
    conn.commit()

    yield conn

    cur.execute("DROP TABLE test_economy_data")
    cur.execute("DROP TABLE test_market_news")
    conn.commit()
    cur.close()
    conn.close()


def test_insert_data_economy_data(db_connection):
    """Test the data insert operation in test economy table."""

    test_data = (
        ('test_ticker_1', datetime(year=2023, month=4, day=6), 7),
        ('test_ticker_2', datetime(year=2023, month=4, day=6), 3.8),
        ('test_ticker_3', datetime(year=2023, month=4, day=6), 2889),
    )

    economy_sql_query = 'INSERT INTO test_economy_data (ticker_name, dates, values) VALUES (%s, %s, %s)'
    insert_data(test_data, db_connection, economy_sql_query)
    db_connection.commit()

    cur = db_connection.cursor()
    cur.execute("SELECT COUNt(*) FROM test_economy_data")
    num_of_records = cur.fetchone()[0]

    assert num_of_records == len(test_data)
    cur.close()