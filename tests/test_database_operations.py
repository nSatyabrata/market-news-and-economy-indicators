import pytest
import os
from datetime import datetime
from src.database.db import Database
from src.database.db_utils import insert_data, delete_old_data
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_PORT = int(os.environ['DB_PORT'])
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
            port=DB_PORT,
        )
            
    conn = db.get_connection()
    cur = conn.cursor()

    test_economy_data_table = '''
        CREATE TABLE IF NOT EXISTS test_economy_data (
            ticker_name VARCHAR(40) NOT NULL,
            dates DATE NOT NULL,
            values FLOAT,
            date_created DATE NOT NULL,
            PRIMARY KEY (ticker_name, dates, date_created)
        );
    '''

    cur.execute(test_economy_data_table)
    conn.commit()

    yield conn

    cur.execute("DROP TABLE test_economy_data")
    conn.commit()
    cur.close()
    conn.close()


def test_insert_data(db_connection):
    """Test the data insert operation."""

    today = datetime.today().date()
    test_data = (
        ('test_ticker_1', datetime(year=2023, month=4, day=6), 7, today),
        ('test_ticker_2', datetime(year=2023, month=4, day=6), 3.8, today),
        ('test_ticker_3', datetime(year=2023, month=4, day=6), 2889, today),
    )

    economy_sql_query = ('INSERT INTO test_economy_data '
                         '(ticker_name, dates, values, date_created) '
                         'VALUES (%s, %s, %s, %s)')
    insert_data(data=test_data, conn=db_connection, sql_query=economy_sql_query)

    cur = db_connection.cursor()
    cur.execute("SELECT COUNt(*) FROM test_economy_data")
    records_count = cur.fetchone()[0]

    assert records_count == len(test_data)

    cur.execute("DELETE FROM test_economy_data")
    db_connection.commit()
    cur.close()


def test_delete_old_data(db_connection):
    """Test the old data operation."""

    test_old_data = [
        ('test_ticker_1', datetime(year=2023, month=4, day=6), 7, datetime(year=2023, month=4, day=7)),
        ('test_ticker_2', datetime(year=2023, month=4, day=6), 3.8, datetime(year=2023, month=4, day=7)),
        ('test_ticker_3', datetime(year=2023, month=4, day=6), 2889, datetime(year=2023, month=4, day=7))
    ]
    today = datetime.today()
    test_new_data = [
        ('test_ticker_1', datetime(year=2023, month=4, day=6), 7, today),
        ('test_ticker_2', datetime(year=2023, month=4, day=6), 3.8, today),
        ('test_ticker_3', datetime(year=2023, month=4, day=6), 2889, today),
        ('test_ticker_4', datetime(year=2023, month=4, day=6), 43, today)
    ]
    test_data = test_old_data + test_new_data
    insert_query = 'INSERT INTO test_economy_data (ticker_name, dates, values, date_created) VALUES (%s, %s, %s, %s)'

    cur = db_connection.cursor()
    cur.executemany(insert_query, test_data)

    delete_old_data(conn=db_connection, table_name='test_economy_data', date_column='date_created')

    cur.execute("SELECT COUNT(*) FROM test_economy_data")
    new_records_count = cur.fetchone()[0]

    assert new_records_count == len(test_new_data)

    cur.execute("DELETE FROM test_economy_data")
    db_connection.commit()
    cur.close()
