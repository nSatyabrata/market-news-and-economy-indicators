from datetime import datetime
from psycopg2.extensions import connection
from psycopg2.extras import execute_batch
from psycopg2.sql import SQL, Identifier
from src.database.db import DatabaseOperationError


class LatestDataNotFoundError(Exception):
    '''Custom exception if latest data is not available.'''

    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)


def create_tables(conn: connection) -> None:
    """Creates economy_data, market_news tables in database."""

    economy_data_table = '''
        CREATE TABLE IF NOT EXISTS economy_data (
            ticker_name VARCHAR(40) NOT NULL,
            dates date NOT NULL,
            values FLOAT,
            date_created DATE NOT NULL DEFAULT CURRENT_DATE,
            PRIMARY KEY (ticker_name, dates, date_created)
        );
    '''

    market_news_table = '''
        CREATE TABLE IF NOT EXISTS market_news (
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
    try:
        with conn.cursor() as cursor:
            cursor.execute(economy_data_table)
            cursor.execute(market_news_table)
    except Exception as error:
        raise DatabaseOperationError(f"Couldn't create tables: {error}")


def get_latest_date_created(conn: connection, table_name: str, date_column: str) -> datetime.date:
    '''Returns latest date created from given table.'''

    with conn.cursor() as cursor:
            cursor.execute(
                SQL("SELECT MAX({}) FROM {}").format(
                    Identifier(date_column),
                    Identifier(table_name)
                )
            )
            max_date = cursor.fetchone()[0]
    
    return max_date


def insert_data(data: tuple, conn: connection, sql_query: str) -> None:
    """Inserts data into table in database."""

    try:
        with conn.cursor() as cursor:
            execute_batch(cursor, sql_query, data, page_size=1000)
    except Exception as error:
        raise DatabaseOperationError(f"Couldn't insert data: {error}")


def delete_old_data(conn: connection, table_name: str, date_column: str) -> None:
    """Delete old data after inserting new data."""

    try:
        with conn.cursor() as cursor:
            today = datetime.today().date()
            max_date = get_latest_date_created(conn, table_name, date_column)
            
            if max_date == today:
                cursor.execute(
                    SQL("DELETE FROM {} where {} <> %s").format(
                        Identifier(table_name),
                        Identifier(date_column)
                    ),
                    (max_date,)
                )
            else:
                raise LatestDataNotFoundError("Couldn't find latest data.")
    except Exception as error:
        raise DatabaseOperationError(f"Couldn't delete old data: {error}")