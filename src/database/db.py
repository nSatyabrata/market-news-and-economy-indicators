import psycopg2
from psycopg2.extensions import connection


class DatabaseConnectionError(Exception):
    """Custom exception for database connection error."""

    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)


class DatabaseOperationError(Exception):
    """Custom exception for database operation error."""

    def __init__(self, message) -> None:
        self.message = message
        super().__init__(self.message)


class Database:
    """
    A class for interacting with a PostgreSQL database.

    Methods:
        get_connection(): Get connection of database.
        disconnect(): Disconnects from the database.
    """

    def __init__(self, host: str, database: str, user: str, password: str, port: int = 5432) -> None:
        try:
            self._connection = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port
            )
        except Exception as error:
            raise DatabaseConnectionError(f"Unable to connect to database.{error=}")

    def get_connection(self) -> connection:
        """Connects to the database."""
        if self._connection:
            return self._connection
        else:
            raise DatabaseOperationError(f"Connection is closed. Create a new connection.")

    def disconnect(self):
        """Disconnects from the database."""
        if self._connection:
            self._connection.close()
            self._connection = None
        else:
            raise DatabaseOperationError(f"Connection is already closed.")
