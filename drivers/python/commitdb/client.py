"""
CommitDB Client - Python driver for CommitDB SQL Server.
"""

import json
import socket
from dataclasses import dataclass, field
from typing import Iterator, Optional


class CommitDBError(Exception):
    """Exception raised for CommitDB errors."""
    pass


@dataclass
class QueryResult:
    """Result from a SELECT query."""
    columns: list[str]
    data: list[list[str]]
    records_read: int
    time_ms: float

    def __iter__(self) -> Iterator[dict[str, str]]:
        """Iterate over rows as dictionaries."""
        for row in self.data:
            yield dict(zip(self.columns, row))

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, index: int) -> dict[str, str]:
        return dict(zip(self.columns, self.data[index]))


@dataclass
class CommitResult:
    """Result from a mutation operation (INSERT, UPDATE, DELETE, CREATE, DROP)."""
    databases_created: int = 0
    databases_deleted: int = 0
    tables_created: int = 0
    tables_deleted: int = 0
    records_written: int = 0
    records_deleted: int = 0
    time_ms: float = 0.0

    @property
    def affected_rows(self) -> int:
        """Total number of affected rows/objects."""
        return (self.databases_created + self.databases_deleted +
                self.tables_created + self.tables_deleted +
                self.records_written + self.records_deleted)


class CommitDB:
    """
    CommitDB Python client.

    Example:
        db = CommitDB('localhost', 3306)
        db.connect()
        result = db.query('SELECT * FROM mydb.users')
        db.close()
    """

    def __init__(self, host: str = 'localhost', port: int = 3306):
        """
        Initialize CommitDB client.

        Args:
            host: Server hostname
            port: Server port (default 3306)
        """
        self.host = host
        self.port = port
        self._socket: Optional[socket.socket] = None
        self._buffer = b''

    def connect(self, timeout: float = 10.0) -> 'CommitDB':
        """
        Connect to the CommitDB server.

        Args:
            timeout: Connection timeout in seconds

        Returns:
            self for method chaining
        """
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(timeout)
        self._socket.connect((self.host, self.port))
        return self

    def close(self) -> None:
        """Close the connection."""
        if self._socket:
            try:
                self._socket.send(b'quit\n')
            except Exception:
                pass
            self._socket.close()
            self._socket = None

    def __enter__(self) -> 'CommitDB':
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _send(self, query: str) -> dict:
        """Send a query and receive the response."""
        if not self._socket:
            raise CommitDBError("Not connected. Call connect() first.")

        # Send query with newline
        self._socket.send((query + '\n').encode('utf-8'))

        # Read response until newline
        while b'\n' not in self._buffer:
            chunk = self._socket.recv(4096)
            if not chunk:
                raise CommitDBError("Connection closed by server")
            self._buffer += chunk

        # Split at first newline
        line, self._buffer = self._buffer.split(b'\n', 1)

        # Parse JSON response
        try:
            return json.loads(line.decode('utf-8'))
        except json.JSONDecodeError as e:
            raise CommitDBError(f"Invalid response from server: {e}")

    def execute(self, query: str) -> CommitResult | QueryResult:
        """
        Execute a SQL query.

        Args:
            query: SQL query to execute

        Returns:
            QueryResult for SELECT queries, CommitResult for mutations
        """
        response = self._send(query)

        if not response.get('success'):
            raise CommitDBError(response.get('error', 'Unknown error'))

        result_type = response.get('type')
        result_data = response.get('result', {})

        if result_type == 'query':
            return QueryResult(
                columns=result_data.get('columns', []),
                data=result_data.get('data', []),
                records_read=result_data.get('records_read', 0),
                time_ms=result_data.get('time_ms', 0.0)
            )
        elif result_type == 'commit':
            return CommitResult(
                databases_created=result_data.get('databases_created', 0),
                databases_deleted=result_data.get('databases_deleted', 0),
                tables_created=result_data.get('tables_created', 0),
                tables_deleted=result_data.get('tables_deleted', 0),
                records_written=result_data.get('records_written', 0),
                records_deleted=result_data.get('records_deleted', 0),
                time_ms=result_data.get('time_ms', 0.0)
            )
        else:
            # Unknown type, return empty commit result
            return CommitResult()

    def query(self, sql: str) -> QueryResult:
        """
        Execute a SELECT query and return results.

        Args:
            sql: SELECT query

        Returns:
            QueryResult with columns and data
        """
        result = self.execute(sql)
        if not isinstance(result, QueryResult):
            raise CommitDBError("Expected query result, got commit result")
        return result

    def create_database(self, name: str) -> CommitResult:
        """Create a database."""
        result = self.execute(f'CREATE DATABASE {name}')
        if not isinstance(result, CommitResult):
            raise CommitDBError("Expected commit result")
        return result

    def drop_database(self, name: str) -> CommitResult:
        """Drop a database."""
        result = self.execute(f'DROP DATABASE {name}')
        if not isinstance(result, CommitResult):
            raise CommitDBError("Expected commit result")
        return result

    def create_table(self, database: str, table: str, columns: str) -> CommitResult:
        """
        Create a table.

        Args:
            database: Database name
            table: Table name
            columns: Column definitions, e.g. "id INT PRIMARY KEY, name STRING"
        """
        result = self.execute(f'CREATE TABLE {database}.{table} ({columns})')
        if not isinstance(result, CommitResult):
            raise CommitDBError("Expected commit result")
        return result

    def insert(self, database: str, table: str, columns: list[str], values: list) -> CommitResult:
        """
        Insert a row.

        Args:
            database: Database name
            table: Table name
            columns: List of column names
            values: List of values (strings will be quoted)
        """
        cols = ', '.join(columns)
        vals = ', '.join(
            f"'{v}'" if isinstance(v, str) else str(v)
            for v in values
        )
        result = self.execute(f'INSERT INTO {database}.{table} ({cols}) VALUES ({vals})')
        if not isinstance(result, CommitResult):
            raise CommitDBError("Expected commit result")
        return result

    def show_databases(self) -> list[str]:
        """List all databases."""
        result = self.query('SHOW DATABASES')
        return [row[0] for row in result.data] if result.data else []

    def show_tables(self, database: str) -> list[str]:
        """List all tables in a database."""
        result = self.query(f'SHOW TABLES IN {database}')
        return [row[0] for row in result.data] if result.data else []
