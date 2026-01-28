"""
CommitDB Client - Python driver for CommitDB SQL Server.
"""

import json
import socket
import ssl
from dataclasses import dataclass, field
from typing import Iterator, Optional, Union


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
        # Basic connection (no auth, no SSL)
        db = CommitDB('localhost', 3306)
        db.connect()
        
        # With SSL (verify certificate)
        db = CommitDB('localhost', 3306, use_ssl=True, ssl_ca_cert='cert.pem')
        db.connect()
        
        # With SSL (skip verification - dev only)
        db = CommitDB('localhost', 3306, use_ssl=True, ssl_verify=False)
        db.connect()
        
        # With JWT authentication
        db = CommitDB('localhost', 3306, jwt_token='your.jwt.token')
        db.connect()  # Auto-authenticates
        
        # With both SSL and JWT
        db = CommitDB('localhost', 3306, use_ssl=True, ssl_ca_cert='cert.pem', 
                      jwt_token='your.jwt.token')
        db.connect()
        
        result = db.query('SELECT * FROM mydb.users')
        db.close()
    """

    def __init__(self, host: str = 'localhost', port: int = 3306, 
                 jwt_token: Optional[str] = None,
                 use_ssl: bool = False, ssl_verify: bool = True,
                 ssl_ca_cert: Optional[str] = None):
        """
        Initialize CommitDB client.

        Args:
            host: Server hostname
            port: Server port (default 3306)
            jwt_token: Optional JWT token for authentication
            use_ssl: Enable SSL/TLS encryption
            ssl_verify: Verify server certificate (default True)
            ssl_ca_cert: Path to CA certificate file for verification
        """
        self.host = host
        self.port = port
        self.jwt_token = jwt_token
        self.use_ssl = use_ssl
        self.ssl_verify = ssl_verify
        self.ssl_ca_cert = ssl_ca_cert
        self._socket: Optional[socket.socket] = None
        self._buffer = b''
        self._authenticated = False
        self._identity: Optional[str] = None

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
        
        # Wrap with SSL if enabled
        if self.use_ssl:
            context = ssl.create_default_context()
            if not self.ssl_verify:
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
            elif self.ssl_ca_cert:
                context.load_verify_locations(self.ssl_ca_cert)
            self._socket = context.wrap_socket(self._socket, server_hostname=self.host)
        
        # Auto-authenticate if JWT token provided
        if self.jwt_token:
            self.authenticate_jwt(self.jwt_token)
        
        return self

    def authenticate_jwt(self, token: str) -> dict:
        """
        Authenticate with a JWT token.

        Args:
            token: JWT token string

        Returns:
            Auth response dict with 'authenticated', 'identity', 'expires_in'

        Raises:
            CommitDBError: If authentication fails
        """
        response = self._send(f'AUTH JWT {token}')
        
        if not response.get('success'):
            raise CommitDBError(f"Authentication failed: {response.get('error')}")
        
        result = response.get('result', {})
        self._authenticated = result.get('authenticated', False)
        self._identity = result.get('identity')
        
        return result

    @property
    def authenticated(self) -> bool:
        """Whether this connection is authenticated."""
        return self._authenticated

    @property
    def identity(self) -> Optional[str]:
        """The authenticated identity (Name <email>), or None."""
        return self._identity

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

    def create_share(self, name: str, url: str, token: str = None,
                     ssh_key: str = None, passphrase: str = None) -> CommitResult:
        """
        Create a share from an external Git repository.

        Args:
            name: Share name
            url: Git repository URL
            token: Optional authentication token (for HTTPS)
            ssh_key: Optional path to SSH private key
            passphrase: Optional passphrase for SSH key

        Example:
            db.create_share('sample', 'https://github.com/org/data.git')
            db.create_share('private', 'git@github.com:org/data.git', ssh_key='~/.ssh/id_rsa')
        """
        query = f"CREATE SHARE {name} FROM '{url}'"
        if token:
            query += f" WITH TOKEN '{token}'"
        elif ssh_key:
            query += f" WITH SSH KEY '{ssh_key}'"
            if passphrase:
                query += f" PASSPHRASE '{passphrase}'"
        return self.execute(query)

    def sync_share(self, name: str, token: str = None,
                   ssh_key: str = None, passphrase: str = None) -> CommitResult:
        """
        Synchronize a share with its remote repository.

        Args:
            name: Share name
            token: Optional authentication token (for HTTPS)
            ssh_key: Optional path to SSH private key
            passphrase: Optional passphrase for SSH key
        """
        query = f"SYNC SHARE {name}"
        if token:
            query += f" WITH TOKEN '{token}'"
        elif ssh_key:
            query += f" WITH SSH KEY '{ssh_key}'"
            if passphrase:
                query += f" PASSPHRASE '{passphrase}'"
        return self.execute(query)

    def drop_share(self, name: str) -> CommitResult:
        """Drop a share."""
        return self.execute(f"DROP SHARE {name}")

    def show_shares(self) -> list[dict[str, str]]:
        """List all shares."""
        result = self.query('SHOW SHARES')
        return [{'name': row[0], 'url': row[1]} for row in result.data] if result.data else []


class CommitDBLocal:
    """
    CommitDB embedded client using Go bindings.
    
    This mode runs the database engine directly in-process without
    requiring a separate server.

    Example:
        # In-memory database
        db = CommitDBLocal()
        
        # File-based database
        db = CommitDBLocal('/path/to/data')
        
        db.execute('CREATE DATABASE mydb')
        result = db.query('SELECT * FROM mydb.users')
        db.close()
    """

    def __init__(self, path: Optional[str] = None, lib_path: Optional[str] = None):
        """
        Initialize CommitDB embedded client.

        Args:
            path: Path for file-based persistence. If None, uses in-memory storage.
            lib_path: Optional path to libcommitdb shared library.
        """
        from .binding import CommitDBBinding
        
        self._binding = CommitDBBinding
        if lib_path:
            self._binding.load(lib_path)
        
        self._path = path
        self._handle: Optional[int] = None

    def open(self) -> 'CommitDBLocal':
        """Open the database."""
        if self._path:
            self._handle = self._binding.open_file(self._path)
        else:
            self._handle = self._binding.open_memory()
        return self

    def close(self) -> None:
        """Close the database."""
        if self._handle is not None:
            self._binding.close(self._handle)
            self._handle = None

    def __enter__(self) -> 'CommitDBLocal':
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _parse_response(self, response: dict) -> CommitResult | QueryResult:
        """Parse a response dict into result objects."""
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
            return CommitResult()

    def execute(self, query: str) -> CommitResult | QueryResult:
        """
        Execute a SQL query.

        Args:
            query: SQL query to execute

        Returns:
            QueryResult for SELECT queries, CommitResult for mutations
        """
        if self._handle is None:
            raise CommitDBError("Database not open. Call open() first.")
        
        response = self._binding.execute(self._handle, query)
        return self._parse_response(response)

    def query(self, sql: str) -> QueryResult:
        """Execute a SELECT query and return results."""
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

    def create_table(self, database: str, table: str, columns: str) -> CommitResult:
        """Create a table."""
        result = self.execute(f'CREATE TABLE {database}.{table} ({columns})')
        if not isinstance(result, CommitResult):
            raise CommitDBError("Expected commit result")
        return result

    def insert(self, database: str, table: str, columns: list[str], values: list) -> CommitResult:
        """Insert a row."""
        cols = ', '.join(columns)
        vals = ', '.join(
            f"'{v}'" if isinstance(v, str) else str(v)
            for v in values
        )
        result = self.execute(f'INSERT INTO {database}.{table} ({cols}) VALUES ({vals})')
        if not isinstance(result, CommitResult):
            raise CommitDBError("Expected commit result")
        return result

