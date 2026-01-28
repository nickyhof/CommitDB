"""
CommitDB Ibis Backend

Provides an Ibis backend for CommitDB, enabling pandas DataFrame support
and lazy expression evaluation.

Usage:
    import ibis

    # Connect using ibis.commitdb
    con = ibis.commitdb.connect('localhost', 3306, database='mydb')

    # Or use URL-based connection
    con = ibis.connect('commitdb://localhost:3306/mydb')

    # Query with Ibis expressions
    users = con.table('users')
    result = users.filter(users.age > 30).execute()  # Returns pandas DataFrame
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
import ibis.expr.types as ir
from ibis.backends.sql import SQLBackend
from ibis.backends.sql.compilers import SQLiteCompiler

from . import __version__

from .client import CommitDB as CommitDBClient, CommitDBError

if TYPE_CHECKING:
    import pandas as pd
    import sqlglot as sg


# Type mapping from CommitDB types to Ibis types
# Based on: STRING, INT, INTEGER, FLOAT, DOUBLE, REAL, BOOL, BOOLEAN, TEXT, DATE, TIMESTAMP, DATETIME, JSON
COMMITDB_TYPE_MAP = {
    # String types
    "STRING": dt.String,
    "TEXT": dt.String,
    # Integer types
    "INT": dt.Int64,
    "INTEGER": dt.Int64,
    # Float types
    "FLOAT": dt.Float64,
    "DOUBLE": dt.Float64,
    "REAL": dt.Float64,
    # Boolean types
    "BOOL": dt.Boolean,
    "BOOLEAN": dt.Boolean,
    # Date/time types
    "DATE": dt.Date,
    "TIMESTAMP": dt.Timestamp,
    "DATETIME": dt.Timestamp,
    # JSON type
    "JSON": dt.JSON,
}


class CommitDBCompiler(SQLiteCompiler):
    """SQL compiler for CommitDB.
    
    CommitDB's SQL is similar enough to SQLite that we can reuse most of it.
    """
    __slots__ = ()
    dialect = "sqlite"  # Use SQLite dialect for sqlglot


class Backend(SQLBackend):
    """Ibis backend for CommitDB.
    
    This backend connects to a CommitDB server and executes queries,
    returning results as pandas DataFrames.
    """
    
    name = "commitdb"
    compiler = CommitDBCompiler
    supports_temporary_tables = False
    supports_python_udfs = False
    
    @classmethod
    def _from_url(cls, url, **kwargs) -> "Backend":
        """Create a backend from a URL.
        
        URL format: commitdb://host:port/database
        
        Examples
        --------
        >>> ibis.connect("commitdb://localhost:3306/mydb")
        """
        from urllib.parse import urlparse, ParseResult
        
        # Handle both string URLs and pre-parsed ParseResult
        if isinstance(url, str):
            parsed = urlparse(url)
        else:
            parsed = url  # Already a ParseResult from ibis.connect()
        
        host = parsed.hostname or "localhost"
        port = parsed.port or 3306
        database = parsed.path.lstrip("/") or None
        
        # Parse query params for additional options
        if parsed.query:
            from urllib.parse import parse_qs
            params = parse_qs(parsed.query)
            for key, values in params.items():
                if key not in kwargs:
                    kwargs[key] = values[0] if len(values) == 1 else values
        
        backend = cls()
        backend.do_connect(host=host, port=port, database=database, **kwargs)
        return backend
    
    def __init__(self):
        super().__init__()
        self._client: CommitDBClient | None = None
        self._current_database: str | None = None
    
    def _get_schema_using_query(self, query: str) -> sch.Schema:
        """Get schema by executing a query with LIMIT 1.
        
        Note: CommitDB doesn't support subqueries, so we execute the query
        directly with a limit to infer the schema from results.
        """
        client = self._ensure_connected()
        
        # Add LIMIT 1 if not already present to minimize data transfer
        query_upper = query.strip().upper()
        if "LIMIT" not in query_upper:
            query = f"{query.rstrip().rstrip(';')} LIMIT 1"
        
        try:
            result = client.query(query)
            # Infer schema from column names (all strings for now since
            # CommitDB returns string values over the wire)
            fields = {col: dt.String(nullable=True) for col in result.columns}
            return sch.Schema(fields)
        except Exception:
            # If query fails, return empty schema
            return sch.Schema({})
    
    def _register_in_memory_table(self, op: Any) -> None:
        """Register a table for in-memory operations.
        
        CommitDB doesn't support in-memory tables, so this is a no-op.
        Required by SQLBackend abstract base class.
        """
        pass
    
    @property
    def version(self) -> str:
        """Return CommitDB Python driver version."""
        return __version__
    
    @property
    def current_database(self) -> str | None:
        """Return the current database."""
        return self._current_database
    
    def connect(self, *args, **kwargs) -> "Backend":
        """Connect to the database.
        
        Creates a new backend instance and connects it.
        """
        new = self.__class__()
        new.do_connect(*args, **kwargs)
        return new
    
    def do_connect(
        self,
        host: str = "localhost",
        port: int = 3306,
        database: str | None = None,
        use_ssl: bool = False,
        ssl_verify: bool = True,
        ssl_ca_cert: str | None = None,
        jwt_token: str | None = None,
    ) -> None:
        """Connect to a CommitDB server.
        
        Parameters
        ----------
        host
            Server hostname
        port
            Server port (default 3306)
        database
            Default database to use
        use_ssl
            Enable SSL/TLS encryption
        ssl_verify
            Verify server certificate
        ssl_ca_cert
            Path to CA certificate file
        jwt_token
            JWT token for authentication
        """
        self._client = CommitDBClient(
            host=host,
            port=port,
            use_ssl=use_ssl,
            ssl_verify=ssl_verify,
            ssl_ca_cert=ssl_ca_cert,
            jwt_token=jwt_token,
        )
        self._client.connect()
        self._current_database = database
    
    def disconnect(self) -> None:
        """Close the connection."""
        if self._client:
            self._client.close()
            self._client = None
    
    def _ensure_connected(self) -> CommitDBClient:
        """Ensure we have a valid connection."""
        if self._client is None:
            raise CommitDBError("Not connected. Call connect() first.")
        return self._client
    
    def list_databases(self, *, like: str | None = None) -> list[str]:
        """List all databases."""
        client = self._ensure_connected()
        result = client.query("SHOW DATABASES")
        databases = [row[0] for row in result.data]
        return self._filter_with_like(databases, like)
    
    def list_tables(
        self, *, like: str | None = None, database: str | None = None
    ) -> list[str]:
        """List tables in a database."""
        client = self._ensure_connected()
        db = database or self._current_database
        if not db:
            raise CommitDBError("No database specified. Use database parameter or set current_database.")
        
        result = client.query(f"SHOW TABLES IN {db}")
        tables = [row[0] for row in result.data]
        return self._filter_with_like(tables, like)
    
    def _parse_type(self, type_str: str) -> dt.DataType:
        """Parse a CommitDB type string into an Ibis DataType."""
        type_upper = type_str.upper().strip()
        
        # Handle PRIMARY KEY suffix
        if "PRIMARY KEY" in type_upper:
            type_upper = type_upper.replace("PRIMARY KEY", "").strip()
        
        type_class = COMMITDB_TYPE_MAP.get(type_upper, dt.String)
        return type_class(nullable=True)
    
    def get_schema(
        self,
        table_name: str,
        *,
        catalog: str | None = None,
        database: str | None = None,
    ) -> sch.Schema:
        """Get the schema of a table."""
        client = self._ensure_connected()
        
        # Handle database.table format
        if "." in table_name:
            db, tbl = table_name.split(".", 1)
        else:
            db = database or self._current_database
            tbl = table_name
            if not db:
                raise CommitDBError("No database specified.")
        
        # Get table structure using DESCRIBE
        result = client.query(f"DESCRIBE {db}.{tbl}")
        
        # Parse columns: DESCRIBE returns (name, type) tuples
        fields = {}
        for row in result.data:
            col_name = row[0]
            col_type = row[1] if len(row) > 1 else "STRING"
            fields[col_name] = self._parse_type(col_type)
        
        return sch.Schema(fields)
    
    def raw_sql(self, query: str | sg.Expression, **kwargs: Any) -> Any:
        """Execute raw SQL."""
        client = self._ensure_connected()
        if not isinstance(query, str):
            query = query.sql(dialect="sqlite")
        return client.execute(query)
    
    def _safe_raw_sql(self, query: str | sg.Expression, **kwargs):
        """Execute SQL and yield the result."""
        yield self.raw_sql(query, **kwargs)
    
    def execute(
        self,
        expr: ir.Expr,
        params: dict | None = None,
        limit: str | None = "default",
        **kwargs: Any,
    ) -> pd.DataFrame:
        """Execute an Ibis expression and return a pandas DataFrame."""
        import pandas as pd
        
        # Compile expression to SQL
        sql = self.compile(expr, params=params, limit=limit)
        
        # Execute
        client = self._ensure_connected()
        result = client.query(sql)
        
        # Convert to DataFrame - pandas infers types from the JSON response
        return pd.DataFrame(result.data, columns=result.columns)
    
    def create_table(
        self,
        name: str,
        obj: ir.Table | pd.DataFrame | None = None,
        *,
        schema: sch.Schema | None = None,
        database: str | None = None,
        temp: bool = False,
        overwrite: bool = False,
    ) -> ir.Table:
        """Create a table."""
        client = self._ensure_connected()
        db = database or self._current_database
        if not db:
            raise CommitDBError("No database specified.")
        
        full_name = f"{db}.{name}"
        
        if overwrite:
            client.execute(f"DROP TABLE IF EXISTS {full_name}")
        
        if schema is not None:
            # Create from schema
            cols = ", ".join(
                f"{col} {self._ibis_type_to_commitdb(dtype)}"
                for col, dtype in schema.items()
            )
            client.execute(f"CREATE TABLE {full_name} ({cols})")
        
        if obj is not None:
            import pandas as pd
            if isinstance(obj, pd.DataFrame):
                # Insert data from DataFrame
                self._insert_dataframe(full_name, obj)
        
        return self.table(name, database=db)
    
    def _ibis_type_to_commitdb(self, dtype: dt.DataType) -> str:
        """Convert Ibis DataType to CommitDB type string."""
        if isinstance(dtype, (dt.Int8, dt.Int16, dt.Int32, dt.Int64,
                              dt.UInt8, dt.UInt16, dt.UInt32, dt.UInt64)):
            return "INT"
        elif isinstance(dtype, (dt.Float32, dt.Float64)):
            return "FLOAT"
        elif isinstance(dtype, dt.String):
            return "STRING"
        elif isinstance(dtype, dt.Boolean):
            return "BOOL"
        elif isinstance(dtype, dt.Date):
            return "DATE"
        elif isinstance(dtype, dt.Timestamp):
            return "TIMESTAMP"
        elif isinstance(dtype, dt.JSON):
            return "JSON"
        else:
            return "STRING"
    
    def _insert_dataframe(self, table: str, df: pd.DataFrame) -> None:
        """Insert a pandas DataFrame into a table."""
        client = self._ensure_connected()
        
        if df.empty:
            return
        
        cols = ", ".join(df.columns)
        
        # Build multi-value INSERT
        values_list = []
        for _, row in df.iterrows():
            vals = []
            for v in row:
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    vals.append("NULL")
                elif isinstance(v, str):
                    # Escape single quotes
                    escaped = v.replace("'", "''")
                    vals.append(f"'{escaped}'")
                elif isinstance(v, bool):
                    vals.append("1" if v else "0")
                else:
                    vals.append(str(v))
            values_list.append(f"({', '.join(vals)})")
        
        # Insert in batches of 100
        batch_size = 100
        for i in range(0, len(values_list), batch_size):
            batch = values_list[i:i + batch_size]
            sql = f"INSERT INTO {table} ({cols}) VALUES {', '.join(batch)}"
            client.execute(sql)
    
    def insert(
        self,
        table_name: str,
        obj: pd.DataFrame,
        *,
        database: str | None = None,
    ) -> None:
        """Insert data from a pandas DataFrame into a table."""
        db = database or self._current_database
        if not db:
            raise CommitDBError("No database specified.")
        
        full_name = f"{db}.{table_name}"
        self._insert_dataframe(full_name, obj)
    
    def drop_table(
        self,
        name: str,
        *,
        database: str | None = None,
        force: bool = False,
    ) -> None:
        """Drop a table.
        
        Parameters
        ----------
        name
            Table name to drop
        database
            Database containing the table
        force
            If True, use IF EXISTS to avoid error if table doesn't exist
        """
        client = self._ensure_connected()
        db = database or self._current_database
        if not db:
            raise CommitDBError("No database specified.")
        
        full_name = f"{db}.{name}"
        if_exists = "IF EXISTS " if force else ""
        client.execute(f"DROP TABLE {if_exists}{full_name}")


def connect(
    host: str = "localhost",
    port: int = 3306,
    database: str | None = None,
    **kwargs,
) -> Backend:
    """Connect to a CommitDB server.
    
    Parameters
    ----------
    host
        Server hostname
    port
        Server port (default 3306)
    database
        Default database to use
    **kwargs
        Additional connection parameters (use_ssl, jwt_token, etc.)
    
    Returns
    -------
    Backend
        Connected Ibis backend
    
    Examples
    --------
    >>> import ibis
    >>> con = ibis.commitdb.connect('localhost', 3306, database='mydb')
    >>> users = con.table('users')
    >>> users.filter(users.age > 30).execute()
    """
    backend = Backend()
    backend.do_connect(host=host, port=port, database=database, **kwargs)
    return backend

