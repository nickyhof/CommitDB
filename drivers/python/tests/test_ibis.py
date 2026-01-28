"""Tests for the Ibis backend."""

import pytest

# Skip all tests if ibis is not installed
ibis = pytest.importorskip("ibis")
pd = pytest.importorskip("pandas")


class TestIbisBackendUnit:
    """Unit tests for ibis backend that don't require a server."""
    
    def test_import_backend(self):
        """Test that the backend can be imported."""
        from commitdb import ibis_backend
        assert hasattr(ibis_backend, "Backend")
    
    def test_backend_registered(self):
        """Test that the backend is registered via entry points."""
        from importlib.metadata import entry_points
        
        # Check entry points registration
        eps = entry_points(group='ibis.backends')
        names = [ep.name for ep in eps]
        assert 'commitdb' in names
    
    def test_type_mapping(self):
        """Test CommitDB to Ibis type mapping."""
        from commitdb.ibis_backend import COMMITDB_TYPE_MAP
        import ibis.expr.datatypes as dt
        
        assert COMMITDB_TYPE_MAP["INT"] == dt.Int64
        assert COMMITDB_TYPE_MAP["STRING"] == dt.String
        assert COMMITDB_TYPE_MAP["FLOAT"] == dt.Float64
        assert COMMITDB_TYPE_MAP["BOOL"] == dt.Boolean
    
    def test_backend_instantiation(self):
        """Test that the backend can be instantiated."""
        from commitdb.ibis_backend import Backend
        
        backend = Backend()
        assert backend.name == "commitdb"
        assert backend._client is None


@pytest.mark.integration
class TestIbisBackendIntegration:
    """Integration tests that require a running CommitDB server.
    
    Run with: pytest -m integration tests/test_ibis.py
    """
    
    @pytest.fixture
    def connection(self):
        """Create a connection to the test server."""
        from commitdb.ibis_backend import Backend
        
        backend = Backend()
        try:
            backend.do_connect(host="localhost", port=3306, database="test")
            yield backend
        finally:
            backend.disconnect()
    
    def test_connect(self, connection):
        """Test connecting to the server."""
        assert connection._client is not None
    
    def test_list_databases(self, connection):
        """Test listing databases."""
        databases = connection.list_databases()
        assert isinstance(databases, list)
    
    def test_query_to_dataframe(self, connection):
        """Test that queries return pandas DataFrames."""
        from commitdb.client import CommitDBError
        
        # Setup: create database (ignore if exists)
        try:
            connection._client.execute("CREATE DATABASE ibis_test")
        except CommitDBError:
            pass  # Already exists
        
        connection._client.execute("DROP TABLE IF EXISTS ibis_test.users")
        connection._client.execute(
            "CREATE TABLE ibis_test.users (id INT PRIMARY KEY, name STRING, age INT)"
        )
        connection._client.execute(
            "INSERT INTO ibis_test.users (id, name, age) VALUES (1, 'Alice', 30)"
        )
        connection._client.execute(
            "INSERT INTO ibis_test.users (id, name, age) VALUES (2, 'Bob', 25)"
        )
        
        try:
            # Query using Ibis
            table = connection.table("ibis_test.users")
            result = table.execute()
            
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2
            assert "id" in result.columns
            assert "name" in result.columns
            assert "age" in result.columns
        finally:
            # Cleanup
            connection._client.execute("DROP TABLE IF EXISTS ibis_test.users")
            connection._client.execute("DROP DATABASE IF EXISTS ibis_test")
    
    def test_drop_table(self, connection):
        """Test dropping a table via the backend."""
        from commitdb.client import CommitDBError
        
        # Setup: create database (ignore if exists)
        try:
            connection._client.execute("CREATE DATABASE ibis_test")
        except CommitDBError:
            pass
        
        connection._client.execute("DROP TABLE IF EXISTS ibis_test.temp_table")
        connection._client.execute(
            "CREATE TABLE ibis_test.temp_table (id INT PRIMARY KEY)"
        )
        
        try:
            # Drop using the backend method
            connection.drop_table("temp_table", database="ibis_test")
            
            # Verify table is gone
            tables = connection.list_tables(database="ibis_test")
            assert "temp_table" not in tables
        finally:
            # Cleanup
            connection._client.execute("DROP TABLE IF EXISTS ibis_test.temp_table")
            connection._client.execute("DROP DATABASE IF EXISTS ibis_test")
    
    def test_drop_table_force(self, connection):
        """Test dropping a non-existent table with force=True."""
        from commitdb.client import CommitDBError
        
        # Setup: create database (ignore if exists)
        try:
            connection._client.execute("CREATE DATABASE ibis_test")
        except CommitDBError:
            pass
        
        try:
            # Should not raise an error with force=True
            connection.drop_table("nonexistent_table", database="ibis_test", force=True)
        finally:
            connection._client.execute("DROP DATABASE IF EXISTS ibis_test")

