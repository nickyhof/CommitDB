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
        # This requires a table to exist
        # Skipped if no tables exist
        databases = connection.list_databases()
        if not databases:
            pytest.skip("No databases available")
        
        tables = connection.list_tables(database=databases[0])
        if not tables:
            pytest.skip("No tables available")
        
        table = connection.table(f"{databases[0]}.{tables[0]}")
        result = table.limit(5).execute()
        assert isinstance(result, pd.DataFrame)
