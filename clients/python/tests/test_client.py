"""
Tests for CommitDB Python Client.

To run with a live server:
    1. Start the server: go run ./cmd/server
    2. Run tests: pytest clients/python/tests/
"""

import pytest
from commitdb import CommitDB, QueryResult, CommitResult, CommitDBError


class TestQueryResult:
    """Tests for QueryResult class."""

    def test_iteration(self):
        result = QueryResult(
            columns=['id', 'name'],
            data=[['1', 'Alice'], ['2', 'Bob']],
            records_read=2,
            execution_time_ms=1.0
        )

        rows = list(result)
        assert rows == [
            {'id': '1', 'name': 'Alice'},
            {'id': '2', 'name': 'Bob'}
        ]

    def test_len(self):
        result = QueryResult(
            columns=['id'],
            data=[['1'], ['2'], ['3']],
            records_read=3,
            execution_time_ms=1.0
        )
        assert len(result) == 3

    def test_getitem(self):
        result = QueryResult(
            columns=['id', 'name'],
            data=[['1', 'Alice'], ['2', 'Bob']],
            records_read=2,
            execution_time_ms=1.0
        )
        assert result[0] == {'id': '1', 'name': 'Alice'}
        assert result[1] == {'id': '2', 'name': 'Bob'}


class TestCommitResult:
    """Tests for CommitResult class."""

    def test_affected_rows(self):
        result = CommitResult(
            databases_created=1,
            tables_created=2,
            records_written=3
        )
        assert result.affected_rows == 6

    def test_defaults(self):
        result = CommitResult()
        assert result.affected_rows == 0
        assert result.execution_time_ms == 0.0


class TestCommitDBUnit:
    """Unit tests for CommitDB client (no server required)."""

    def test_init(self):
        db = CommitDB('localhost', 3306)
        assert db.host == 'localhost'
        assert db.port == 3306

    def test_init_with_jwt_token(self):
        db = CommitDB('localhost', 3306, jwt_token='test.jwt.token')
        assert db.jwt_token == 'test.jwt.token'
        assert db.authenticated is False
        assert db.identity is None

    def test_not_connected_error(self):
        db = CommitDB('localhost', 3306)
        with pytest.raises(CommitDBError, match="Not connected"):
            db.execute("SELECT 1")

    def test_auth_not_connected_error(self):
        db = CommitDB('localhost', 3306)
        with pytest.raises(CommitDBError, match="Not connected"):
            db.authenticate_jwt("some.jwt.token")


# Integration tests require a running server
# These run automatically in CI where the server is started

import os
SKIP_INTEGRATION = os.environ.get('COMMITDB_SERVER_URL') is None and os.environ.get('CI') is None


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Server not running - set COMMITDB_SERVER_URL or CI env var")
class TestCommitDBIntegration:
    """Integration tests (requires running server)."""

    @pytest.fixture
    def db(self):
        host = os.environ.get('COMMITDB_HOST', 'localhost')
        port = int(os.environ.get('COMMITDB_PORT', '3306'))
        db = CommitDB(host, port)
        db.connect()
        yield db
        db.close()

    def test_create_database(self, db):
        result = db.execute('CREATE DATABASE pytest_int_test1')
        assert isinstance(result, CommitResult)
        assert result.databases_created == 1

    def test_create_table(self, db):
        db.execute('CREATE DATABASE pytest_int_test2')
        result = db.execute('CREATE TABLE pytest_int_test2.users (id INT PRIMARY KEY, name STRING)')
        assert isinstance(result, CommitResult)
        assert result.tables_created == 1

    def test_insert_and_query(self, db):
        db.execute('CREATE DATABASE pytest_int_test3')
        db.execute('CREATE TABLE pytest_int_test3.items (id INT PRIMARY KEY, value STRING)')
        db.execute("INSERT INTO pytest_int_test3.items (id, value) VALUES (1, 'hello')")

        result = db.query('SELECT * FROM pytest_int_test3.items')
        assert isinstance(result, QueryResult)
        assert len(result) == 1
        assert result[0] == {'id': '1', 'value': 'hello'}


# Auth integration tests require a server running with --jwt-secret
# Run with: go run ./cmd/server --jwt-secret "test-secret" &
# Set env: COMMITDB_JWT_SECRET=test-secret pytest clients/python/tests/ -v -k auth

SKIP_AUTH_INTEGRATION = os.environ.get('COMMITDB_JWT_SECRET') is None


@pytest.mark.skipif(SKIP_AUTH_INTEGRATION, reason="Auth server not running - set COMMITDB_JWT_SECRET")
class TestCommitDBAuthIntegration:
    """Integration tests for JWT authentication (requires server with --jwt-secret)."""

    @pytest.fixture
    def jwt_secret(self):
        return os.environ.get('COMMITDB_JWT_SECRET', 'test-secret')

    @pytest.fixture
    def jwt_token(self, jwt_secret):
        """Generate a valid JWT token for testing."""
        import jwt
        import time
        payload = {
            'name': 'Test User',
            'email': 'testuser@example.com',
            'exp': int(time.time()) + 3600,
        }
        return jwt.encode(payload, jwt_secret, algorithm='HS256')

    def test_unauthenticated_rejected(self):
        """Verify server rejects unauthenticated requests."""
        host = os.environ.get('COMMITDB_HOST', 'localhost')
        port = int(os.environ.get('COMMITDB_PORT', '3306'))
        db = CommitDB(host, port)
        db.connect()
        try:
            with pytest.raises(CommitDBError, match="authentication"):
                db.execute('CREATE DATABASE auth_test_reject')
        finally:
            db.close()

    def test_authenticate_jwt(self, jwt_token):
        """Verify JWT authentication works."""
        host = os.environ.get('COMMITDB_HOST', 'localhost')
        port = int(os.environ.get('COMMITDB_PORT', '3306'))
        db = CommitDB(host, port)
        db.connect()
        try:
            result = db.authenticate_jwt(jwt_token)
            assert db.authenticated is True
            assert 'Test User' in db.identity
            assert 'testuser@example.com' in db.identity
        finally:
            db.close()

    def test_auto_authenticate_on_connect(self, jwt_token):
        """Verify jwt_token parameter auto-authenticates on connect."""
        host = os.environ.get('COMMITDB_HOST', 'localhost')
        port = int(os.environ.get('COMMITDB_PORT', '3306'))
        db = CommitDB(host, port, jwt_token=jwt_token)
        db.connect()
        try:
            assert db.authenticated is True
            # Query should work
            result = db.execute('CREATE DATABASE auth_test_auto')
            assert result.databases_created == 1
        finally:
            db.close()

    def test_query_after_auth(self, jwt_token):
        """Verify queries work after authentication."""
        host = os.environ.get('COMMITDB_HOST', 'localhost')
        port = int(os.environ.get('COMMITDB_PORT', '3306'))
        db = CommitDB(host, port)
        db.connect()
        try:
            db.authenticate_jwt(jwt_token)
            
            # Execute various operations
            db.execute('CREATE DATABASE auth_test_query')
            db.execute('CREATE TABLE auth_test_query.items (id INT PRIMARY KEY, name STRING)')
            db.execute("INSERT INTO auth_test_query.items (id, name) VALUES (1, 'test')")
            
            result = db.query('SELECT * FROM auth_test_query.items')
            assert len(result) == 1
            assert result[0]['name'] == 'test'
        finally:
            db.close()


# === SSL Tests ===

class TestCommitDBSSLUnit:
    """Unit tests for CommitDB SSL features (no server required)."""

    def test_init_with_ssl_defaults(self):
        """Verify SSL defaults are set correctly."""
        db = CommitDB('localhost', 3306)
        assert db.use_ssl is False
        assert db.ssl_verify is True
        assert db.ssl_ca_cert is None

    def test_init_with_ssl_enabled(self):
        """Verify SSL parameters are stored correctly."""
        db = CommitDB('localhost', 3306, use_ssl=True, ssl_verify=False)
        assert db.use_ssl is True
        assert db.ssl_verify is False
        assert db.ssl_ca_cert is None

    def test_init_with_ssl_ca_cert(self):
        """Verify SSL CA cert path is stored."""
        db = CommitDB('localhost', 3306, use_ssl=True, ssl_ca_cert='/path/to/cert.pem')
        assert db.use_ssl is True
        assert db.ssl_verify is True
        assert db.ssl_ca_cert == '/path/to/cert.pem'

    def test_init_with_all_options(self):
        """Verify all SSL and auth options can be combined."""
        db = CommitDB(
            'localhost', 3306,
            jwt_token='test.jwt.token',
            use_ssl=True,
            ssl_verify=True,
            ssl_ca_cert='/path/to/cert.pem'
        )
        assert db.jwt_token == 'test.jwt.token'
        assert db.use_ssl is True
        assert db.ssl_verify is True
        assert db.ssl_ca_cert == '/path/to/cert.pem'


# Integration tests for SSL require a TLS-enabled server
# Run with: go run ./cmd/server --tls-cert cert.pem --tls-key key.pem
# Set environment: COMMITDB_SSL_ENABLED=1 COMMITDB_SSL_CERT=cert.pem

@pytest.fixture
def ssl_server_running():
    """Check if SSL server is available."""
    ssl_enabled = os.environ.get('COMMITDB_SSL_ENABLED')
    if not ssl_enabled:
        pytest.skip("SSL server not configured (set COMMITDB_SSL_ENABLED=1)")
    return True


@pytest.fixture
def ssl_cert_path():
    """Get SSL certificate path from environment."""
    cert_path = os.environ.get('COMMITDB_SSL_CERT')
    if not cert_path:
        pytest.skip("SSL certificate not configured (set COMMITDB_SSL_CERT=/path/to/cert.pem)")
    return cert_path


@pytest.mark.skipif(not os.environ.get('COMMITDB_SSL_ENABLED'), 
                    reason="SSL server not configured")
class TestCommitDBSSLIntegration:
    """Integration tests for SSL connections (requires TLS-enabled server)."""

    def test_connect_with_ssl_verify(self, ssl_server_running, ssl_cert_path):
        """Test SSL connection with certificate verification."""
        host = os.environ.get('COMMITDB_HOST', 'localhost')
        port = int(os.environ.get('COMMITDB_PORT', '3306'))
        
        db = CommitDB(host, port, use_ssl=True, ssl_ca_cert=ssl_cert_path)
        db.connect()
        try:
            result = db.execute('SHOW DATABASES')
            assert isinstance(result, QueryResult)
        finally:
            db.close()

    def test_connect_with_ssl_skip_verify(self, ssl_server_running):
        """Test SSL connection skipping certificate verification."""
        host = os.environ.get('COMMITDB_HOST', 'localhost')
        port = int(os.environ.get('COMMITDB_PORT', '3306'))
        
        db = CommitDB(host, port, use_ssl=True, ssl_verify=False)
        db.connect()
        try:
            result = db.execute('SHOW DATABASES')
            assert isinstance(result, QueryResult)
        finally:
            db.close()

    def test_ssl_with_jwt_auth(self, ssl_server_running, ssl_cert_path):
        """Test SSL connection combined with JWT authentication."""
        jwt_secret = os.environ.get('COMMITDB_JWT_SECRET')
        if not jwt_secret:
            pytest.skip("JWT secret not configured (set COMMITDB_JWT_SECRET)")
        
        # Generate a token
        import jwt
        token = jwt.encode(
            {'name': 'SSL Test User', 'email': 'ssltest@example.com'},
            jwt_secret,
            algorithm='HS256'
        )
        
        host = os.environ.get('COMMITDB_HOST', 'localhost')
        port = int(os.environ.get('COMMITDB_PORT', '3306'))
        
        db = CommitDB(host, port, 
                      use_ssl=True, ssl_ca_cert=ssl_cert_path,
                      jwt_token=token)
        db.connect()
        try:
            assert db.authenticated is True
            assert 'SSL Test User' in db.identity
            result = db.execute('SHOW DATABASES')
            assert isinstance(result, QueryResult)
        finally:
            db.close()

    def test_query_over_ssl(self, ssl_server_running, ssl_cert_path):
        """Test executing queries over SSL connection."""
        host = os.environ.get('COMMITDB_HOST', 'localhost')
        port = int(os.environ.get('COMMITDB_PORT', '3306'))
        
        db = CommitDB(host, port, use_ssl=True, ssl_ca_cert=ssl_cert_path)
        db.connect()
        try:
            # Create database
            result = db.execute('CREATE DATABASE ssl_test_db')
            assert result.databases_created == 1
            
            # Create table
            db.execute('CREATE TABLE ssl_test_db.items (id INT PRIMARY KEY, data STRING)')
            
            # Insert data
            db.execute("INSERT INTO ssl_test_db.items (id, data) VALUES (1, 'encrypted')")
            
            # Query data
            result = db.query('SELECT * FROM ssl_test_db.items')
            assert len(result) == 1
            assert result[0]['data'] == 'encrypted'
        finally:
            db.close()

