"""
Tests for CommitDB Python driver.

To run with a live server:
    1. Start the server: go run ./cmd/server
    2. Run tests: pytest drivers/python/tests/
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
            time_ms=1.0
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
            time_ms=1.0
        )
        assert len(result) == 3

    def test_getitem(self):
        result = QueryResult(
            columns=['id', 'name'],
            data=[['1', 'Alice'], ['2', 'Bob']],
            records_read=2,
            time_ms=1.0
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
        assert result.time_ms == 0.0


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
# Set env: COMMITDB_JWT_SECRET=test-secret pytest drivers/python/tests/ -v -k auth

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


# Embedded mode tests (require libcommitdb shared library)
# Run with: make lib && pytest drivers/python/tests/ -v

import os
from pathlib import Path

# Try to find the shared library
def _find_lib():
    # Path: drivers/python/tests/test_client.py -> repo root is 4 levels up
    repo_root = Path(__file__).parent.parent.parent.parent
    lib_paths = [
        repo_root / 'lib' / 'libcommitdb.dylib',
        repo_root / 'lib' / 'libcommitdb.so',
    ]
    for p in lib_paths:
        if p.exists():
            return str(p)
    return None

LIB_PATH = _find_lib()


@pytest.mark.skipif(LIB_PATH is None, reason="libcommitdb not found - run 'make lib' first")
class TestCommitDBLocal:
    """Tests for embedded mode using Go bindings."""

    @pytest.fixture
    def db(self):
        from commitdb import CommitDBLocal
        db = CommitDBLocal(lib_path=LIB_PATH)
        db.open()
        yield db
        db.close()

    def test_create_database(self, db):
        result = db.execute('CREATE DATABASE local_test1')
        assert isinstance(result, CommitResult)
        assert result.databases_created == 1

    def test_create_table(self, db):
        db.execute('CREATE DATABASE local_test2')
        result = db.execute('CREATE TABLE local_test2.users (id INT PRIMARY KEY, name STRING)')
        assert isinstance(result, CommitResult)
        assert result.tables_created == 1

    def test_insert_and_query(self, db):
        db.execute('CREATE DATABASE local_test3')
        db.execute('CREATE TABLE local_test3.items (id INT PRIMARY KEY, value STRING)')
        db.execute("INSERT INTO local_test3.items (id, value) VALUES (1, 'hello')")
        db.execute("INSERT INTO local_test3.items (id, value) VALUES (2, 'world')")

        result = db.query('SELECT * FROM local_test3.items')
        assert isinstance(result, QueryResult)
        assert len(result) == 2
        assert result[0] == {'id': '1', 'value': 'hello'}
        assert result[1] == {'id': '2', 'value': 'world'}

    def test_update(self, db):
        db.execute('CREATE DATABASE local_test4')
        db.execute('CREATE TABLE local_test4.data (id INT PRIMARY KEY, val STRING)')
        db.execute("INSERT INTO local_test4.data (id, val) VALUES (1, 'old')")
        
        result = db.execute("UPDATE local_test4.data SET val = 'new' WHERE id = 1")
        assert isinstance(result, CommitResult)

        result = db.query('SELECT * FROM local_test4.data WHERE id = 1')
        assert result[0]['val'] == 'new'

    def test_delete(self, db):
        db.execute('CREATE DATABASE local_test5')
        db.execute('CREATE TABLE local_test5.data (id INT PRIMARY KEY)')
        db.execute('INSERT INTO local_test5.data (id) VALUES (1)')
        db.execute('INSERT INTO local_test5.data (id) VALUES (2)')
        
        db.execute('DELETE FROM local_test5.data WHERE id = 1')
        
        result = db.query('SELECT * FROM local_test5.data')
        assert len(result) == 1
        assert result[0]['id'] == '2'

    def test_context_manager(self):
        from commitdb import CommitDBLocal
        with CommitDBLocal(lib_path=LIB_PATH) as db:
            result = db.execute('CREATE DATABASE local_test6')
            assert result.databases_created == 1

    def test_convenience_methods(self, db):
        db.create_database('local_test7')
        db.create_table('local_test7', 'users', 'id INT PRIMARY KEY, name STRING')
        db.insert('local_test7', 'users', ['id', 'name'], [1, 'Alice'])
        
        result = db.query('SELECT * FROM local_test7.users')
        assert len(result) == 1
        assert result[0] == {'id': '1', 'name': 'Alice'}

    def test_error_handling(self, db):
        with pytest.raises(CommitDBError):
            db.query('SELECT * FROM nonexistent.table')

    def test_in_operator(self, db):
        """Test IN operator for filtering."""
        db.execute('CREATE DATABASE in_test')
        db.execute('CREATE TABLE in_test.items (id INT PRIMARY KEY, status STRING, category STRING)')
        
        # Insert test data
        db.execute("INSERT INTO in_test.items (id, status, category) VALUES (1, 'active', 'A')")
        db.execute("INSERT INTO in_test.items (id, status, category) VALUES (2, 'pending', 'B')")
        db.execute("INSERT INTO in_test.items (id, status, category) VALUES (3, 'active', 'C')")
        db.execute("INSERT INTO in_test.items (id, status, category) VALUES (4, 'archived', 'A')")
        db.execute("INSERT INTO in_test.items (id, status, category) VALUES (5, 'pending', 'B')")
        
        # Test IN with strings
        result = db.query("SELECT * FROM in_test.items WHERE status IN ('active', 'pending')")
        assert len(result) == 4
        
        # Test IN with single value
        result = db.query("SELECT * FROM in_test.items WHERE status IN ('archived')")
        assert len(result) == 1
        
        # Test IN with integers
        result = db.query("SELECT * FROM in_test.items WHERE id IN (1, 3, 5)")
        assert len(result) == 3
        
        # Test NOT IN
        result = db.query("SELECT * FROM in_test.items WHERE NOT status IN ('archived')")
        assert len(result) == 4

    def test_alter_table(self, db):
        """Test ALTER TABLE operations."""
        db.execute('CREATE DATABASE alter_test')
        db.execute('CREATE TABLE alter_test.users (id INT PRIMARY KEY, name STRING)')
        
        # Test ADD COLUMN
        result = db.execute('ALTER TABLE alter_test.users ADD COLUMN email STRING')
        assert isinstance(result, CommitResult)
        
        # Verify column was added
        result = db.query('DESCRIBE alter_test.users')
        assert len(result) == 3
        
        # Test MODIFY COLUMN
        result = db.execute('ALTER TABLE alter_test.users MODIFY COLUMN email TEXT')
        assert isinstance(result, CommitResult)
        
        # Test RENAME COLUMN
        result = db.execute('ALTER TABLE alter_test.users RENAME COLUMN email TO contact')
        assert isinstance(result, CommitResult)
        
        # Verify rename
        result = db.query('DESCRIBE alter_test.users')
        column_names = [row['Column'] for row in result]
        assert 'contact' in column_names
        assert 'email' not in column_names
        
        # Test DROP COLUMN
        result = db.execute('ALTER TABLE alter_test.users DROP COLUMN contact')
        assert isinstance(result, CommitResult)
        
        # Verify column was dropped
        result = db.query('DESCRIBE alter_test.users')
        assert len(result) == 2

    def test_string_functions(self, db):
        """Test string functions like UPPER, LOWER, CONCAT, etc."""
        db.execute('CREATE DATABASE strfunc_test')
        db.execute('CREATE TABLE strfunc_test.data (id INT PRIMARY KEY, name STRING)')
        
        db.execute("INSERT INTO strfunc_test.data (id, name) VALUES (1, 'Alice')")
        db.execute("INSERT INTO strfunc_test.data (id, name) VALUES (2, 'Bob')")
        
        # Test UPPER
        result = db.query('SELECT UPPER(name) FROM strfunc_test.data WHERE id = 1')
        assert len(result) == 1
        assert list(result[0].values())[0] == 'ALICE'
        
        # Test LOWER
        result = db.query('SELECT LOWER(name) FROM strfunc_test.data WHERE id = 1')
        assert len(result) == 1
        assert list(result[0].values())[0] == 'alice'
        
        # Test LENGTH
        result = db.query('SELECT LENGTH(name) FROM strfunc_test.data WHERE id = 2')
        assert len(result) == 1
        assert list(result[0].values())[0] == '3'
        
        # Test CONCAT
        result = db.query("SELECT CONCAT(name, '-test') FROM strfunc_test.data WHERE id = 1")
        assert len(result) == 1
        assert list(result[0].values())[0] == 'Alice-test'

    def test_date_functions(self, db):
        """Test date functions like NOW, YEAR, MONTH, DAY, etc."""
        db.execute('CREATE DATABASE datefunc_test')
        db.execute('CREATE TABLE datefunc_test.events (id INT PRIMARY KEY, name STRING, created STRING)')
        
        db.execute("INSERT INTO datefunc_test.events (id, name, created) VALUES (1, 'Event1', '2024-06-15 14:30:00')")
        
        # Test NOW()
        result = db.query('SELECT NOW() FROM datefunc_test.events WHERE id = 1')
        assert len(result) == 1
        assert len(list(result[0].values())[0]) > 0  # Non-empty
        
        # Test YEAR
        result = db.query('SELECT YEAR(created) FROM datefunc_test.events WHERE id = 1')
        assert len(result) == 1
        assert list(result[0].values())[0] == '2024'
        
        # Test MONTH
        result = db.query('SELECT MONTH(created) FROM datefunc_test.events WHERE id = 1')
        assert len(result) == 1
        assert list(result[0].values())[0] == '6'
        
        # Test DAY
        result = db.query('SELECT DAY(created) FROM datefunc_test.events WHERE id = 1')
        assert len(result) == 1
        assert list(result[0].values())[0] == '15'
        
        # Test DATE
        result = db.query('SELECT DATE(created) FROM datefunc_test.events WHERE id = 1')
        assert len(result) == 1
        assert list(result[0].values())[0] == '2024-06-15'

    def test_date_columns(self, db):
        """Test DATE/TIMESTAMP column types and NOW() in INSERT."""
        db.execute('CREATE DATABASE datecol_test')
        db.execute('CREATE TABLE datecol_test.events (id INT PRIMARY KEY, name STRING, event_date DATE, created_at TIMESTAMP)')
        
        # Test INSERT with NOW()
        result = db.execute("INSERT INTO datecol_test.events (id, name, event_date, created_at) VALUES (1, 'Event1', '2024-06-15', NOW())")
        assert isinstance(result, CommitResult)
        
        # Verify the data was inserted
        result = db.query('SELECT created_at FROM datecol_test.events WHERE id = 1')
        assert len(result) == 1
        assert len(list(result[0].values())[0]) > 0  # Non-empty timestamp
        
        # Test NOW() for DATE column
        db.execute("INSERT INTO datecol_test.events (id, name, event_date, created_at) VALUES (2, 'Event2', NOW(), '2024-12-25 08:00:00')")
        result = db.query('SELECT event_date FROM datecol_test.events WHERE id = 2')
        assert len(result) == 1
        assert len(list(result[0].values())[0]) == 10  # Date format YYYY-MM-DD

    def test_create_branch(self, db):
        """Test CREATE BRANCH SQL syntax."""
        db.execute('CREATE DATABASE branch_test1')
        db.execute('CREATE TABLE branch_test1.items (id INT PRIMARY KEY, name STRING)')
        db.execute("INSERT INTO branch_test1.items (id, name) VALUES (1, 'original')")
        
        # Create branch
        result = db.execute('CREATE BRANCH feature')
        assert isinstance(result, CommitResult)
        
        # Show branches
        result = db.query('SHOW BRANCHES')
        assert len(result) >= 2  # master/main + feature
        
    def test_checkout(self, db):
        """Test CHECKOUT SQL syntax."""
        db.execute('CREATE DATABASE branch_test2')
        db.execute('CREATE TABLE branch_test2.data (id INT PRIMARY KEY)')
        db.execute('INSERT INTO branch_test2.data (id) VALUES (1)')
        
        db.execute('CREATE BRANCH feature2')
        
        # Checkout feature branch
        result = db.execute('CHECKOUT feature2')
        assert isinstance(result, CommitResult)
        
        # Make changes on feature branch
        db.execute('INSERT INTO branch_test2.data (id) VALUES (2)')
        
        # Verify 2 rows on feature
        result = db.query('SELECT * FROM branch_test2.data')
        assert len(result) == 2
        
        # Checkout master - should only have 1 row
        db.execute('CHECKOUT master')
        result = db.query('SELECT * FROM branch_test2.data')
        assert len(result) == 1

    def test_bulk_insert(self, db):
        """Test bulk INSERT with multiple value rows."""
        db.execute('CREATE DATABASE bulk_test')
        db.execute('CREATE TABLE bulk_test.items (id INT PRIMARY KEY, name STRING, value INT)')
        
        # Bulk insert multiple rows
        result = db.execute("INSERT INTO bulk_test.items (id, name, value) VALUES (1, 'Item1', 100), (2, 'Item2', 200), (3, 'Item3', 300)")
        assert result.records_written == 3
        
        # Verify all rows were inserted
        result = db.query('SELECT * FROM bulk_test.items ORDER BY id ASC')
        assert len(result) == 3
        assert result[0]['name'] == 'Item1'
        assert result[1]['name'] == 'Item2'
        assert result[2]['name'] == 'Item3'

    def test_copy_into(self, db):
        """Test COPY INTO for bulk CSV import/export."""
        import tempfile
        import os
        
        db.execute('CREATE DATABASE copy_test')
        db.execute('CREATE TABLE copy_test.users (id INT PRIMARY KEY, name STRING, email STRING)')
        
        # Insert data
        db.execute("INSERT INTO copy_test.users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')")
        db.execute("INSERT INTO copy_test.users (id, name, email) VALUES (2, 'Bob', 'bob@test.com')")
        
        # Export to CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            export_path = f.name
        
        try:
            result = db.execute(f"COPY INTO '{export_path}' FROM copy_test.users")
            assert result.records_written == 2
            
            # Verify file exists and has content
            with open(export_path, 'r') as f:
                content = f.read()
            assert 'Alice' in content
            assert 'id,name,email' in content
            
            # Create new table and import
            db.execute('CREATE TABLE copy_test.imported (id INT PRIMARY KEY, name STRING, email STRING)')
            result = db.execute(f"COPY INTO copy_test.imported FROM '{export_path}'")
            assert result.records_written == 2
            
            # Verify imported data
            result = db.query('SELECT * FROM copy_test.imported')
            assert len(result) == 2
        finally:
            os.unlink(export_path)
        
    def test_merge(self, db):
        """Test MERGE SQL syntax."""
        db.execute('CREATE DATABASE branch_test3')
        db.execute('CREATE TABLE branch_test3.data (id INT PRIMARY KEY)')
        db.execute('INSERT INTO branch_test3.data (id) VALUES (1)')
        
        # Create and checkout feature branch
        db.execute('CREATE BRANCH feature3')
        db.execute('CHECKOUT feature3')
        
        # Make changes
        db.execute('INSERT INTO branch_test3.data (id) VALUES (2)')
        
        # Merge back to master
        db.execute('CHECKOUT master')
        result = db.execute('MERGE feature3')
        assert isinstance(result, CommitResult)
        
        # After merge, should have both rows
        result = db.query('SELECT * FROM branch_test3.data')
        assert len(result) == 2


    def test_merge_manual_resolution(self, db):
        """Test MERGE WITH MANUAL RESOLUTION syntax"""
        # Setup
        db.execute('CREATE DATABASE manualtest')
        db.execute('CREATE TABLE manualtest.items (id INT PRIMARY KEY, name STRING)')
        db.execute("INSERT INTO manualtest.items (id, name) VALUES (1, 'Original')")
        
        # Create branch and add data
        db.execute('CREATE BRANCH feature_manual')
        db.execute('CHECKOUT feature_manual')
        db.execute("INSERT INTO manualtest.items (id, name) VALUES (2, 'Feature')")
        
        # Add different data on master
        db.execute('CHECKOUT master')
        db.execute("INSERT INTO manualtest.items (id, name) VALUES (3, 'Master')")
        
        # Merge with manual resolution
        result = db.execute('MERGE feature_manual WITH MANUAL RESOLUTION')
        
        # Check conflicts with SHOW MERGE CONFLICTS
        conflicts = db.query('SHOW MERGE CONFLICTS')
        # Conflicts may or may not exist, both are valid
        
        # If there were conflicts, they would need resolution
        if len(conflicts) > 0:
            # Resolve each conflict
            for conflict in conflicts:
                key = f"{conflict['Database']}.{conflict['Table']}.{conflict['Key']}"
                db.execute(f'RESOLVE CONFLICT {key} USING HEAD')
            
            # Complete merge
            db.execute('COMMIT MERGE')
        
        # After merge, should have data from both branches
        result = db.query('SELECT * FROM manualtest.items')
        assert len(result) >= 2


    def test_abort_merge(self, db):
        """Test ABORT MERGE syntax"""
        # Setup: create a record that will be modified on both branches
        db.execute('CREATE DATABASE aborttest')
        db.execute('CREATE TABLE aborttest.data (id INT PRIMARY KEY, val STRING)')
        db.execute("INSERT INTO aborttest.data (id, val) VALUES (1, 'Original')")
        
        # Create branch and modify the same record
        db.execute('CREATE BRANCH feature_abort')
        db.execute('CHECKOUT feature_abort')
        db.execute("UPDATE aborttest.data SET val = 'FeatureValue' WHERE id = 1")
        
        # Go back to master and modify the same record (creates conflict)
        db.execute('CHECKOUT master')
        db.execute("UPDATE aborttest.data SET val = 'MasterValue' WHERE id = 1")
        
        # Start manual merge - should have conflict on id=1
        db.execute('MERGE feature_abort WITH MANUAL RESOLUTION')
        
        # Abort the merge
        db.execute('ABORT MERGE')
        
        # Verify no pending conflicts after abort
        conflicts = db.query('SHOW MERGE CONFLICTS')
        assert len(conflicts) == 0

    def test_remote_management(self, db):
        """Test remote management SQL commands"""
        # Initially no remotes
        remotes = db.query('SHOW REMOTES')
        assert len(remotes) == 0
        
        # Add a remote
        result = db.query("CREATE REMOTE origin 'https://github.com/test/repo.git'")
        assert len(result) == 1
        
        # Show remotes
        remotes = db.query('SHOW REMOTES')
        assert len(remotes) == 1
        assert remotes[0]['Name'] == 'origin'
        
        # Add another remote
        db.execute("CREATE REMOTE upstream 'https://github.com/upstream/repo.git'")
        
        # Show remotes - should have 2
        remotes = db.query('SHOW REMOTES')
        assert len(remotes) == 2
        
        # Drop a remote
        result = db.query('DROP REMOTE upstream')
        assert len(result) == 1
        
        # Show remotes - should have 1
        remotes = db.query('SHOW REMOTES')
        assert len(remotes) == 1
        assert remotes[0]['Name'] == 'origin'

    def test_push_pull_syntax(self, db):
        """Test PUSH/PULL/FETCH SQL syntax parsing (operations will fail but should parse)"""
        # Add a remote first
        db.execute("CREATE REMOTE origin 'https://github.com/test/repo.git'")
        
        # Test various PUSH syntax variations - they should parse but fail at execution
        # since the remote doesn't actually exist
        try:
            db.execute('PUSH')
        except Exception as e:
            # Expected to fail with connection error, not parse error
            assert 'unknown statement type' not in str(e).lower()
        
        try:
            db.execute('PUSH TO origin')
        except Exception as e:
            assert 'unknown statement type' not in str(e).lower()
        
        try:
            db.execute('PUSH TO origin BRANCH master')
        except Exception as e:
            assert 'unknown statement type' not in str(e).lower()
        
        # Test PULL syntax
        try:
            db.execute('PULL')
        except Exception as e:
            assert 'unknown statement type' not in str(e).lower()
        
        try:
            db.execute('PULL FROM origin')
        except Exception as e:
            assert 'unknown statement type' not in str(e).lower()
        
        # Test FETCH syntax
        try:
            db.execute('FETCH')
        except Exception as e:
            assert 'unknown statement type' not in str(e).lower()
        
        try:
            db.execute('FETCH FROM origin')
        except Exception as e:
            assert 'unknown statement type' not in str(e).lower()


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

