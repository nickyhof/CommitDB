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

    def test_not_connected_error(self):
        db = CommitDB('localhost', 3306)
        with pytest.raises(CommitDBError, match="Not connected"):
            db.execute("SELECT 1")


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




# Embedded mode tests (require libcommitdb shared library)
# Run with: make lib && pytest drivers/python/tests/ -v

import os
from pathlib import Path

# Try to find the shared library
def _find_lib():
    lib_paths = [
        Path(__file__).parent.parent.parent.parent.parent / 'lib' / 'libcommitdb.dylib',
        Path(__file__).parent.parent.parent.parent.parent / 'lib' / 'libcommitdb.so',
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

