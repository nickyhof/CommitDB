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
# Uncomment and run with: pytest drivers/python/tests/ -v

# class TestCommitDBIntegration:
#     """Integration tests (requires running server)."""
#
#     @pytest.fixture
#     def db(self):
#         db = CommitDB('localhost', 3306)
#         db.connect()
#         yield db
#         db.close()
#
#     def test_create_database(self, db):
#         result = db.execute('CREATE DATABASE pytest_test')
#         assert isinstance(result, CommitResult)
#         assert result.databases_created == 1
#
#     def test_create_table(self, db):
#         db.execute('CREATE DATABASE pytest_test2')
#         result = db.execute('CREATE TABLE pytest_test2.users (id INT PRIMARY KEY, name STRING)')
#         assert isinstance(result, CommitResult)
#         assert result.tables_created == 1
#
#     def test_insert_and_query(self, db):
#         db.execute('CREATE DATABASE pytest_test3')
#         db.execute('CREATE TABLE pytest_test3.items (id INT PRIMARY KEY, value STRING)')
#         db.execute("INSERT INTO pytest_test3.items (id, value) VALUES (1, 'hello')")
#
#         result = db.query('SELECT * FROM pytest_test3.items')
#         assert isinstance(result, QueryResult)
#         assert len(result) == 1
#         assert result[0] == {'id': '1', 'value': 'hello'}
