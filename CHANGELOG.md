# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2026-01-23

### Added

#### Go Bindings for Python (Embedded Mode)
- CGO shared library (`libcommitdb`) for embedded database mode
- `CommitDBLocal` class for in-process database without server
- ctypes bindings in `commitdb/binding.py`
- Makefile with `make lib` target for building shared libraries
- Bundled shared libraries in PyPI package

#### Documentation
- Embedded mode documentation in main README and Python driver README
- Full CRUD examples for `CommitDBLocal`

## [1.1.2] - 2026-01-23

### Added

#### Python Driver
- Python client library at `drivers/python/`
- `CommitDB` class with connection handling and query execution
- `QueryResult` and `CommitResult` dataclasses for typed results
- Context manager support for automatic connection cleanup
- Convenience methods: `create_database`, `create_table`, `insert`, `show_databases`, `show_tables`
- Unit tests and documentation

## [1.1.1] - 2026-01-23

### Added

#### Snapshots & Restore Documentation
- Added "Snapshots & Restore" section to README
- Comprehensive test suite for `Snapshot`, `Recover`, and `Restore` functions

#### Release Pipeline
- Release binaries for both CLI (`commitdb-cli-*`) and Server (`commitdb-server-*`)
- Cross-platform builds: Linux, macOS, Windows (amd64/arm64)

### Fixed
- Fixed nil pointer dereference in `Recover` when snapshot doesn't exist
- Added proper error handling to `Restore` function

## [1.1.0] - 2026-01-23

### Added

#### SQL Server
- TCP-based SQL server for remote database access
- Line-based protocol: send SQL queries, receive JSON responses
- Persistent connections supporting multiple queries
- Server CLI with `-port`, `-baseDir`, and `-gitUrl` flags
- Comprehensive test suite for server functionality

## [1.0.0] - 2026-01-23

### Added

#### SQL Operations
- `SELECT` with `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`, `DISTINCT` clauses
- Comparison operators: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`
- Logical operators: `AND`, `OR`, `NOT`
- `IS NULL` / `IS NOT NULL` null handling
- `LIKE` pattern matching
- `INSERT`, `UPDATE`, `DELETE` with `WHERE` clauses
- `CREATE DATABASE`, `DROP DATABASE`
- `CREATE TABLE`, `DROP TABLE`
- `DESCRIBE`, `SHOW DATABASES`, `SHOW TABLES`, `SHOW INDEXES` introspection commands

#### Aggregate Functions
- `COUNT(*)` and `COUNT(column)`
- `SUM(column)`, `AVG(column)`
- `MIN(column)`, `MAX(column)`
- `GROUP BY` clause support

#### JOIN Support
- `INNER JOIN`
- `LEFT JOIN`
- `RIGHT JOIN`

#### Data Types
- `INT` / `INTEGER`
- `STRING`
- `FLOAT` / `DOUBLE` / `REAL`
- `BOOL` / `BOOLEAN`
- `TEXT`
- `TIMESTAMP` / `DATETIME`

#### Indexing
- `CREATE INDEX` for faster queries
- Index management and introspection

#### CLI Features
- Interactive REPL with command history
- SQL file import via `-f` flag or `.import` command
- Database context switching with `.use` command
- Compact result display with execution stats
- Human-readable time formatting

#### Persistence
- Memory storage mode for fast in-memory operations
- File persistence mode with Git-backed storage
- Every transaction stored as a Git commit
- Full version history with restore capability

#### Testing
- Comprehensive integration test suite
- Tests run with both memory and file persistence modes
- Persistence reopen tests for data durability verification

[1.2.0]: https://github.com/nickyhof/CommitDB/releases/tag/v1.2.0
[1.1.2]: https://github.com/nickyhof/CommitDB/releases/tag/v1.1.2
[1.1.1]: https://github.com/nickyhof/CommitDB/releases/tag/v1.1.1
[1.1.0]: https://github.com/nickyhof/CommitDB/releases/tag/v1.1.0
[1.0.0]: https://github.com/nickyhof/CommitDB/releases/tag/v1.0.0
