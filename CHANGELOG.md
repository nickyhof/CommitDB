# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.5.0] - 2026-01-29

### Added

#### Views and Materialized Views
- `CREATE VIEW database.name AS SELECT ...` for virtual views
- `CREATE MATERIALIZED VIEW database.name AS SELECT ...` for cached query results
- `REFRESH VIEW database.name` to update cached materialized view data
- `SHOW VIEWS IN database` to list all views
- `DROP VIEW [IF EXISTS]` works for both regular and materialized views
- Views are resolved automatically in SELECT queries

#### Time-Travel Queries
- `SELECT ... FROM table AS OF 'transaction_id'` to query historical data
- Supports full and abbreviated transaction IDs (Git commit hashes)
- Works with WHERE, ORDER BY, LIMIT, and all standard SELECT clauses
- Works on both tables and views (including materialized views)

### Changed

#### Persistence Layer Optimization
- Empty commits are now avoided when there are no actual data changes
- Tree hash comparison prevents redundant Git commits
- Duplicate writes no longer create unnecessary commit history

## [2.4.0] - 2026-01-28

### Added

#### Index Population on CREATE INDEX
- Indexes now automatically populate with existing table data on creation
- `CREATE UNIQUE INDEX` validates uniqueness during index build
- Failed unique constraint returns clear error message with duplicate value

#### Index-Accelerated Queries
- `SELECT` with equality `WHERE` conditions now use indexes when available
- Significant performance improvement for filtered queries on indexed columns

#### CLI Enhancements
- `-name` flag for custom Git commit author name
- `-email` flag for custom Git commit author email
- `-sqlFile` flag for executing SQL files (non-interactive mode)

#### Documentation
- New [CLI & Server Reference](docs/cli-server.md) with comprehensive usage guide
- Decision table for when to use CLI vs Server
- All CLI and Server options documented with examples

### Changed

#### Renamed Python Driver to Python Client
- `drivers/` directory renamed to `clients/`
- All documentation updated to use "Python Client" terminology

### Fixed

#### File Persistence Fixes
- Fixed `SHOW DATABASES` not listing databases after creation in file mode
- Fixed symref HEAD resolution for initial repository state
- Improved database discovery from Git tree structure

#### COPY INTO Performance
- Changed from per-row commits to batched atomic commits
- Fast import of large CSV files (3000 rows in 1.1s)

#### SHOW INDEXES Parsing
- `SHOW INDEXES ON table` now works (previously only `SHOW INDEX` worked)

## [2.3.0] - 2026-01-28

### Added

#### IF EXISTS Clause
- `DROP TABLE IF EXISTS db.table` - Silently succeeds if table doesn't exist
- `DROP DATABASE IF EXISTS db` - Silently succeeds if database doesn't exist

### Changed

#### Ibis Backend Improvements
- Simplified `execute()` method - removed redundant type conversion logic
- Simplified `insert()` method - now only accepts pandas DataFrames
- Added `drop_table()` method with `force` parameter for IF EXISTS support
- Removed unused internal code and imports

## [2.2.0] - 2026-01-28

### Added

#### Shared Databases
- Query external Git repositories without copying data
- `CREATE SHARE name FROM 'url'` - Create a read-only reference to external repository
- `SYNC SHARE name` - Pull latest changes from remote
- `DROP SHARE name` - Remove a share
- `SHOW SHARES` - List all configured shares
- 3-level naming for shared tables: `share.database.table`
- Cross-repository JOINs: `SELECT * FROM local.orders JOIN external.users u ON ...`
- Authentication support: SSH keys, tokens
- Share metadata persisted in Git (`.commitdb/shares.json`)
- Cloned repositories stored in `.shares/` directory

#### Ibis Backend for Pandas Integration
- Official Ibis backend enabling pandas DataFrame support
- Connect via `ibis.commitdb.connect(host, port, database=...)`
- URL-based connection: `ibis.connect('commitdb://localhost:3306/mydb')`
- Lazy expression evaluation with Ibis query DSL
- Full pandas DataFrame output from `execute()` calls
- Schema introspection and table listing

#### Python Driver Improvements
- Dynamic version reading from package metadata using `importlib.metadata`
- Single source of truth for version in `pyproject.toml`

### Changed

#### Execution Time Standardization
- Renamed `ExecutionTimeSec` to `ExecutionTimeMs` across all result types
- Added `ExecutionOps` field to CGO bindings and server protocol
- Renamed JSON fields from `time_ms` to `execution_time_ms`
- Added `execution_ops` JSON field
- Python driver updated to use `execution_time_ms` and `execution_ops`

#### Benchmarks
- Added DuckDB comparative benchmarks for performance baseline

## [2.1.0] - 2026-01-25

### Added

#### S3 and HTTPS Support for COPY INTO
- Import CSV files from HTTPS URLs: `COPY INTO table FROM 'https://...'`
- Import/export CSV files from/to S3: `COPY INTO table FROM 's3://bucket/file.csv'`
- S3 authentication via AWS environment variables or explicit credentials:
  ```sql
  COPY INTO mydb.users FROM 's3://bucket/file.csv' WITH (
      AWS_KEY = '...', AWS_SECRET = '...', AWS_REGION = 'us-east-1'
  )
  ```

## [2.0.0] - 2026-01-25

### Added

#### Git Plumbing API (Major Performance Improvement)
- Low-level Git object manipulation bypassing worktree operations (`ps/plumbing.go`)
- `createBlob()` - Direct blob creation in object store
- `updateTreePath()` - Direct tree building without filesystem I/O
- `batchUpdateTree()` - Efficient multi-record updates in single tree operation
- `createCommitDirect()` - Creates commits without worktree index manipulation
- `ReadFileDirect()` / `WriteFileDirect()` - Direct Git tree reads/writes
- `ListEntriesDirect()` - Directory listing from Git tree
- Memory mode optimization: skips worktree sync entirely
- **Result: ~10x faster writes for INSERT/UPDATE operations**

#### Performance Testing Suite
- Comprehensive performance test framework (`tests/performance_test.go`)
- Configurable test parameters via environment variables
- Metrics collection: p50/p95/p99 latencies, throughput, error rates
- Test scenarios:
  - `TestPerfConcurrentReads` - Concurrent SELECT performance
  - `TestPerfConcurrentWrites` - Concurrent INSERT/UPDATE performance
  - `TestPerfMixedWorkload` - Realistic 70/30 read/write mix
  - `TestPerfConnectionChurn` - Rapid connect/disconnect cycles
  - `TestPerfTLSOverhead` - TLS vs non-TLS latency comparison
  - `TestPerfSustainedLoad` - Long-running soak test with memory monitoring

#### Benchmark Dashboard
- Live benchmark dashboard on GitHub Pages
- Version comparison across releases and commits
- `scripts/benchmark.sh` - Benchmark runner with JSON output
- `scripts/compare_benchmarks.sh` - Baseline comparison with regression warnings
- `scripts/benchmark_dashboard.html` - Interactive web dashboard

#### Makefile Improvements
- `make bench-json` - Run benchmarks with JSON output
- `make test-race` - Run tests with race detector
- `make test-cover` - Run tests with coverage report
- `make run-server` / `make run-cli` - Run server/CLI
- `make fmt` / `make vet` / `make lint` - Code quality tools
- `make deps` - Download and tidy dependencies
- `make help` - Show all available commands

### Changed
- CRUD operations now use Git Plumbing API for ~10x faster writes

## [1.7.0] - 2026-01-24

### Added

#### COPY INTO Command (Bulk CSV Import/Export)
- `COPY INTO 'file.csv' FROM database.table` - Export table to CSV file
- `COPY INTO database.table FROM 'file.csv'` - Import CSV file into table
- `WITH (HEADER = TRUE, DELIMITER = ',')` - Options for header and delimiter
- Streaming CSV parsing for memory-efficient large file imports
- Atomic transactions using staging table approach
- Zero-memory copy via `CopyRecords` persistence method

#### Bulk INSERT
- Multi-row INSERT with VALUES list: `INSERT INTO ... VALUES (...), (...), (...)`

#### JSON Data Type
- `JSON` column type with validation on insert
- `JSON_EXTRACT(column, '$.path')` - Extract values using JSONPath
- `JSON_SET(column, '$.path', value)` - Set values in JSON
- `JSON_REMOVE(column, '$.path')` - Remove keys from JSON
- `JSON_CONTAINS(column, value)` - Check if value exists
- `JSON_KEYS(column)`, `JSON_LENGTH(column)`, `JSON_TYPE(column)`

#### Date Functions
- `NOW()` - Current timestamp
- `YEAR()`, `MONTH()`, `DAY()`, `HOUR()` - Extract date parts
- `DATE_ADD()`, `DATE_SUB()` - Date arithmetic
- `DATEDIFF()` - Days between dates
- `DATE()`, `DATE_FORMAT()` - Date formatting

#### String Functions
- `UPPER()`, `LOWER()` - Case conversion
- `CONCAT()` - String concatenation
- `SUBSTRING()`, `TRIM()`, `LENGTH()`, `REPLACE()`

#### SQL Enhancements
- `IN` operator: `WHERE column IN (1, 2, 3)`
- `ALTER TABLE ... RENAME COLUMN old TO new`
- Mixed column + aggregate SELECT: `SELECT city, COUNT(*) FROM ... GROUP BY city`

#### Benchmarks
- `BenchmarkBulkInsert` - Multi-value INSERT performance
- `BenchmarkCopyIntoExport` - CSV export performance
- `BenchmarkCopyIntoImport` - CSV import performance
- `BenchmarkGroupBy` - GROUP BY with aggregates
- `BenchmarkJoin` - JOIN operation performance
- `BenchmarkStringFunctions` - String function performance

#### Documentation
- COPY INTO usage in README
- Bulk I/O section in Features table

### Changed
- Parser now supports mixed column + aggregate in SELECT list

## [1.6.0] - 2026-01-24

### Added

#### Server Authentication (JWT)
- Optional JWT-based authentication for TCP connections
- Per-connection identity tracking for Git commit authorship
- JWT validation with HS256 shared secret
- CLI flags: `--jwt-secret`, `--jwt-issuer`, `--jwt-audience`, `--jwt-name-claim`, `--jwt-email-claim`
- `AUTH JWT <token>` command for client authentication
- Authenticated identity flows through to Git commits

#### TLS/SSL Encryption
- Optional TLS encryption for secure connections
- CLI flags: `--tls-cert`, `--tls-key`
- Minimum TLS 1.2 support
- Combined TLS + JWT authentication support

#### Docker Support
- Multi-stage Dockerfile with Alpine base
- Multi-architecture builds (linux/amd64, linux/arm64)
- Published to GitHub Container Registry (`ghcr.io/nickyhof/commitdb`)
- Volume mount support for persistent storage

#### Python Driver Updates
- `jwt_token` parameter for auto-authentication on connect
- `authenticate_jwt(token)` method for explicit authentication
- `authenticated` and `identity` properties
- `use_ssl`, `ssl_verify`, `ssl_ca_cert` parameters for TLS
- Unit tests for auth and SSL features
- Integration tests for auth-enabled server scenarios

#### Documentation
- JWT authentication architecture diagram
- TLS/SSL server and client usage examples
- Docker usage examples in README

### Changed
- Server now supports per-connection engines with separate identities
- CI pipeline generates TLS certificates for SSL integration tests
- Release workflow includes Docker image build and push

## [1.5.0] - 2026-01-23

### Added

#### Remote Repository Operations
- Push/pull/fetch branches to/from remote Git repositories (`ps/remote.go`)
- Remote management: Add, list, and remove remotes
- Multiple authentication methods: Token, SSH key (with optional passphrase), Basic auth
- `AddRemote(name, url)` - Add a remote repository
- `ListRemotes()` - List all configured remotes
- `RemoveRemote(name)` - Remove a remote
- `Push(remote, branch, auth)` - Push to remote (defaults to origin)
- `Pull(remote, branch, auth)` - Pull from remote with merge
- `Fetch(remote, auth)` - Fetch without merging

#### Remote SQL Syntax
- `CREATE REMOTE name 'url'` - Add a remote
- `SHOW REMOTES` - List configured remotes
- `DROP REMOTE name` - Remove a remote
- `PUSH [TO remote] [BRANCH name]` - Push to remote
- `PULL [FROM remote] [BRANCH name]` - Pull from remote
- `FETCH [FROM remote]` - Fetch without merging

#### Remote Authentication Syntax
- `WITH TOKEN 'token'` - Token-based auth (GitHub, GitLab, etc.)
- `WITH SSH KEY 'path' [PASSPHRASE 'pass']` - SSH key authentication
- `WITH USER 'username' PASSWORD 'password'` - Basic authentication

#### Documentation
- Remote operations section in README
- Python driver remote commands reference
- Go API examples for remote operations

## [1.4.0] - 2026-01-23

### Added

#### Row-Level Merge Strategy
- Three-way merge algorithm for handling diverged branches (`ps/merge.go`)
- Automatic conflict resolution using Last-Writer-Wins (later commit wins)
- Merge at record level by primary key, not file-level
- `MergeWithOptions(source, identity, opts)` - Merge with configurable strategy
- `MergeOptions` struct with `Strategy` field

#### Manual Conflict Resolution
- `MergeStrategyManual` - Pauses merge when conflicts detected
- `GetPendingMerge()` - Get pending merge state
- `ResolveConflict(db, table, key, resolution)` - Resolve individual conflicts
- `CompleteMerge(identity)` - Finish merge after resolving all conflicts
- `AbortMerge()` - Cancel pending merge

#### SQL Syntax for Manual Resolution
- `MERGE branch WITH MANUAL RESOLUTION` - Start merge with manual conflict handling
- `SHOW MERGE CONFLICTS` - View pending conflicts (database, table, key, HEAD value, SOURCE value)
- `RESOLVE CONFLICT db.table.key USING HEAD` - Keep current branch value
- `RESOLVE CONFLICT db.table.key USING SOURCE` - Keep source branch value
- `RESOLVE CONFLICT db.table.key USING 'value'` - Use custom value
- `COMMIT MERGE` - Complete pending merge
- `ABORT MERGE` - Cancel pending merge

#### Documentation
- Updated README with merge strategies table
- Go API examples for manual conflict resolution
- Python driver examples for conflict resolution workflow
- Architecture diagram and package summary table
- Package documentation for `op/` layer (`op/doc.go`)

#### Tests
- Unit tests: `TestMergeRowLevel`, `TestMergeRowLevelWithConflict`, `TestMergeRecordMaps`
- Manual merge tests: `TestMergeManualMode`, `TestMergeManualResolveAndComplete`, `TestMergeAbort`
- SQL integration tests: `TestMergeManualResolutionSQL`, `TestAbortMergeSQL`
- Python tests: `test_merge_manual_resolution`, `test_abort_merge`

### Changed
- `Merge()` now uses row-level strategy by default instead of fast-forward only
- Diverged branches no longer error; conflicts are auto-resolved with Last-Writer-Wins

## [1.3.0] - 2026-01-23

### Added

#### Git Branching & Merging
- Branch operations in persistence layer (`ps/branch.go`)
  - `Branch(name, from)` - Create branch at HEAD or specific transaction
  - `Checkout(name)` - Switch to a branch
  - `Merge(source, identity)` - Fast-forward merge
  - `ListBranches()` - List all branches
  - `CurrentBranch()` - Get current branch name
  - `DeleteBranch(name)` - Delete a branch
  - `RenameBranch(old, new)` - Rename a branch

#### SQL Syntax for Branching
- `CREATE BRANCH name` - Create branch at current HEAD
- `CREATE BRANCH name FROM 'txn_id'` - Create branch from specific transaction
- `CHECKOUT name` - Switch to a branch
- `MERGE name` - Merge branch into current
- `SHOW BRANCHES` - List all branches

#### Tests
- Persistence layer branch tests (`ps/branch_test.go`)
- Go integration tests for SQL branching (`tests/integration_test.go`)
- Python embedded mode branching tests

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
- Python client library at `clients/python/`
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

[2.5.0]: https://github.com/nickyhof/CommitDB/releases/tag/v2.5.0
[2.4.0]: https://github.com/nickyhof/CommitDB/releases/tag/v2.4.0
[2.3.0]: https://github.com/nickyhof/CommitDB/releases/tag/v2.3.0
[2.2.0]: https://github.com/nickyhof/CommitDB/releases/tag/v2.2.0
[2.1.0]: https://github.com/nickyhof/CommitDB/releases/tag/v2.1.0
[2.0.0]: https://github.com/nickyhof/CommitDB/releases/tag/v2.0.0
[1.7.0]: https://github.com/nickyhof/CommitDB/releases/tag/v1.7.0
[1.6.0]: https://github.com/nickyhof/CommitDB/releases/tag/v1.6.0
[1.5.0]: https://github.com/nickyhof/CommitDB/releases/tag/v1.5.0
[1.4.0]: https://github.com/nickyhof/CommitDB/releases/tag/v1.4.0
[1.3.0]: https://github.com/nickyhof/CommitDB/releases/tag/v1.3.0
[1.2.0]: https://github.com/nickyhof/CommitDB/releases/tag/v1.2.0
[1.1.2]: https://github.com/nickyhof/CommitDB/releases/tag/v1.1.2
[1.1.1]: https://github.com/nickyhof/CommitDB/releases/tag/v1.1.1
[1.1.0]: https://github.com/nickyhof/CommitDB/releases/tag/v1.1.0
[1.0.0]: https://github.com/nickyhof/CommitDB/releases/tag/v1.0.0