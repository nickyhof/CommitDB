# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[1.6.0]: https://github.com/nickyhof/CommitDB/releases/tag/v1.6.0
[1.5.0]: https://github.com/nickyhof/CommitDB/releases/tag/v1.5.0
[1.4.0]: https://github.com/nickyhof/CommitDB/releases/tag/v1.4.0
[1.3.0]: https://github.com/nickyhof/CommitDB/releases/tag/v1.3.0
[1.2.0]: https://github.com/nickyhof/CommitDB/releases/tag/v1.2.0
[1.1.2]: https://github.com/nickyhof/CommitDB/releases/tag/v1.1.2
[1.1.1]: https://github.com/nickyhof/CommitDB/releases/tag/v1.1.1
[1.1.0]: https://github.com/nickyhof/CommitDB/releases/tag/v1.1.0
[1.0.0]: https://github.com/nickyhof/CommitDB/releases/tag/v1.0.0