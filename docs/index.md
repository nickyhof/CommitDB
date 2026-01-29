# CommitDB

A Git-backed SQL database engine written in Go. Every transaction is a Git commit, providing built-in version control, complete history, branching, and the ability to restore to any point in time.

- 🔄 **Version Control** – Every change is tracked, nothing is lost
- 🌿 **Git Branching** – Experiment in branches, merge when ready  
- ⏪ **Time Travel** – Restore any table to any previous state
- 🔗 **Remote Sync** – Push/pull to GitHub, GitLab, or any Git remote
- 🐍 **Python Support** – Native driver for Python applications

## Quick Start

=== "CLI"

    ```bash
    # Install
    go install github.com/nickyhof/CommitDB/cmd/cli@latest
    
    # Run with in-memory storage
    commitdb-cli
    
    # Run with file persistence
    commitdb-cli -baseDir=/path/to/data
    ```

=== "Docker"

    ```bash
    docker run -p 3306:3306 ghcr.io/nickyhof/commitdb:latest
    ```

=== "Go Library"

    ```go
    import "github.com/nickyhof/CommitDB/op"
    
    engine := op.NewEngine(op.FileMode, "/path/to/data")
    engine.Query("CREATE DATABASE myapp")
    engine.Query("CREATE TABLE myapp.users (id INT, name STRING)")
    engine.Query("INSERT INTO myapp.users (id, name) VALUES (1, 'Alice')")
    result, _ := engine.Query("SELECT * FROM myapp.users")
    ```

=== "Python"

    ```python
    from commitdb import CommitDB
    
    db = CommitDB('localhost', 3306)
    db.connect()
    db.execute("SELECT * FROM myapp.users")
    ```

## Features

| Category | Features |
|----------|----------|
| **SQL** | SELECT, INSERT, UPDATE, DELETE, CREATE/DROP TABLE/DATABASE/INDEX |
| **Queries** | WHERE, ORDER BY, LIMIT, OFFSET, DISTINCT, GROUP BY, HAVING |
| **Aggregates** | SUM, AVG, MIN, MAX, COUNT |
| **JOINs** | INNER, LEFT, RIGHT |
| **Bulk I/O** | COPY INTO for CSV import/export |
| **Version Control** | Branching, merging, snapshots, time-travel restore |
| **Remote** | Push, pull, fetch with token/SSH/basic auth |

## Documentation

- [Quick Start](quickstart.md) – Get running in 5 minutes
- [CLI & Server Reference](cli-server.md) – Command-line options and usage
- [SQL Reference](sql-reference.md) – Full SQL syntax
- [Branching & Merging](branching.md) – Version control features
- [Shared Databases](shared-databases.md) – Cross-database queries
- [Python Driver](python-driver.md) – Python client library
- [Go API](go-api.md) – Embedding in Go applications
