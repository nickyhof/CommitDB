# CommitDB

A Git-backed SQL database engine written in Go. Every transaction is a Git commit, providing built-in version control, complete history, branching, and the ability to restore to any point in time.

<div class="grid cards" markdown>

- :material-source-branch: **Version Control** – Every change is tracked, nothing is lost
- :material-merge: **Git Branching** – Experiment in branches, merge when ready
- :material-history: **Time Travel** – Restore any table to any previous state
- :material-cloud-sync: **Remote Sync** – Push/pull to GitHub, GitLab, or any Git remote
- :fontawesome-brands-python: **Python Support** – Native driver for Python applications

</div>

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
