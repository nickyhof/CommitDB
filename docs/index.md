# CommitDB

A Git-backed SQL database engine written in Go. Every transaction is a Git commit, providing built-in version control, complete history, branching, and the ability to restore to any point in time.

- 🔄 **Version Control** – Every change is tracked, nothing is lost
- 🌿 **Git Branching** – Experiment in branches, merge when ready  
- ⏪ **Time Travel** – Restore any table to any previous state
- 🔗 **Remote Sync** – Push/pull to GitHub, GitLab, or any Git remote
- 📡 **Shared Databases** – Query and JOIN across external repositories

## Quick Start

=== "CLI"

    ```bash
    # Install
    go install github.com/nickyhof/CommitDB/v2/cmd/cli@latest
    
    # Run with in-memory storage
    commitdb
    
    # Run with file persistence
    commitdb -dir=/path/to/data
    ```

=== "Go Library"

    ```go
    import (
        commitdb "github.com/nickyhof/CommitDB/v2"
        "github.com/nickyhof/CommitDB/v2/core"
        "github.com/nickyhof/CommitDB/v2/persistence"
    )
    
    p, _ := persistence.NewFilePersistence("./mydata", nil)
    instance := commitdb.Open(&p)
    e := instance.Engine(core.Identity{Name: "Alice", Email: "alice@example.com"})
    
    e.Execute("CREATE DATABASE myapp")
    e.Execute("CREATE TABLE myapp.users (id INT, name STRING)")
    e.Execute("INSERT INTO myapp.users VALUES (1, 'Alice')")
    result, _ := e.Execute("SELECT * FROM myapp.users")
    result.Display()
    ```

## Features

| Category | Features |
|----------|----------|
| **SQL** | SELECT, INSERT, UPDATE, DELETE, CREATE/DROP TABLE/DATABASE/INDEX, VIEWS |
| **Queries** | WHERE, ORDER BY, LIMIT, OFFSET, DISTINCT, GROUP BY, HAVING |
| **Aggregates** | SUM, AVG, MIN, MAX, COUNT |
| **JOINs** | INNER, LEFT, RIGHT |
| **Bulk I/O** | COPY INTO for CSV import/export |
| **Version Control** | Branching, merging, snapshots, time-travel restore |
| **Remote** | Push, pull, fetch with token/SSH/basic auth |

## Documentation

- [Quick Start](quickstart.md) – Get running in 5 minutes
- [SQL Reference](sql-reference.md) – Full SQL syntax
- [Branching & Merging](branching.md) – Version control features
- [Shared Databases](shared-databases.md) – Cross-database queries
- [Go API](go-api.md) – Embedding in Go applications
