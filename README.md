# CommitDB

[![Go Reference](https://pkg.go.dev/badge/github.com/nickyhof/CommitDB/v2.svg)](https://pkg.go.dev/github.com/nickyhof/CommitDB/v2)
[![Go Report Card](https://goreportcard.com/badge/github.com/nickyhof/CommitDB)](https://goreportcard.com/report/github.com/nickyhof/CommitDB)

A Git-backed SQL database engine. Every transaction is a Git commit.

**[📚 Full Documentation](https://nickyhof.github.io/CommitDB)**

> ⚠️ **Experimental Project** - This is a hobby project and should not be used in any production environment.

## Why CommitDB?

Traditional databases lose history. Once you UPDATE or DELETE, the old data is gone. CommitDB stores every change as a Git commit, giving you:

- **Complete audit trail** - Know exactly who changed what and when
- **Instant rollback** - Made a mistake? Restore any table to any point in time
- **Safe experimentation** - Create a branch, try risky changes, merge if it works
- **Built-in backup** - Push your entire database to GitHub/GitLab as a remote
- **No migration headaches** - Branch your schema, test changes, merge when ready

## Features

- 🔄 **Version history** - Every change tracked, nothing lost
- 🌿 **Git branching** - Experiment in branches, merge when ready
- ⏪ **Time travel** - Restore any table to any previous state
- 🔗 **Remote sync** - Push/pull to GitHub, GitLab, or any Git remote
- 📡 **Shared databases** - Query and JOIN across external repositories

## Quick Start

### Go Library

```go
import (
    commitdb "github.com/nickyhof/CommitDB/v2"
    "github.com/nickyhof/CommitDB/v2/core"
    "github.com/nickyhof/CommitDB/v2/persistence"
)

// In-memory
p, _ := persistence.NewMemoryPersistence()
instance := commitdb.Open(&p)

// Or file-backed (a real Git repo)
p, _ := persistence.NewFilePersistence("./mydata", nil)
instance := commitdb.Open(&p)

e := instance.Engine(core.Identity{Name: "Alice", Email: "alice@example.com"})

e.Execute("CREATE DATABASE myapp")
e.Execute("CREATE TABLE myapp.users (id INT, name STRING)")
e.Execute("INSERT INTO myapp.users VALUES (1, 'Alice')")
result, _ := e.Execute("SELECT * FROM myapp.users")
result.Display()
```

### CLI

```bash
# Install
go install github.com/nickyhof/CommitDB/v2/cmd/commitdb@latest

# In-memory mode
commitdb

# File-backed mode (creates a Git repo)
commitdb -dir ./mydata

# Execute SQL directly
commitdb -e "CREATE DATABASE myapp"

# Execute a SQL file
commitdb -dir ./mydata -f setup.sql

# Pipe SQL via stdin
echo "SHOW DATABASES;" | commitdb -dir ./mydata
```

## Documentation

- [Installation](https://nickyhof.github.io/CommitDB/installation/)
- [SQL Reference](https://nickyhof.github.io/CommitDB/sql-reference/)
- [Branching & Merging](https://nickyhof.github.io/CommitDB/branching/)
- [Shared Databases](https://nickyhof.github.io/CommitDB/shared-databases/)
- [Go API](https://nickyhof.github.io/CommitDB/go-api/)
- [Benchmarks](https://nickyhof.github.io/CommitDB/benchmarks/)

## Performance

CommitDB vs [DuckDB](https://duckdb.org/) (1,000 rows, Apple M1 Pro):

| Operation | CommitDB | DuckDB | Ratio |
|-----------|----------|--------|-------|
| INSERT | 2.66 ms | 0.19 ms | 14x |
| SELECT * | 1.39 ms | 0.61 ms | 2.3x |
| WHERE | 1.42 ms | 0.43 ms | 3.3x |
| ORDER BY | 2.07 ms | 0.74 ms | 2.8x |
| COUNT(*) | 1.32 ms | 0.11 ms | 11.6x |
| SUM | 1.36 ms | 0.13 ms | 10.3x |
| AVG | 1.35 ms | 0.13 ms | 10.5x |
| GROUP BY | 1.37 ms | 0.43 ms | 3.2x |
| LIMIT | 1.32 ms | 0.13 ms | 10.5x |
| Complex | 1.44 ms | 0.53 ms | 2.7x |

> **Why is DuckDB faster?** DuckDB is an OLAP-optimized columnar database built for analytics. CommitDB uses a row-based Git object model that trades raw query speed for:
>
> - **Git-native storage** - Every row is a Git blob, enabling branching, merging, and time travel
> - **Full audit trail** - Query any table at any point in history
> - **Standard Git tooling** - Push/pull to GitHub, diff changes, bisect bugs

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## License

Apache 2.0