# CommitDB

[![Go Reference](https://pkg.go.dev/badge/github.com/nickyhof/CommitDB.svg)](https://pkg.go.dev/github.com/nickyhof/CommitDB)
[![Go Report Card](https://goreportcard.com/badge/github.com/nickyhof/CommitDB)](https://goreportcard.com/report/github.com/nickyhof/CommitDB)

A Git-backed SQL database engine. Every transaction is a Git commit.

**[📚 Full Documentation](https://nickyhof.github.io/CommitDB)**

## Why CommitDB?

Traditional databases lose history. Once you UPDATE or DELETE, the old data is gone. CommitDB stores every change as a Git commit, giving you:

- **Complete audit trail** - Know exactly who changed what and when
- **Instant rollback** - Made a mistake? Restore any table to any point in time
- **Safe experimentation** - Create a branch, try risky changes, merge if it works
- **Built-in backup** - Push your entire database to GitHub/GitLab as a remote
- **No migration headaches** - Branch your schema, test changes, merge when ready

> ⚠️ **Experimental Project** - This is a hobby project and should not be used in any production environment.

## Features

- 🔄 **Version history** - Every change tracked, nothing lost
- 🌿 **Git branching** - Experiment in branches, merge when ready
- ⏪ **Time travel** - Restore any table to any previous state
- 🔗 **Remote sync** - Push/pull to GitHub, GitLab, or any Git remote
- 📡 **Shared databases** - Query and JOIN across external repositories
- 🐍 **Python support** - Native driver for Python applications

## Quick Start

```bash
# Docker
docker run -p 3306:3306 ghcr.io/nickyhof/commitdb:latest

# Go
go install github.com/nickyhof/CommitDB/cmd/cli@latest

# Python
pip install commitdb
```

```python
from commitdb import CommitDB

db = CommitDB('localhost', 3306)
db.connect()
db.execute("CREATE DATABASE myapp")
db.execute("CREATE TABLE myapp.users (id INT, name STRING)")
db.execute("INSERT INTO myapp.users VALUES (1, 'Alice')")
result = db.execute("SELECT * FROM myapp.users")
print(result.rows)
```

## Performance

CommitDB vs [DuckDB](https://duckdb.org/) (1000 rows, Apple M1 Pro):

| Operation | CommitDB | DuckDB | Winner |
|-----------|----------|--------|--------|
| INSERT | 52 µs | 227 µs | **CommitDB 4x** |
| SELECT * | 161 ms | 0.6 ms | DuckDB |
| WHERE | 164 ms | 0.5 ms | DuckDB |
| COUNT(*) | 163 ms | 0.1 ms | DuckDB |
| GROUP BY | 163 ms | 0.5 ms | DuckDB |

**Why is DuckDB faster on reads?** DuckDB is an OLAP-optimized columnar database built for analytics. CommitDB uses a row-based Git object model that trades raw query speed for:

- **Git-native storage** - Every row is a Git blob, enabling branching, merging, and time travel
- **Full audit trail** - Query any table at any point in history
- **Standard Git tooling** - Push/pull to GitHub, diff changes, bisect bugs

If you need sub-millisecond analytics, use DuckDB. If you need version control for your data, use CommitDB.

## Documentation

- [Installation](https://nickyhof.github.io/CommitDB/installation/)
- [SQL Reference](https://nickyhof.github.io/CommitDB/sql-reference/)
- [Branching & Merging](https://nickyhof.github.io/CommitDB/branching/)
- [Shared Databases](https://nickyhof.github.io/CommitDB/shared-databases/)
- [Python Client](https://nickyhof.github.io/CommitDB/python-client/)
- [Go API](https://nickyhof.github.io/CommitDB/go-api/)
- [Benchmarks](https://nickyhof.github.io/CommitDB/benchmarks/)

## License

Apache 2.0