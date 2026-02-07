# CommitDB

[![Go Reference](https://pkg.go.dev/badge/github.com/nickyhof/CommitDB/v2.svg)](https://pkg.go.dev/github.com/nickyhof/CommitDB/v2)
[![Go Report Card](https://goreportcard.com/badge/github.com/nickyhof/CommitDB/v2)](https://goreportcard.com/report/github.com/nickyhof/CommitDB/v2)

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
- 🐍 **Python support** - Native driver for Python applications
- 🦀 **Rust support** - Native driver for Rust applications

## Quick Start

```bash
# Docker
docker run -p 3306:3306 ghcr.io/nickyhof/commitdb:latest

# Go
go install github.com/nickyhof/CommitDB/v2/cmd/cli@latest

# Python
pip install commitdb

# Rust (Cargo.toml)
commitdb = "2"
```

**Python example:**

```python
from commitdb import CommitDB

with CommitDB('localhost', 3306) as db:
    db.execute("CREATE DATABASE myapp")
    db.execute("CREATE TABLE myapp.users (id INT, name STRING)")
    db.execute("INSERT INTO myapp.users VALUES (1, 'Alice')")
    result = db.query("SELECT * FROM myapp.users")
    for row in result:
        print(row)
```

**Rust example:**

```rust
use commitdb::CommitDB;

let mut db = CommitDB::connect("localhost", 3306)?;
db.execute("CREATE DATABASE myapp")?;
db.execute("CREATE TABLE myapp.users (id INT, name STRING)")?;
db.execute("INSERT INTO myapp.users VALUES (1, 'Alice')")?;
let result = db.query("SELECT * FROM myapp.users")?;
for row in &result {
    println!("{:?}", row);
}
```

## Documentation

- [Installation](https://nickyhof.github.io/CommitDB/installation/)
- [SQL Reference](https://nickyhof.github.io/CommitDB/sql-reference/)
- [Branching & Merging](https://nickyhof.github.io/CommitDB/branching/)
- [Shared Databases](https://nickyhof.github.io/CommitDB/shared-databases/)
- [Python Client](https://nickyhof.github.io/CommitDB/python-client/)
- [Rust Client](https://nickyhof.github.io/CommitDB/rust-client/)
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

## License

Apache 2.0