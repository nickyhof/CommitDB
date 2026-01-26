# CommitDB

[![Go Reference](https://pkg.go.dev/badge/github.com/nickyhof/CommitDB.svg)](https://pkg.go.dev/github.com/nickyhof/CommitDB)
[![Go Report Card](https://goreportcard.com/badge/github.com/nickyhof/CommitDB)](https://goreportcard.com/report/github.com/nickyhof/CommitDB)

A Git-backed SQL database engine. Every transaction is a Git commit.

**[📚 Full Documentation](https://nickyhof.github.io/CommitDB)**

## Features

- 🔄 **Version history** - Every change tracked, nothing lost
- 🌿 **Git branching** - Experiment in branches, merge when ready
- ⏪ **Time travel** - Restore any table to any previous state
- 🔗 **Remote sync** - Push/pull to GitHub, GitLab, or any Git remote
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

## Documentation

- [Installation](https://nickyhof.github.io/CommitDB/installation/)
- [SQL Reference](https://nickyhof.github.io/CommitDB/sql-reference/)
- [Branching & Merging](https://nickyhof.github.io/CommitDB/branching/)
- [Python Driver](https://nickyhof.github.io/CommitDB/python-driver/)
- [Go API](https://nickyhof.github.io/CommitDB/go-api/)
- [Benchmarks](https://nickyhof.github.io/CommitDB/benchmarks/)

## License

MIT