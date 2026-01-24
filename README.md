# CommitDB

[![Go Reference](https://pkg.go.dev/badge/github.com/nickyhof/CommitDB.svg)](https://pkg.go.dev/github.com/nickyhof/CommitDB)
[![Go Report Card](https://goreportcard.com/badge/github.com/nickyhof/CommitDB)](https://goreportcard.com/report/github.com/nickyhof/CommitDB)

A Git-backed SQL database engine written in Go. Every transaction is a Git commit, providing built-in version control, complete history, branching, and the ability to restore to any point in time.

**Why CommitDB?**
- 🔄 **Full version history** - Every change is tracked, nothing is lost
- 🌿 **Git branching** - Experiment in branches, merge when ready
- ⏪ **Time travel** - Restore any table to any previous state
- 🔗 **Remote sync** - Push/pull to GitHub, GitLab, or any Git remote
- 🐍 **Python support** - Native driver for Python applications

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
- [SQL Reference](#sql-reference)
- [Branching & Merging](#branching--merging)
- [Remote Operations](#remote-operations)
- [Programmatic API](#programmatic-api)
- [CLI Commands](#cli-commands)
- [Benchmarks](#benchmarks)
- [Architecture](#architecture)

## Features

| Category | Features |
|----------|----------|
| **SQL** | SELECT, INSERT, UPDATE, DELETE, CREATE/DROP TABLE/DATABASE/INDEX |
| **Queries** | WHERE, ORDER BY, LIMIT, OFFSET, DISTINCT, GROUP BY, HAVING |
| **Aggregates** | SUM, AVG, MIN, MAX, COUNT |
| **JOINs** | INNER, LEFT, RIGHT |
| **Version Control** | Branching, merging, snapshots, time-travel restore |
| **Remote** | Push, pull, fetch with token/SSH/basic authentication |
| **Performance** | Indexing, in-memory or file-based storage |
| **Concurrency** | Thread-safe with RWMutex |
| **Drivers** | Python, Go, TCP/JSON protocol |

## Quick Start

### Installation

```bash
go get github.com/nickyhof/CommitDB
```

### Using the CLI

```bash
# Build the CLI
go build -o commitdb-cli ./cmd/cli

# Run with in-memory storage
./commitdb-cli

# Run with file-based persistence
./commitdb-cli -baseDir=/path/to/data
```

### Docker

```bash
# Pull from GitHub Container Registry
docker pull ghcr.io/nickyhof/commitdb:latest

# Run with in-memory storage
docker run -p 3306:3306 ghcr.io/nickyhof/commitdb

# Run with persistent storage
docker run -p 3306:3306 -v /path/to/data:/data ghcr.io/nickyhof/commitdb

# Run with TLS
docker run -p 3306:3306 \
  -v /path/to/certs:/certs \
  ghcr.io/nickyhof/commitdb \
  --tls-cert /certs/cert.pem --tls-key /certs/key.pem

# Run with JWT authentication
docker run -p 3306:3306 -v /path/to/data:/data \
  ghcr.io/nickyhof/commitdb \
  --jwt-secret "your-secret" --jwt-issuer "https://auth.example.com"
```

### Using the SQL Server

```bash
# Build the server
go build -o commitdb-server ./cmd/server

# Run on default port (3306)
./commitdb-server

# Run with custom port and file persistence
./commitdb-server -port 5432 -baseDir=/path/to/data

# Connect with netcat
echo 'CREATE DATABASE test' | nc localhost 3306
echo 'SELECT * FROM test.users' | nc localhost 3306
```

The server accepts SQL queries (one per line) and returns JSON responses:
```json
{"success":true,"type":"query","result":{"columns":["id","name"],"data":[["1","Alice"]],"records_read":1}}
```

### Server Authentication (JWT)

The server supports optional JWT authentication. When enabled, clients must authenticate before executing queries, and their identity is used for Git commit authorship.

**Architecture:**
```
┌────────────────┐      ┌─────────────────┐      ┌────────────────┐
│ Identity       │      │ CommitDB        │      │ Client         │
│ Provider       │◄─────│ Server          │◄─────│ (Python/CLI)   │
│ (Auth0/etc)    │      │                 │      │                │
└────────────────┘      └─────────────────┘      └────────────────┘
        │                        │                       │
        │ 1. JWKS keys           │                       │
        │───────────────────────►│                       │
        │                        │                       │
        │                        │ 2. AUTH JWT <token>   │
        │                        │◄──────────────────────│
        │                        │                       │
        │                        │ 3. Validate + extract │
        │                        │    name/email claims  │
        │                        │                       │
        │                        │ 4. OK (authenticated) │
        │                        │──────────────────────►│
        │                        │                       │
        │                        │ 5. SQL queries with   │
        │                        │    authenticated      │
        │                        │    identity for Git   │
```

**Server Configuration:**
```bash
# Run with JWT authentication (HS256 shared secret)
./commitdb-server --jwt-secret "your-shared-secret" --jwt-issuer "https://auth.example.com"

# Without auth (default identity used for all connections)
./commitdb-server
```

**Client Authentication:**
```bash
# Authenticate via netcat
echo 'AUTH JWT eyJhbGciOiJIUzI1NiIs...' | nc localhost 3306
# Response: {"success":true,"type":"auth","result":{"authenticated":true,"identity":"Alice <alice@example.com>"}}

# Then execute queries
echo 'CREATE DATABASE mydb' | nc localhost 3306
```

**Expected JWT Claims:**
```json
{
  "name": "Alice Smith",
  "email": "alice@example.com",
  "exp": 1706140800
}
```

### Server TLS/SSL Encryption

Enable encrypted connections with TLS:

```bash
# Generate self-signed certificate (for development)
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes \
    -subj "/CN=localhost"

# Start server with TLS
./commitdb-server --tls-cert cert.pem --tls-key key.pem

# With TLS and JWT authentication
./commitdb-server --tls-cert cert.pem --tls-key key.pem --jwt-secret "your-secret"

# Test with openssl
openssl s_client -connect localhost:3306
```

**Python client with SSL:**
```python
from commitdb import CommitDB

# SSL with certificate verification
db = CommitDB('localhost', 3306, use_ssl=True, ssl_ca_cert='cert.pem')
db.connect()

# SSL without verification (dev only)
db = CommitDB('localhost', 3306, use_ssl=True, ssl_verify=False)
db.connect()

# SSL + JWT authentication
db = CommitDB('localhost', 3306, 
              use_ssl=True, ssl_ca_cert='cert.pem',
              jwt_token='eyJhbG...')
db.connect()
```

### Python Driver

```bash
pip install commitdb
```

**Remote mode** (connect to server):
```python
from commitdb import CommitDB

with CommitDB('localhost', 3306) as db:
    db.execute('CREATE DATABASE mydb')
    result = db.query('SELECT * FROM mydb.users')
    for row in result:
        print(row)
```

**Embedded mode** (no server required):
```python
from commitdb import CommitDBLocal

# In-memory database
with CommitDBLocal() as db:
    db.execute('CREATE DATABASE mydb')
    db.execute('CREATE TABLE mydb.users (id INT PRIMARY KEY, name STRING)')
    db.execute("INSERT INTO mydb.users (id, name) VALUES (1, 'Alice')")
    result = db.query('SELECT * FROM mydb.users')

# File-based persistence
with CommitDBLocal('/path/to/data') as db:
    db.execute('CREATE DATABASE mydb')
```

See [drivers/python/README.md](drivers/python/README.md) for full documentation.

### Example Session

```
╔═══════════════════════════════════════╗
║         CommitDB v1.5.0               ║
║   Git-backed SQL Database Engine      ║
╚═══════════════════════════════════════╝

commitdb> CREATE DATABASE myapp;
commitdb> CREATE TABLE myapp.users (id INT PRIMARY KEY, name STRING, email STRING);
commitdb> INSERT INTO myapp.users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');
commitdb> SELECT * FROM myapp.users;
+----+-------+-------------------+
| id | name  | email             |
+----+-------+-------------------+
| 1  | Alice | alice@example.com |
+----+-------+-------------------+
```

## SQL Reference

### Data Definition

```sql
-- Databases
CREATE DATABASE mydb;
DROP DATABASE mydb;
SHOW DATABASES;

-- Tables
CREATE TABLE mydb.users (
    id INT PRIMARY KEY,
    name STRING,
    email STRING,
    age INT,
    active BOOL,
    created TIMESTAMP
);
DROP TABLE mydb.users;
SHOW TABLES IN mydb;
DESCRIBE mydb.users;

-- Indexes
CREATE INDEX idx_name ON mydb.users(name);
CREATE UNIQUE INDEX idx_email ON mydb.users(email);
DROP INDEX idx_name ON mydb.users;
SHOW INDEXES ON mydb.users;

-- Alter Table
ALTER TABLE mydb.users ADD COLUMN phone STRING;
ALTER TABLE mydb.users DROP COLUMN phone;
ALTER TABLE mydb.users MODIFY COLUMN name TEXT;
ALTER TABLE mydb.users RENAME COLUMN name TO username;
```

### Data Manipulation

```sql
-- Insert
INSERT INTO mydb.users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');

-- Select
SELECT * FROM mydb.users;
SELECT name, email FROM mydb.users WHERE age > 25;
SELECT * FROM mydb.users ORDER BY name ASC LIMIT 10 OFFSET 5;
SELECT DISTINCT city FROM mydb.users;

-- Update (by primary key)
UPDATE mydb.users SET name = 'Bob' WHERE id = 1;

-- Delete (by primary key)
DELETE FROM mydb.users WHERE id = 1;
```

### Aggregate Functions

```sql
SELECT COUNT(*) FROM mydb.orders;
SELECT SUM(amount) FROM mydb.orders;
SELECT AVG(price) FROM mydb.products;
SELECT MIN(age), MAX(age) FROM mydb.users;

-- With GROUP BY
SELECT category, SUM(amount) FROM mydb.orders GROUP BY category;
SELECT city, COUNT(id) FROM mydb.users GROUP BY city HAVING COUNT(id) > 10;
```

### Joins

```sql
-- Inner Join
SELECT * FROM mydb.orders 
INNER JOIN mydb.customers ON customer_id = id;

-- Left Join
SELECT o.id, c.name FROM mydb.orders o
LEFT JOIN mydb.customers c ON o.customer_id = c.id;

-- Right Join
SELECT * FROM mydb.products
RIGHT JOIN mydb.categories ON category_id = id;
```

### Transactions

```sql
BEGIN;
-- ... operations ...
COMMIT;
-- or
ROLLBACK;
```

### Snapshots & Restore

CommitDB's Git-backed storage enables powerful version control features:

```go
// Create a named snapshot (git tag) at current state
persistence.Snapshot("v1.0.0", nil)

// Create a snapshot at a specific transaction
persistence.Snapshot("before-migration", &transaction)

// Recover to a named snapshot (resets all data)
persistence.Recover("v1.0.0")

// Restore to a specific transaction
persistence.Restore(transaction, nil, nil)

// Restore only a specific database
db := "mydb"
persistence.Restore(transaction, &db, nil)

// Restore only a specific table
table := "users"
persistence.Restore(transaction, &db, &table)
```

### Branching & Merging

Create isolated branches to experiment with changes, then merge back:

```sql
-- Create a new branch
CREATE BRANCH feature_x

-- Switch to the branch
CHECKOUT feature_x

-- Make changes (only visible on this branch)
INSERT INTO mydb.users (id, name) VALUES (100, 'NewUser');
ALTER TABLE mydb.users ADD COLUMN email STRING;

-- See all branches
SHOW BRANCHES

-- Switch back to main branch
CHECKOUT master

-- Merge changes from feature branch (auto-resolves conflicts with Last-Writer-Wins)
MERGE feature_x

-- Or merge with manual conflict resolution
MERGE feature_x WITH MANUAL RESOLUTION

-- View pending conflicts
SHOW MERGE CONFLICTS

-- Resolve conflicts
RESOLVE CONFLICT mydb.users.1 USING HEAD    -- Keep current branch value
RESOLVE CONFLICT mydb.users.1 USING SOURCE  -- Keep feature branch value
RESOLVE CONFLICT mydb.users.1 USING '{"custom":"value"}'  -- Custom value

-- Complete the merge
COMMIT MERGE

-- Or abort the merge
ABORT MERGE
```

**Merge Strategies:**
| Strategy | Description |
|----------|-------------|
| Row-level (default) | Auto-resolves conflicts using Last-Writer-Wins (later commit wins) |
| Manual | Pauses on conflicts, requires resolution before completing |

**Go API:**
```go
// Create branch at current HEAD
persistence.Branch("feature", nil)

// Create branch from specific transaction
persistence.Branch("old-state", &transaction)

// Switch branches
persistence.Checkout("feature")

// List all branches
branches, _ := persistence.ListBranches()

// Get current branch
current, _ := persistence.CurrentBranch()

// Merge with default strategy (row-level, Last-Writer-Wins)
txn, err := persistence.Merge("feature", identity)

// Merge with options
result, err := persistence.MergeWithOptions("feature", identity, ps.MergeOptions{
    Strategy: ps.MergeStrategyManual,
})

// Handle pending merge
if result.Pending {
    for _, conflict := range result.Unresolved {
        // Resolve each conflict
        persistence.ResolveConflict(conflict.Database, conflict.Table, conflict.Key, resolution)
    }
    persistence.CompleteMerge(identity)
}

// Or abort
persistence.AbortMerge()

// Delete branch
persistence.DeleteBranch("feature")
```

### Remote Operations

Push and pull branches to/from remote Git repositories:

```sql
-- Add a remote
CREATE REMOTE origin 'https://github.com/user/repo.git'
CREATE REMOTE upstream 'git@github.com:upstream/repo.git'

-- List remotes
SHOW REMOTES

-- Remove a remote
DROP REMOTE upstream

-- Push to remote (default: origin, current branch)
PUSH
PUSH TO origin
PUSH TO origin BRANCH feature

-- Pull from remote
PULL
PULL FROM origin
PULL FROM origin BRANCH master

-- Fetch without merging
FETCH
FETCH FROM upstream
```

**Authentication:**
```sql
-- Token-based (GitHub, GitLab, etc.)
PUSH WITH TOKEN 'ghp_xxxxxxxxxxxx'
PULL FROM origin WITH TOKEN 'ghp_xxxxxxxxxxxx'

-- SSH key authentication
PUSH WITH SSH KEY '/path/to/id_rsa'
PUSH WITH SSH KEY '/path/to/id_rsa' PASSPHRASE 'mypassword'

-- Basic auth (username/password)
PULL FROM origin WITH USER 'username' PASSWORD 'password'
```

**Go API:**
```go
// Add a remote
persistence.AddRemote("origin", "https://github.com/user/repo.git")

// List remotes
remotes, _ := persistence.ListRemotes()

// Remove a remote
persistence.RemoveRemote("origin")

// Push (no auth)
persistence.Push("origin", "master", nil)

// Push with token auth
auth := &ps.RemoteAuth{
    Type:  ps.AuthTypeToken,
    Token: "ghp_xxxxxxxxxxxx",
}
persistence.Push("origin", "master", auth)

// Pull with SSH auth
auth := &ps.RemoteAuth{
    Type:       ps.AuthTypeSSH,
    KeyPath:    "/path/to/id_rsa",
    Passphrase: "optional_passphrase",
}
persistence.Pull("origin", "master", auth)

// Fetch
persistence.Fetch("origin", nil)
```

### WHERE Operators

| Operator | Example |
|----------|---------|
| `=` | `WHERE id = 1` |
| `!=`, `<>` | `WHERE status != 'deleted'` |
| `<`, `>` | `WHERE age > 18` |
| `<=`, `>=` | `WHERE price <= 100` |
| `AND` | `WHERE age > 18 AND active = true` |
| `OR` | `WHERE city = 'NYC' OR city = 'LA'` |
| `IN` | `WHERE status IN ('active', 'pending')` |
| `NOT IN` | `WHERE NOT category IN ('archived')` |
| `IS NULL` | `WHERE email IS NULL` |
| `IS NOT NULL` | `WHERE email IS NOT NULL` |
| `LIKE` | `WHERE name LIKE 'A%'` |

## Programmatic API

```go
package main

import (
    "fmt"
    "github.com/nickyhof/CommitDB"
    "github.com/nickyhof/CommitDB/core"
    "github.com/nickyhof/CommitDB/ps"
)

func main() {
    // Create persistence layer
    persistence, _ := ps.NewMemoryPersistence()
    // Or use file-based: persistence, _ := ps.NewFilePersistence("/path/to/data", nil)

    // Open database
    db := CommitDB.Open(&persistence)

    // Create engine with identity (for git commits)
    engine := db.Engine(core.Identity{
        Name:  "MyApp",
        Email: "app@example.com",
    })

    // Execute SQL
    result, err := engine.Execute("CREATE DATABASE myapp")
    if err != nil {
        panic(err)
    }

    result, _ = engine.Execute("CREATE TABLE myapp.users (id INT PRIMARY KEY, name STRING)")
    result, _ = engine.Execute("INSERT INTO myapp.users (id, name) VALUES (1, 'Alice')")
    result, _ = engine.Execute("SELECT * FROM myapp.users")

    // Display results
    result.Display()
}
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `.help` | Show help message |
| `.quit`, `.exit` | Exit the CLI |
| `.databases` | List all databases |
| `.tables <db>` | List tables in database |
| `.use <db>` | Set default database |
| `.history` | Show command history |
| `.clear` | Clear screen |
| `.version` | Show version |

## Column Types

| Type | Description |
|------|-------------|
| `INT` | Integer values |
| `STRING` | Short strings |
| `TEXT` | Long text |
| `FLOAT` | Floating point |
| `BOOL` | Boolean (true/false) |
| `TIMESTAMP` | Date/time values |

## Testing

```bash
# Run all tests
go test ./...

# Run benchmarks
go test -bench=. ./...

# Run integration tests only
go test -run Integration ./...
```

## Benchmarks

Performance benchmarks run on Apple M1 Pro (v1.5.0):

### SQL Parsing (no I/O)

| Benchmark | Time | Memory |
|-----------|------|--------|
| Simple SELECT | 184 ns | 336 B |
| SELECT with WHERE | 282 ns | 389 B |
| SELECT with ORDER BY | 310 ns | 365 B |
| Complex SELECT | 587 ns | 528 B |
| INSERT | 573 ns | 405 B |
| UPDATE | 308 ns | 240 B |
| DELETE | 219 ns | 176 B |

### Query Execution (1000 rows, in-memory)

| Benchmark | Time | Throughput |
|-----------|------|------------|
| SELECT * | 2.5 ms | ~400 ops/sec |
| SELECT with WHERE | 2.6 ms | ~385 ops/sec |
| SELECT with ORDER BY | 3.3 ms | ~305 ops/sec |
| SELECT with LIMIT | 2.4 ms | ~415 ops/sec |
| COUNT(*) | 2.4 ms | ~415 ops/sec |
| SUM/AVG/MIN/MAX | 2.5 ms | ~400 ops/sec |
| DISTINCT | 2.5 ms | ~400 ops/sec |
| INSERT | 11.2 ms | ~89 ops/sec |
| UPDATE | 4.9 ms | ~205 ops/sec |

### Lexer

| Benchmark | Time | Memory |
|-----------|------|--------|
| Tokenize complex query | 447 ns | 48 B |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Applications                           │
│   cmd/cli/        CLI application                          │
│   cmd/server/     TCP SQL server with JSON protocol        │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                    sql/  SQL Layer                          │
│   Lexer:   Tokenizes SQL into keywords, identifiers, etc.  │
│   Parser:  Builds AST (statement structs) from tokens      │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                    db/   Engine Layer                       │
│   Executes parsed statements, handles transactions         │
│   Returns QueryResult or CommitResult                      │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                    op/   Operations Layer                   │
│   High-level wrappers for database and table operations    │
│   DatabaseOp:  Create, Get, Drop, Restore, TableNames      │
│   TableOp:     Get, Put, Delete, Scan, Keys, Count         │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                    ps/   Persistence Layer                  │
│   Git-backed storage with go-git library                   │
│   CRUD, branching, merging, snapshots, transactions        │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                    Git Repository                           │
│   Every transaction = Git commit                           │
│   Tables stored as JSON files in directories               │
└─────────────────────────────────────────────────────────────┘
```

### Package Summary

| Package | Purpose |
|---------|---------|
| `core/` | Shared types: `Identity`, `Table`, `Column`, `Database` |
| `sql/` | SQL lexer and parser, statement types |
| `db/` | Query execution engine, result types |
| `op/` | Database/table operations wrapper (convenience methods) |
| `ps/` | Git-backed persistence, branching, merging, remotes |
| `cmd/cli/` | Interactive command-line interface |
| `cmd/server/` | TCP server with JSON protocol |
| `bindings/` | CGO bindings for embedded use |
| `drivers/python/` | Python client library |