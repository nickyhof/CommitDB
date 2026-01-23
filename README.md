# CommitDB

A Git-backed SQL database engine written in Go. Every transaction is a Git commit, providing built-in version control, history, and the ability to restore to any point in time.

https://pkg.go.dev/github.com/nickyhof/CommitDB

## Features

- **SQL Support**: Full SQL parsing and execution
- **Git-backed Storage**: Every transaction is a git commit
- **Version Control**: Time-travel queries, restore to any point
- **Aggregates**: SUM, AVG, MIN, MAX, COUNT with GROUP BY
- **JOINs**: INNER, LEFT, RIGHT joins
- **Transactions**: BEGIN, COMMIT, ROLLBACK
- **Indexing**: CREATE INDEX for faster queries
- **Concurrency Safe**: Thread-safe operations with RWMutex
- **SQL Server**: TCP server with JSON protocol for remote access

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

### Example Session

```
╔═══════════════════════════════════════╗
║         CommitDB v1.0.0               ║
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

### WHERE Operators

| Operator | Example |
|----------|---------|
| `=` | `WHERE id = 1` |
| `!=`, `<>` | `WHERE status != 'deleted'` |
| `<`, `>` | `WHERE age > 18` |
| `<=`, `>=` | `WHERE price <= 100` |
| `AND` | `WHERE age > 18 AND active = true` |
| `OR` | `WHERE city = 'NYC' OR city = 'LA'` |
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

Performance benchmarks run on Apple M1 Pro:

### SQL Parsing (no I/O)

| Benchmark | Time | Memory |
|-----------|------|--------|
| Simple SELECT | 178 ns | 336 B |
| SELECT with WHERE | 275 ns | 389 B |
| SELECT with ORDER BY | 302 ns | 365 B |
| Complex SELECT | 578 ns | 528 B |
| INSERT | 569 ns | 405 B |
| UPDATE | 297 ns | 240 B |
| DELETE | 217 ns | 176 B |

### Query Execution (1000 rows, in-memory)

| Benchmark | Time | Throughput |
|-----------|------|------------|
| SELECT * | 2.3 ms | ~430 ops/sec |
| SELECT with WHERE | 2.3 ms | ~430 ops/sec |
| SELECT with ORDER BY | 3.0 ms | ~330 ops/sec |
| SELECT with LIMIT | 2.4 ms | ~420 ops/sec |
| COUNT(*) | 2.5 ms | ~400 ops/sec |
| SUM/AVG/MIN/MAX | 2.5 ms | ~400 ops/sec |
| DISTINCT | 2.5 ms | ~400 ops/sec |
| INSERT | 11.3 ms | ~88 ops/sec |
| UPDATE | 4.6 ms | ~220 ops/sec |

### Lexer

| Benchmark | Time | Memory |
|-----------|------|--------|
| Tokenize complex query | 442 ns | 48 B |

## Architecture

```
CommitDB/
├── cmd/cli/      # CLI application
├── cmd/server/   # TCP SQL server
├── core/         # Core types (Identity, Table, Column)
├── db/           # Database engine and execution
├── op/           # Table operations
├── ps/           # Persistence layer (Git-backed)
└── sql/          # SQL lexer and parser
```