# CommitDB Python Driver

[![PyPI version](https://badge.fury.io/py/commitdb.svg)](https://badge.fury.io/py/commitdb)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

Python client for CommitDB - a Git-backed SQL database engine. Use CommitDB from Python applications with full support for branching, merging, and remote sync.

**[📚 Full Documentation](https://nickyhof.github.io/CommitDB/python-driver/)**

**Two modes:**
- **Remote mode** (`CommitDB`) - Connect to a CommitDB server over TCP
- **Embedded mode** (`CommitDBLocal`) - Run the database directly in-process

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Embedded Mode](#embedded-mode-commitdblocal)
- [Branching & Merging](#branching--merging)
- [Remote Commands](#remote-commands)

## Installation

```bash
pip install commitdb
```

## Quick Start

### Remote Mode (connect to server)

```python
from commitdb import CommitDB

# Using context manager (recommended)
with CommitDB('localhost', 3306) as db:
    # Create database and table
    db.execute('CREATE DATABASE mydb')
    db.execute('CREATE TABLE mydb.users (id INT PRIMARY KEY, name STRING, email STRING)')

    # Insert data
    db.execute("INSERT INTO mydb.users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')")
    db.execute("INSERT INTO mydb.users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')")
    
    # Bulk insert (multiple rows in one statement)
    db.execute("INSERT INTO mydb.users (id, name, email) VALUES (3, 'Charlie', 'c@example.com'), (4, 'Diana', 'd@example.com')")

    # Query data - returns iterable of dictionaries
    result = db.query('SELECT * FROM mydb.users')
    print(f"Found {len(result)} users:")
    for row in result:
        print(f"  {row['id']}: {row['name']} ({row['email']})")

    # Use convenience methods
    db.insert('mydb', 'users', ['id', 'name', 'email'], [5, 'Eve', 'eve@example.com'])

    # Use IN operator for multiple values
    result = db.query("SELECT * FROM mydb.users WHERE id IN (1, 3)")

    # Modify table schema
    db.execute('ALTER TABLE mydb.users ADD COLUMN phone STRING')
    db.execute('ALTER TABLE mydb.users RENAME COLUMN phone TO mobile')
    db.execute('ALTER TABLE mydb.users DROP COLUMN mobile')

    # Use string functions
    result = db.query('SELECT UPPER(name) AS upper_name FROM mydb.users')
    result = db.query("SELECT CONCAT(name, '@example.com') AS email FROM mydb.users")

    # Use date functions
    result = db.query('SELECT NOW() FROM mydb.users')
    result = db.query("SELECT YEAR(created_at), MONTH(created_at) FROM mydb.events")

    # Use JSON columns and functions
    db.execute("CREATE TABLE mydb.docs (id INT PRIMARY KEY, data JSON)")
    db.execute("INSERT INTO mydb.docs (id, data) VALUES (1, '{\"name\":\"Alice\"}')")
    result = db.query("SELECT JSON_EXTRACT(data, '$.name') FROM mydb.docs")

    # Bulk CSV import/export with COPY INTO
    db.execute("COPY INTO '/tmp/users.csv' FROM mydb.users")              # Export
    db.execute("COPY INTO mydb.users FROM '/tmp/users.csv'")              # Import
```

### Embedded Mode (no server required)

```python
from commitdb import CommitDBLocal

# In-memory (data lost when process exits)
with CommitDBLocal() as db:
    db.execute('CREATE DATABASE app')
    db.execute('CREATE TABLE app.users (id INT PRIMARY KEY, name STRING)')
    result = db.query('SELECT * FROM app.users')

# File-based (data persists between sessions)
with CommitDBLocal('/path/to/data') as db:
    db.execute('CREATE DATABASE app')
    # Data is saved to disk and available next time
```

## API Reference

### CommitDB

```python
CommitDB(host='localhost', port=3306)
```

**Methods:**

- `connect(timeout=10.0)` - Connect to server
- `close()` - Close connection
- `authenticate_jwt(token)` - Authenticate with JWT token
- `execute(sql)` - Execute any SQL query
- `query(sql)` - Execute SELECT query, returns QueryResult
- `create_database(name)` - Create a database
- `drop_database(name)` - Drop a database
- `create_table(database, table, columns)` - Create a table
- `insert(database, table, columns, values)` - Insert a row
- `show_databases()` - List databases
- `show_tables(database)` - List tables

**Properties:**
- `authenticated` - Whether connection is authenticated
- `identity` - Authenticated identity string ("Name <email>")
- `use_ssl` - Whether SSL is enabled
- `ssl_verify` - Whether certificate verification is enabled
- `ssl_ca_cert` - Path to CA certificate file

### SSL/TLS Encryption

Secure your connection with TLS encryption:

```python
from commitdb import CommitDB

# SSL with certificate verification (production)
db = CommitDB('localhost', 3306, use_ssl=True, ssl_ca_cert='cert.pem')
db.connect()

# SSL without verification (development only)
db = CommitDB('localhost', 3306, use_ssl=True, ssl_verify=False)
db.connect()

# SSL with JWT authentication
db = CommitDB('localhost', 3306,
              use_ssl=True, ssl_ca_cert='cert.pem',
              jwt_token='eyJhbG...')
db.connect()  # Authenticates over encrypted connection
```

**SSL Parameters:**
| Parameter | Default | Description |
|-----------|---------|-------------|
| `use_ssl` | `False` | Enable SSL/TLS encryption |
| `ssl_verify` | `True` | Verify server certificate |
| `ssl_ca_cert` | `None` | Path to CA certificate for verification |

### Authentication

When connecting to a server with JWT authentication enabled, you must authenticate before executing queries:

```python
from commitdb import CommitDB

# Option 1: Auto-authenticate on connect
db = CommitDB('localhost', 3306, jwt_token='eyJhbG...')
db.connect()  # Automatically authenticates
print(f"Authenticated as: {db.identity}")

# Option 2: Authenticate after connecting
db = CommitDB('localhost', 3306)
db.connect()
db.authenticate_jwt('eyJhbG...')
print(f"Authenticated: {db.authenticated}")

# Queries now use authenticated identity for Git commits
db.execute('CREATE DATABASE mydb')
```

**Authentication flow:**
1. Client sends `AUTH JWT <token>`
2. Server validates token and extracts name/email claims
3. All subsequent queries use that identity for Git commit authorship

### QueryResult

```python
result = db.query('SELECT * FROM mydb.users')
result.columns  # ['id', 'name']
result.data     # [['1', 'Alice'], ['2', 'Bob']]
len(result)     # 2
result[0]       # {'id': '1', 'name': 'Alice'}
for row in result:
    print(row)  # {'id': '1', 'name': 'Alice'}
```

### CommitResult

```python
result = db.execute('INSERT INTO mydb.users ...')
result.records_written   # 1
result.affected_rows     # 1
result.time_ms           # 1.5
```

### Error Handling

```python
from commitdb import CommitDB, CommitDBError

try:
    with CommitDB('localhost', 3306) as db:
        db.execute('SELECT * FROM nonexistent.table')
except CommitDBError as e:
    print(f"Database error: {e}")
except ConnectionError as e:
    print(f"Connection failed: {e}")
```

## Embedded Mode (CommitDBLocal)

Run CommitDB directly in-process without a separate server. Requires the `libcommitdb` shared library.

### Quick Start

```python
from commitdb import CommitDBLocal

# In-memory database (no persistence)
with CommitDBLocal() as db:
    db.execute('CREATE DATABASE mydb')
    db.execute('CREATE TABLE mydb.users (id INT PRIMARY KEY, name STRING)')
    db.execute("INSERT INTO mydb.users (id, name) VALUES (1, 'Alice')")
    result = db.query('SELECT * FROM mydb.users')

# File-based persistence
with CommitDBLocal('/path/to/data') as db:
    db.execute('CREATE DATABASE mydb')
    # Data persists between sessions
```

### Building the Shared Library

If the library isn't bundled with your pip install:

```bash
# From CommitDB root
make lib  # Creates lib/libcommitdb.dylib (macOS) or .so (Linux)
```

### CommitDBLocal API

```python
CommitDBLocal(path=None, lib_path=None)
```

**Parameters:**
- `path` - Directory for file-based persistence. If `None`, uses in-memory storage.
- `lib_path` - Optional path to `libcommitdb` shared library.

**Methods:**
- `open()` - Open the database
- `close()` - Close the database
- `execute(sql)` - Execute any SQL query
- `query(sql)` - Execute SELECT query
- `create_database(name)` - Create a database
- `create_table(database, table, columns)` - Create a table
- `insert(database, table, columns, values)` - Insert a row

### Example: Full CRUD

```python
from commitdb import CommitDBLocal

with CommitDBLocal() as db:
    # Create
    db.create_database('app')
    db.create_table('app', 'users', 'id INT PRIMARY KEY, name STRING, email STRING')
    
    # Insert
    db.insert('app', 'users', ['id', 'name', 'email'], [1, 'Alice', 'alice@example.com'])
    db.insert('app', 'users', ['id', 'name', 'email'], [2, 'Bob', 'bob@example.com'])
    
    # Read
    result = db.query('SELECT * FROM app.users')
    for row in result:
        print(f"{row['name']}: {row['email']}")
    
    # Update
    db.execute("UPDATE app.users SET email = 'alice@new.com' WHERE id = 1")
    
    # Delete
    db.execute('DELETE FROM app.users WHERE id = 2')
```

### Branching & Merging

Use Git-like branching for isolated experimentation:

```python
from commitdb import CommitDBLocal

with CommitDBLocal() as db:
    # Setup
    db.execute('CREATE DATABASE mydb')
    db.execute('CREATE TABLE mydb.users (id INT PRIMARY KEY, name STRING)')
    db.execute("INSERT INTO mydb.users (id, name) VALUES (1, 'Alice')")
    
    # Create a branch
    db.execute('CREATE BRANCH feature')
    
    # Switch to the branch
    db.execute('CHECKOUT feature')
    
    # Make changes (only visible on this branch)
    db.execute("INSERT INTO mydb.users (id, name) VALUES (2, 'Bob')")
    
    # See all branches
    branches = db.query('SHOW BRANCHES')
    for branch in branches:
        print(f"{branch['Branch']} {'(current)' if branch['Current'] == '*' else ''}")
    
    # Switch back to master
    db.execute('CHECKOUT master')
    
    # Merge feature branch (auto-resolves conflicts with Last-Writer-Wins)
    db.execute('MERGE feature')
    
    # Now master has both rows
    result = db.query('SELECT * FROM mydb.users')
    print(f"Users after merge: {len(result)}")  # 2
```

### Manual Conflict Resolution

When branches have conflicting changes, you can resolve them manually:

```python
with CommitDBLocal() as db:
    # ... setup with conflicting changes on master and feature ...
    
    # Merge with manual resolution mode
    db.execute('MERGE feature WITH MANUAL RESOLUTION')
    
    # View pending conflicts
    conflicts = db.query('SHOW MERGE CONFLICTS')
    for c in conflicts:
        print(f"Conflict: {c['Database']}.{c['Table']}.{c['Key']}")
        print(f"  HEAD: {c['HEAD']}")
        print(f"  SOURCE: {c['SOURCE']}")
    
    # Resolve each conflict
    for c in conflicts:
        key = f"{c['Database']}.{c['Table']}.{c['Key']}"
        # Choose HEAD, SOURCE, or custom value
        db.execute(f'RESOLVE CONFLICT {key} USING HEAD')
    
    # Complete the merge
    db.execute('COMMIT MERGE')
    
    # Or abort if needed
    # db.execute('ABORT MERGE')
```

**Branch Commands:**
| Command | Description |
|---------|-------------|
| `CREATE BRANCH name` | Create new branch at HEAD |
| `CREATE BRANCH name FROM 'txn_id'` | Create branch at specific transaction |
| `CHECKOUT name` | Switch to branch |
| `MERGE name` | Merge branch (Last-Writer-Wins auto-resolve) |
| `MERGE name WITH MANUAL RESOLUTION` | Merge with manual conflict resolution |
| `SHOW BRANCHES` | List all branches |
| `SHOW MERGE CONFLICTS` | View pending conflicts |
| `RESOLVE CONFLICT path USING HEAD\|SOURCE\|'value'` | Resolve a conflict |
| `COMMIT MERGE` | Complete pending merge |
| `ABORT MERGE` | Cancel pending merge |

**Remote Commands:**
| Command | Description |
|---------|-------------|
| `CREATE REMOTE name 'url'` | Add a remote repository |
| `SHOW REMOTES` | List configured remotes |
| `DROP REMOTE name` | Remove a remote |
| `PUSH` | Push to origin (default) |
| `PUSH TO remote` | Push to specific remote |
| `PUSH TO remote BRANCH name` | Push specific branch |
| `PULL` | Pull from origin (default) |
| `PULL FROM remote` | Pull from specific remote |
| `PULL FROM remote BRANCH name` | Pull specific branch |
| `FETCH` | Fetch from origin |
| `FETCH FROM remote` | Fetch from specific remote |

**Remote Authentication:**
```python
# Token-based auth
db.execute("PUSH WITH TOKEN 'ghp_xxxxxxxxxxxx'")

# SSH key auth
db.execute("PUSH WITH SSH KEY '/path/to/id_rsa'")
db.execute("PUSH WITH SSH KEY '/path/to/id_rsa' PASSPHRASE 'password'")

# Basic auth
db.execute("PULL FROM origin WITH USER 'username' PASSWORD 'password'")
```
