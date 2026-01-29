# CommitDB Python Client

[![PyPI version](https://badge.fury.io/py/commitdb.svg)](https://badge.fury.io/py/commitdb)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

Python client for CommitDB - a Git-backed SQL database engine.

**[📚 Full Documentation](https://nickyhof.github.io/CommitDB/python-client/)**

> ⚠️ **Experimental Project** - This is a hobby project and should not be used in any production environment.

## Installation

```bash
pip install commitdb
```

## Quick Start

### Remote Mode (connect to server)

```python
from commitdb import CommitDB

with CommitDB('localhost', 3306) as db:
    db.execute('CREATE DATABASE mydb')
    db.execute('CREATE TABLE mydb.users (id INT PRIMARY KEY, name STRING)')
    db.execute("INSERT INTO mydb.users VALUES (1, 'Alice')")
    
    result = db.query('SELECT * FROM mydb.users')
    for row in result:
        print(f"{row['id']}: {row['name']}")
```

### Embedded Mode (no server required)

```python
from commitdb import CommitDBLocal

with CommitDBLocal() as db:  # In-memory
    db.execute('CREATE DATABASE mydb')
    db.execute('CREATE TABLE mydb.users (id INT PRIMARY KEY, name STRING)')
    
with CommitDBLocal('/path/to/data') as db:  # File-based (persistent)
    db.execute('CREATE DATABASE mydb')
```

### Ibis Mode (pandas DataFrame support)

```bash
pip install commitdb[ibis]
```

```python
import ibis

con = ibis.commitdb.connect('localhost', 3306, database='mydb')

# Or use URL-based connection:
con = ibis.connect('commitdb://localhost:3306/mydb')

# Query with Ibis expressions
users = con.table('users')
result = users.filter(users.age > 30).select('name', 'city').execute()  # → pandas DataFrame
print(result)

# Insert from DataFrame
import pandas as pd
df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
con.insert('users', df)
```

---

## API Reference

### CommitDB (Remote)

```python
CommitDB(host='localhost', port=3306, use_ssl=False, ssl_verify=True, 
         ssl_ca_cert=None, jwt_token=None)
```

| Method | Description |
|--------|-------------|
| `connect(timeout=10.0)` | Connect to server |
| `close()` | Close connection |
| `execute(sql)` | Execute SQL (INSERT, UPDATE, CREATE, etc.) |
| `query(sql)` | Execute SELECT, returns QueryResult |
| `authenticate_jwt(token)` | Authenticate with JWT |

### CommitDBLocal (Embedded)

```python
CommitDBLocal(path=None, lib_path=None)
```

- `path` - Directory for persistence (`None` = in-memory)
- `lib_path` - Path to `libcommitdb` shared library

| Method | Description |
|--------|-------------|
| `open()` / `close()` | Open/close database |
| `execute(sql)` | Execute SQL |
| `query(sql)` | Execute SELECT |

### QueryResult

```python
result = db.query('SELECT * FROM mydb.users')
result.columns  # ['id', 'name']
len(result)     # Row count
result[0]       # {'id': '1', 'name': 'Alice'}

for row in result:
    print(row)
```

### Error Handling

```python
from commitdb import CommitDB, CommitDBError

try:
    with CommitDB('localhost', 3306) as db:
        db.execute('SELECT * FROM nonexistent.table')
except CommitDBError as e:
    print(f"Database error: {e}")
```

---

## Security

### SSL/TLS Encryption

```python
# Production (with certificate verification)
db = CommitDB('localhost', 3306, use_ssl=True, ssl_ca_cert='cert.pem')

# Development (skip verification)
db = CommitDB('localhost', 3306, use_ssl=True, ssl_verify=False)
```

### JWT Authentication

```python
# Auto-authenticate on connect
db = CommitDB('localhost', 3306, jwt_token='eyJhbG...')
db.connect()
print(f"Authenticated as: {db.identity}")

# Or authenticate after connect
db.connect()
db.authenticate_jwt('eyJhbG...')
```

---

## Version Control

### Branching & Merging

```python
with CommitDBLocal() as db:
    db.execute('CREATE DATABASE mydb')
    db.execute('CREATE TABLE mydb.users (id INT, name STRING)')
    db.execute("INSERT INTO mydb.users VALUES (1, 'Alice')")
    
    # Create and switch to branch
    db.execute('CREATE BRANCH feature')
    db.execute('CHECKOUT feature')
    
    # Make changes (isolated to this branch)
    db.execute("INSERT INTO mydb.users VALUES (2, 'Bob')")
    
    # Switch back and merge
    db.execute('CHECKOUT master')
    db.execute('MERGE feature')
```

**Branch Commands:**

| Command | Description |
|---------|-------------|
| `CREATE BRANCH name` | Create new branch |
| `CHECKOUT name` | Switch to branch |
| `MERGE name` | Merge branch (auto-resolve) |
| `MERGE name WITH MANUAL RESOLUTION` | Merge with manual conflict resolution |
| `SHOW BRANCHES` | List branches |
| `SHOW MERGE CONFLICTS` | View pending conflicts |
| `RESOLVE CONFLICT path USING HEAD\|SOURCE` | Resolve conflict |
| `COMMIT MERGE` / `ABORT MERGE` | Finalize or cancel merge |

### Remote Sync (Git)

```python
# Add remote
db.execute("CREATE REMOTE origin 'https://github.com/user/repo.git'")

# Push/Pull with authentication
db.execute("PUSH WITH TOKEN 'ghp_xxxxxxxxxxxx'")
db.execute("PUSH WITH SSH KEY '/path/to/id_rsa'")
db.execute("PULL FROM origin")
```

---

## Bulk Import/Export

### Local Files

```python
# Export to CSV
db.execute("COPY INTO '/path/to/users.csv' FROM mydb.users WITH (HEADER = TRUE)")

# Import from CSV
db.execute("COPY INTO mydb.users FROM '/path/to/users.csv' WITH (HEADER = TRUE)")
```

### S3 & HTTPS

```python
# Import from HTTPS
db.execute("COPY INTO mydb.users FROM 'https://example.com/data.csv'")

# Import from S3 (uses AWS env vars)
db.execute("COPY INTO mydb.users FROM 's3://bucket/users.csv'")

# Import from S3 with explicit credentials
db.execute("""
    COPY INTO mydb.users FROM 's3://bucket/users.csv' WITH (
        AWS_KEY = 'AKIAIOSFODNN7EXAMPLE',
        AWS_SECRET = 'your-secret-key',
        AWS_REGION = 'us-east-1'
    )
""")

# Export to S3
db.execute("COPY INTO 's3://bucket/export.csv' FROM mydb.users WITH (HEADER = TRUE)")
```

---

## Building the Shared Library

For embedded mode, if the library isn't bundled:

```bash
# From CommitDB root
make lib  # Creates lib/libcommitdb.dylib (macOS) or .so (Linux)
```
