# Python Driver

The CommitDB Python driver provides a native Python interface for connecting to CommitDB servers.

## Installation

```bash
pip install commitdb
```

## Quick Start

```python
from commitdb import CommitDB

# Connect to server
db = CommitDB('localhost', 3306)
db.connect()

# Execute queries
db.execute("CREATE DATABASE myapp")
db.execute("CREATE TABLE myapp.users (id INT, name STRING)")
db.execute("INSERT INTO myapp.users VALUES (1, 'Alice')")

# Query data
result = db.execute("SELECT * FROM myapp.users")
print(result.rows)  # [{'id': 1, 'name': 'Alice'}]

db.close()
```

## Connection Options

### Basic Connection

```python
db = CommitDB('localhost', 3306)
db.connect()
```

### With SSL/TLS

```python
# SSL with certificate verification
db = CommitDB('localhost', 3306, use_ssl=True, ssl_ca_cert='ca.pem')
db.connect()

# SSL without verification (development only)
db = CommitDB('localhost', 3306, use_ssl=True, ssl_verify=False)
db.connect()
```

### With JWT Authentication

```python
db = CommitDB('localhost', 3306, jwt_token='eyJhbG...')
db.connect()
```

### Combined SSL + JWT

```python
db = CommitDB('localhost', 3306, 
              use_ssl=True, ssl_ca_cert='cert.pem',
              jwt_token='eyJhbG...')
db.connect()
```

## Executing Queries

```python
# Simple execute
result = db.execute("SELECT * FROM mydb.users")

# Access results
print(result.columns)  # ['id', 'name', 'email']
print(result.rows)     # [{'id': 1, 'name': 'Alice', 'email': 'alice@example.com'}]

# Get affected rows (for INSERT/UPDATE/DELETE)
result = db.execute("INSERT INTO mydb.users VALUES (2, 'Bob')")
print(result.affected_rows)  # 1
```

## Context Manager

```python
from commitdb import CommitDB

with CommitDB('localhost', 3306) as db:
    db.execute("SELECT * FROM myapp.users")
# Connection automatically closed
```

## Embedded Mode

For testing or development without a server:

```python
from commitdb import CommitDB

# In-memory embedded database
db = CommitDB.embedded()

# File-based embedded database
db = CommitDB.embedded(base_dir='/path/to/data')

db.execute("CREATE DATABASE test")
```

## Error Handling

```python
from commitdb import CommitDB, CommitDBError

try:
    db = CommitDB('localhost', 3306)
    db.connect()
    result = db.execute("SELECT * FROM nonexistent.table")
except CommitDBError as e:
    print(f"Database error: {e}")
finally:
    db.close()
```
