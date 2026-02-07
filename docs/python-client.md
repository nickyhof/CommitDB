# Python Client

[![PyPI version](https://badge.fury.io/py/commitdb.svg)](https://pypi.org/project/commitdb/)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

The CommitDB Python client provides a native Python interface for connecting to CommitDB servers.

## Installation

```bash
pip install commitdb
```


## Quick Start

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

## API Reference

### CommitDB

```python
CommitDB(host='localhost', port=3306, use_ssl=False, ssl_verify=True, 
         ssl_ca_cert=None, jwt_token=None)
```

**Methods:**

| Method | Description |
|--------|-------------|
| `connect(timeout=10.0)` | Connect to server |
| `close()` | Close connection |
| `authenticate_jwt(token)` | Authenticate with JWT token |
| `execute(sql)` | Execute any SQL query |
| `query(sql)` | Execute SELECT, returns QueryResult |
| `create_database(name)` | Create a database |
| `drop_database(name)` | Drop a database |
| `create_table(db, table, columns)` | Create a table |
| `insert(db, table, columns, values)` | Insert a row |
| `show_databases()` | List databases |
| `show_tables(database)` | List tables |


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
result.records_written    # 1
result.affected_rows      # 1
result.execution_time_ms  # 1.5
result.execution_ops      # 1
```

## SSL/TLS Encryption

```python
from commitdb import CommitDB

# SSL with certificate verification (production)
db = CommitDB('localhost', 3306, use_ssl=True, ssl_ca_cert='cert.pem')
db.connect()

# SSL without verification (development only)
db = CommitDB('localhost', 3306, use_ssl=True, ssl_verify=False)
db.connect()

# SSL + JWT authentication
db = CommitDB('localhost', 3306,
              use_ssl=True, ssl_ca_cert='cert.pem',
              jwt_token='eyJhbG...')
db.connect()
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `use_ssl` | `False` | Enable SSL/TLS encryption |
| `ssl_verify` | `True` | Verify server certificate |
| `ssl_ca_cert` | `None` | Path to CA certificate |

## JWT Authentication

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
```

## Branching & Merging

```python
from commitdb import CommitDB

with CommitDB('localhost', 3306) as db:
    db.execute('CREATE DATABASE mydb')
    db.execute('CREATE TABLE mydb.users (id INT, name STRING)')
    db.execute("INSERT INTO mydb.users VALUES (1, 'Alice')")
    
    # Create and switch to branch
    db.execute('CREATE BRANCH feature')
    db.execute('CHECKOUT feature')
    
    # Make changes (only visible on this branch)
    db.execute("INSERT INTO mydb.users VALUES (2, 'Bob')")
    
    # Switch back and merge
    db.execute('CHECKOUT master')
    db.execute('MERGE feature')
    
    # Now master has both rows
    result = db.query('SELECT * FROM mydb.users')
    print(f"Users: {len(result)}")  # 2
```

### Manual Conflict Resolution

```python
# Merge with manual resolution
db.execute('MERGE feature WITH MANUAL RESOLUTION')

# View conflicts
conflicts = db.query('SHOW MERGE CONFLICTS')
for c in conflicts:
    print(f"Conflict: {c['Database']}.{c['Table']}.{c['Key']}")

# Resolve each conflict
db.execute('RESOLVE CONFLICT mydb.users.1 USING HEAD')    # Keep current
db.execute('RESOLVE CONFLICT mydb.users.2 USING SOURCE')  # Keep feature
db.execute("RESOLVE CONFLICT mydb.users.3 USING '{...}'") # Custom value

# Complete the merge
db.execute('COMMIT MERGE')
```

## Remote Operations

```python
# Add a remote
db.execute("CREATE REMOTE origin 'https://github.com/user/repo.git'")

# Push with authentication
db.execute("PUSH WITH TOKEN 'ghp_xxxxxxxxxxxx'")
db.execute("PUSH WITH SSH KEY '/path/to/id_rsa'")

# Pull
db.execute("PULL FROM origin")
```

## Bulk Import/Export

```python
from commitdb import CommitDB

with CommitDB('localhost', 3306) as db:
    # Import from HTTPS URL
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

## Error Handling

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

