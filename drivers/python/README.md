# CommitDB Python Driver

Python client for connecting to CommitDB.

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

# Or use context manager
with CommitDB('localhost', 3306) as db:
    # Create database and table
    db.execute('CREATE DATABASE mydb')
    db.execute('CREATE TABLE mydb.users (id INT PRIMARY KEY, name STRING)')

    # Insert data
    db.execute("INSERT INTO mydb.users (id, name) VALUES (1, 'Alice')")
    db.execute("INSERT INTO mydb.users (id, name) VALUES (2, 'Bob')")

    # Query data
    result = db.query('SELECT * FROM mydb.users')
    print(f"Columns: {result.columns}")
    print(f"Rows: {len(result)}")

    # Iterate over rows as dictionaries
    for row in result:
        print(f"  {row['id']}: {row['name']}")

    # Use convenience methods
    db.insert('mydb', 'users', ['id', 'name'], [3, 'Charlie'])
    tables = db.show_tables('mydb')
```

## API Reference

### CommitDB

```python
CommitDB(host='localhost', port=3306)
```

**Methods:**

- `connect(timeout=10.0)` - Connect to server
- `close()` - Close connection
- `execute(sql)` - Execute any SQL query
- `query(sql)` - Execute SELECT query, returns QueryResult
- `create_database(name)` - Create a database
- `drop_database(name)` - Drop a database
- `create_table(database, table, columns)` - Create a table
- `insert(database, table, columns, values)` - Insert a row
- `show_databases()` - List databases
- `show_tables(database)` - List tables

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

