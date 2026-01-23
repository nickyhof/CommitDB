# CommitDB Python Driver

Python client for connecting to CommitDB SQL Server.

## Installation

```bash
pip install -e drivers/python
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
