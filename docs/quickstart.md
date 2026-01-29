# Quick Start

This guide will get you running CommitDB in under 5 minutes.

## Start the Server

=== "Docker"

    ```bash
    docker run -p 3306:3306 ghcr.io/nickyhof/commitdb:latest
    ```

=== "Binary"

    ```bash
    ./commitdb-server -port 3306 -baseDir=/path/to/data
    ```

## Connect and Query

=== "Python"

    ```python
    from commitdb import CommitDB
    
    db = CommitDB('localhost', 3306)
    db.connect()
    
    # Create database and table
    db.execute("CREATE DATABASE myapp")
    db.execute("CREATE TABLE myapp.users (id INT PRIMARY KEY, name STRING, email STRING)")
    
    # Insert data
    db.execute("INSERT INTO myapp.users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')")
    
    # Query data
    result = db.execute("SELECT * FROM myapp.users")
    print(result.rows)  # [{'id': 1, 'name': 'Alice', 'email': 'alice@example.com'}]
    
    db.close()
    ```

=== "netcat"

    ```bash
    echo "CREATE DATABASE myapp" | nc localhost 3306
    echo "CREATE TABLE myapp.users (id INT, name STRING)" | nc localhost 3306
    echo "INSERT INTO myapp.users VALUES (1, 'Alice')" | nc localhost 3306
    echo "SELECT * FROM myapp.users" | nc localhost 3306
    ```

=== "CLI"

    ```bash
    ./commitdb-cli

    commitdb> CREATE DATABASE myapp;
    commitdb> CREATE TABLE myapp.users (id INT, name STRING);
    commitdb> INSERT INTO myapp.users (id, name) VALUES (1, 'Alice');
    commitdb> SELECT * FROM myapp.users;
    +----+-------+
    | id | name  |
    +----+-------+
    | 1  | Alice |
    +----+-------+
    ```

## Use Git Features

```sql
-- Create a branch
CREATE BRANCH feature_x

-- Switch to it
CHECKOUT feature_x

-- Make changes
INSERT INTO myapp.users VALUES (2, 'Bob')

-- Switch back and merge
CHECKOUT master
MERGE feature_x
```

## Next Steps

- [SQL Reference](sql-reference.md) - Full SQL syntax
- [Branching & Merging](branching.md) - Version control features
- [Python Client](python-client.md) - Python client library
