# Quick Start

This guide will get you running CommitDB in under 5 minutes.

## Install

```bash
go install github.com/nickyhof/CommitDB/v2/cmd/cli@latest
```

## Start the CLI

=== "In-Memory"

    ```bash
    commitdb
    ```

=== "File-Backed"

    ```bash
    commitdb -dir=/path/to/data
    ```

## Scripting

```bash
# Execute SQL directly
commitdb -e "SHOW DATABASES"

# Execute a SQL file
commitdb -dir ./mydata -f setup.sql

# Pipe via stdin
echo "SELECT * FROM mydb.users;" | commitdb -dir ./mydata
```

## Create and Query

```bash
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
- [Go API](go-api.md) - Embedding in Go applications
