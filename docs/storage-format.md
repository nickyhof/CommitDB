# Storage Format Specification

CommitDB stores all data as files in a standard Git repository. Every transaction is a Git commit, and every record is a Git blob. This document describes the file and directory layout so that external tools can read and write CommitDB repositories directly using standard Git tools.

## Repository Structure

```
<repo-root>/
├── mydb.database                     # Database metadata
├── mydb/                             # Database directory
│   ├── users.table                   # Table schema
│   ├── users/                        # Table data directory
│   │   ├── 1                         # Record (primary key = "1")
│   │   ├── 2                         # Record (primary key = "2")
│   │   └── alice@example.com         # Record (primary key = "alice@example.com")
│   ├── users.index.email             # Index on "email" column
│   └── orders.table                  # Another table schema
├── .commitdb/                        # CommitDB metadata
│   ├── views/
│   │   └── mydb/
│   │       └── active_users.json     # View definition
│   ├── materialized/
│   │   └── mydb/
│   │       └── summary/
│   │           └── data.json         # Materialized view cache
│   └── shares.json                   # Shared database references
└── .git/                             # Standard Git directory
```

## Databases

**Path:** `{database}.database`

A JSON file at the repository root containing database metadata.

```json
{"name": "mydb"}
```

Databases are discovered by listing root-level `.database` files or directories.

## Tables

### Schema

**Path:** `{database}/{table}.table`

A JSON file defining the table schema.

```json
{
  "database": "mydb",
  "name": "users",
  "columns": [
    {"name": "id", "type": 1, "primaryKey": true},
    {"name": "name", "type": 0, "primaryKey": false},
    {"name": "email", "type": 0, "primaryKey": false},
    {"name": "age", "type": 1, "primaryKey": false}
  ]
}
```

**Column types** (integer enum):

| Value | Type | SQL Name |
|-------|------|----------|
| 0 | String | `STRING` |
| 1 | Int | `INT`, `INTEGER` |
| 2 | Float | `FLOAT`, `DOUBLE`, `REAL` |
| 3 | Bool | `BOOL`, `BOOLEAN` |
| 4 | Text | `TEXT` |
| 5 | Date | `DATE` |
| 6 | Timestamp | `TIMESTAMP`, `DATETIME` |
| 7 | JSON | `JSON` |

Exactly one column must have `"primaryKey": true`.

### Records

**Path:** `{database}/{table}/{primary_key}`

Each record is a file named by its primary key value, containing a JSON object mapping column names to string values.

```json
{"id": "1", "name": "Alice", "email": "alice@example.com", "age": "30"}
```

> **Note:** All values are stored as strings regardless of column type. Type interpretation is done at query time using the table schema.

## Indexes

**Path:** `{database}/{table}.index.{column}`

A JSON file containing a B-tree-style index mapping column values to lists of primary keys.

```json
{
  "name": "idx_email",
  "database": "mydb",
  "table": "users",
  "column": "email",
  "unique": true,
  "entries": {
    "alice@example.com": ["1"],
    "bob@example.com": ["2"]
  }
}
```

## Views

### View Definitions

**Path:** `.commitdb/views/{database}/{view_name}.json`

```json
{
  "database": "mydb",
  "name": "active_users",
  "query": "SELECT * FROM mydb.users WHERE active = 'true'",
  "materialized": false,
  "columns": [],
  "created_at": "2026-02-08T22:00:00Z",
  "updated_at": "2026-02-08T22:00:00Z"
}
```

### Materialized View Data

**Path:** `.commitdb/materialized/{database}/{view_name}/data.json`

A JSON array of row objects representing the cached query result.

```json
[
  {"id": "1", "name": "Alice", "active": "true"},
  {"id": "3", "name": "Charlie", "active": "true"}
]
```

## Shares

**Path:** `.commitdb/shares.json`

Shared database references for querying external Git repositories.

```json
{
  "shares": [
    {
      "name": "external",
      "url": "https://github.com/org/data.git"
    }
  ]
}
```

Cloned share repositories are stored in `.shares/` (excluded from database listing).

## Transactions

Every write operation creates a Git commit. The commit metadata serves as the transaction record:

- **Commit hash** → Transaction ID
- **Author name/email** → Identity that performed the operation
- **Timestamp** → When the transaction occurred
- **Message** → Description of the operation

## Branches

CommitDB branches map directly to Git branches. `CREATE BRANCH`, `CHECKOUT`, and `MERGE` operations use standard Git refs under `refs/heads/`.

## Reading a CommitDB Repo with Git

You can read any CommitDB repository using standard Git commands:

```bash
# List databases
ls *.database

# Read a table schema
cat mydb/users.table | jq .

# Read a record
cat mydb/users/1 | jq .

# List all records in a table
ls mydb/users/

# View transaction history
git log --oneline

# Query a table at a specific point in time
git show <commit>:mydb/users/1

# Compare table state across branches
git diff main..feature -- mydb/users/
```
