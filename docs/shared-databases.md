# Shared Databases

Shared databases allow you to query data from external Git repositories without copying the data into your local database. This enables cross-repository JOINs and read-only access to external datasets.

## Overview

A **share** is a read-only reference to an external CommitDB repository. Once created, you can:

- Query tables from the share using 3-level naming: `share.database.table`
- JOIN local tables with shared tables
- Sync the share to pull the latest changes

## Creating a Share

```sql
CREATE SHARE external FROM 'https://github.com/company/shared-data.git';

-- With authentication (SSH)
CREATE SHARE reports FROM 'git@github.com:company/reports.git'
    WITH SSH KEY '/path/to/key';

-- With SSH and passphrase
CREATE SHARE reports FROM 'git@github.com:company/reports.git'
    WITH SSH KEY '/path/to/key' PASSPHRASE 'mypassphrase';

-- With token authentication (GitHub, GitLab, etc.)
CREATE SHARE data FROM 'https://github.com/company/data.git'
    WITH TOKEN 'ghp_xxxxxxxxxxxx';
```

## Querying Shared Data

Use 3-level naming to access shared tables:

```sql
-- Query a shared table directly
SELECT * FROM external.mydb.users;

-- Join local data with shared data
SELECT o.id, o.product, u.name 
FROM local.orders o 
JOIN external.customers.users u ON o.user_id = u.id;
```

!!! note "Read-Only Access"
    Shares are read-only. You cannot INSERT, UPDATE, or DELETE data in a shared database.

## Syncing Shares

Pull the latest changes from the remote repository:

```sql
SYNC SHARE external;

-- With authentication if required
SYNC SHARE reports WITH SSH KEY '/path/to/key';
SYNC SHARE analytics WITH TOKEN 'ghp_xxxxxxxxxxxx';
```

## Managing Shares

```sql
-- List all shares
SHOW SHARES;

-- Remove a share
DROP SHARE external;
```

## Use Cases

### 1. Centralized Reference Data

Maintain common lookup tables (countries, currencies, categories) in a shared repository:

```sql
CREATE SHARE reference FROM 'https://github.com/company/reference-data.git';

SELECT p.name, c.category_name
FROM local.products p
JOIN reference.catalog.categories c ON p.category_id = c.id;
```

### 2. Cross-Team Data Access

Query data from another team's repository without duplicating it:

```sql
CREATE SHARE analytics FROM 'https://github.com/analytics-team/metrics.git';

SELECT * FROM analytics.reports.monthly_summary
WHERE month = '2024-01';
```

### 3. External Datasets

Access public or partner datasets:

```sql
CREATE SHARE public_data FROM 'https://github.com/public-datasets/census.git';

SELECT * FROM public_data.census.population
WHERE state = 'California';
```

## Python Client

```python
from commitdb import CommitDB

db = CommitDB('localhost', 3306)
db.connect()

# Create a share
db.execute("CREATE SHARE external FROM 'https://github.com/company/data.git'")

# Query shared data
result = db.execute("SELECT * FROM external.mydb.users")
print(result.rows)

# Join with local data
result = db.execute("""
    SELECT o.id, u.name 
    FROM local.orders o 
    JOIN external.customers.users u ON o.user_id = u.id
""")

# Sync latest changes
db.execute("SYNC SHARE external")
```

## Architecture

Shares are stored in `.shares/` within your database directory:

```
/your-database/
├── .git/
├── .commitdb/
│   └── shares.json      # Share metadata
├── .shares/
│   ├── external/        # Cloned share repository
│   └── analytics/       # Another share
└── mydb/                # Your local database
```

Each share is a full Git clone, allowing efficient syncing with `git pull`.
