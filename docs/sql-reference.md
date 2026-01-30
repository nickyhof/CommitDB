# SQL Reference

Complete SQL syntax reference for CommitDB.

## Data Definition

### Databases

```sql
CREATE DATABASE mydb;
DROP DATABASE mydb;
DROP DATABASE IF EXISTS mydb;  -- No error if database doesn't exist
SHOW DATABASES;
```

### Tables

```sql
CREATE TABLE mydb.users (
    id INT PRIMARY KEY,
    name STRING,
    email STRING,
    age INT,
    active BOOL,
    birth_date DATE,       -- Date only (YYYY-MM-DD)
    created TIMESTAMP,     -- Date + time (YYYY-MM-DD HH:MM:SS)
    metadata JSON          -- JSON object or array
);

DROP TABLE mydb.users;
DROP TABLE IF EXISTS mydb.users;  -- No error if table doesn't exist
SHOW TABLES IN mydb;
DESCRIBE mydb.users;
```

### Indexes

```sql
CREATE INDEX idx_name ON mydb.users(name);
CREATE UNIQUE INDEX idx_email ON mydb.users(email);
DROP INDEX idx_name ON mydb.users;
SHOW INDEXES ON mydb.users;
```

### Alter Table

```sql
ALTER TABLE mydb.users ADD COLUMN phone STRING;
ALTER TABLE mydb.users DROP COLUMN phone;
ALTER TABLE mydb.users MODIFY COLUMN name TEXT;
ALTER TABLE mydb.users RENAME COLUMN name TO username;
```

### Views

Views are virtual tables defined by a SELECT query. Materialized views cache the query results for faster access.

```sql
-- Create a view
CREATE VIEW mydb.active_users AS SELECT * FROM mydb.users WHERE active = 1;

-- Create a materialized view (caches results)
CREATE MATERIALIZED VIEW mydb.user_stats AS 
    SELECT city, COUNT(*) AS count FROM mydb.users GROUP BY city;

-- Query views like tables
SELECT * FROM mydb.active_users;
SELECT * FROM mydb.user_stats;

-- Refresh materialized view after underlying data changes
REFRESH VIEW mydb.user_stats;

-- Show views in database
SHOW VIEWS IN mydb;

-- Drop views (works for both regular and materialized)
DROP VIEW mydb.active_users;
DROP VIEW IF EXISTS mydb.user_stats;
```

## Data Manipulation

### Insert

```sql
-- Single row
INSERT INTO mydb.users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');

-- With timestamp
INSERT INTO mydb.users (id, name, created) VALUES (2, 'Bob', NOW());

-- Bulk insert (multiple rows)
INSERT INTO mydb.users (id, name, email) VALUES 
    (3, 'Charlie', 'charlie@example.com'),
    (4, 'Diana', 'diana@example.com'),
    (5, 'Eve', 'eve@example.com');
```

### Select

```sql
SELECT * FROM mydb.users;
SELECT name, email FROM mydb.users WHERE age > 25;
SELECT * FROM mydb.users ORDER BY name ASC LIMIT 10 OFFSET 5;
SELECT DISTINCT city FROM mydb.users;
```

### Update & Delete

```sql
UPDATE mydb.users SET name = 'Bob' WHERE id = 1;
DELETE FROM mydb.users WHERE id = 1;
```

## Queries

### WHERE Clauses

```sql
SELECT * FROM mydb.users WHERE age > 25;
SELECT * FROM mydb.users WHERE name = 'Alice' AND active = true;
SELECT * FROM mydb.users WHERE city IN ('NYC', 'LA', 'Chicago');
```

### ORDER BY, LIMIT, OFFSET

```sql
SELECT * FROM mydb.users ORDER BY created DESC;
SELECT * FROM mydb.users ORDER BY name ASC LIMIT 10;
SELECT * FROM mydb.users LIMIT 10 OFFSET 20;
```

### GROUP BY & HAVING

```sql
SELECT category, SUM(amount) FROM mydb.orders GROUP BY category;
SELECT city, COUNT(id) FROM mydb.users GROUP BY city HAVING COUNT(id) > 10;
```

## Aggregate Functions

| Function | Description |
|----------|-------------|
| `COUNT(*)` | Count rows |
| `SUM(column)` | Sum numeric values |
| `AVG(column)` | Average of numeric values |
| `MIN(column)` | Minimum value |
| `MAX(column)` | Maximum value |

## JOINs

```sql
-- Inner Join
SELECT * FROM mydb.orders 
INNER JOIN mydb.customers ON customer_id = id;

-- Left Join
SELECT o.id, c.name FROM mydb.orders o
LEFT JOIN mydb.customers c ON o.customer_id = c.id;

-- Right Join
SELECT * FROM mydb.products
RIGHT JOIN mydb.categories ON category_id = id;
```

## String Functions

| Function | Description |
|----------|-------------|
| `UPPER(str)` | Convert to uppercase |
| `LOWER(str)` | Convert to lowercase |
| `CONCAT(a, b, ...)` | Concatenate strings |
| `SUBSTRING(str, start, len)` | Extract substring (1-indexed) |
| `TRIM(str)` | Remove leading/trailing whitespace |
| `LENGTH(str)` | String length |
| `REPLACE(str, old, new)` | Replace occurrences |

```sql
SELECT UPPER(name) FROM mydb.users;
SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM mydb.users;
SELECT SUBSTRING(name, 1, 3) FROM mydb.users;
```

## Date Functions

| Function | Description |
|----------|-------------|
| `NOW()` | Current date/time |
| `DATE(timestamp)` | Extract date part |
| `YEAR(date)`, `MONTH(date)`, `DAY(date)` | Extract date components |
| `HOUR(ts)`, `MINUTE(ts)`, `SECOND(ts)` | Extract time components |
| `DATE_ADD(date, n, unit)` | Add interval (DAY, MONTH, YEAR, etc.) |
| `DATE_SUB(date, n, unit)` | Subtract interval |
| `DATEDIFF(date1, date2)` | Days between dates |
| `DATE_FORMAT(date, format)` | Format date |

```sql
SELECT NOW() FROM mydb.events;
SELECT YEAR(created_at), MONTH(created_at) FROM mydb.events;
SELECT DATE_ADD(created_at, 7, 'DAY') FROM mydb.events;
SELECT DATE_FORMAT(created_at, '%Y-%m-%d') FROM mydb.events;
```

## JSON Functions

| Function | Description |
|----------|-------------|
| `JSON_EXTRACT(json, path)` | Extract value using path (`$.key.nested`) |
| `JSON_KEYS(json)` | Get comma-separated keys |
| `JSON_LENGTH(json)` | Number of elements |
| `JSON_TYPE(json)` | Type (object, array, string, number, boolean, null) |
| `JSON_CONTAINS(json, value)` | Returns 1 if value exists |

```sql
SELECT JSON_EXTRACT(data, '$.name') FROM mydb.documents;
SELECT JSON_KEYS(data) FROM mydb.documents;
```

## Bulk Import/Export

```sql
-- Export table to CSV (local file)
COPY INTO '/path/to/users.csv' FROM mydb.users;
COPY INTO '/path/to/data.csv' FROM mydb.users WITH (HEADER = TRUE, DELIMITER = ',');

-- Import CSV into table (local file)
COPY INTO mydb.users FROM '/path/to/users.csv';
COPY INTO mydb.users FROM '/path/to/data.tsv' WITH (HEADER = TRUE, DELIMITER = '\t');

-- Import from HTTPS URL
COPY INTO mydb.users FROM 'https://example.com/data.csv';

-- Export to S3
COPY INTO 's3://bucket/path/file.csv' FROM mydb.users;
COPY INTO 's3://bucket/file.csv' FROM mydb.users WITH (AWS_REGION = 'us-east-1');

-- Import from S3
COPY INTO mydb.users FROM 's3://bucket/path/file.csv';
COPY INTO mydb.users FROM 's3://bucket/file.csv' WITH (
    AWS_KEY = 'AKIAIOSFODNN7EXAMPLE',
    AWS_SECRET = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    AWS_REGION = 'us-east-1'
);
```

**S3 Authentication:**
- Uses AWS environment variables by default (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`)
- Or specify credentials via `WITH` clause (AWS_KEY, AWS_SECRET, AWS_REGION)
- IAM roles work automatically on EC2/ECS/Lambda

## Shared Databases

Query external Git repositories without copying data:

```sql
-- Create a share from an external repository
CREATE SHARE external FROM 'https://github.com/company/data.git';

-- With SSH authentication
CREATE SHARE reports FROM 'git@github.com:company/reports.git'
    WITH SSH KEY '/path/to/key';

-- With token authentication
CREATE SHARE data FROM 'https://github.com/company/data.git'
    WITH TOKEN 'ghp_xxxxxxxxxxxx';

-- Query shared tables using 3-level naming
SELECT * FROM external.mydb.users;

-- JOIN local and shared tables
SELECT o.id, u.name 
FROM local.orders o 
JOIN external.customers.users u ON o.user_id = u.id;

-- Sync latest changes
SYNC SHARE external;

-- List shares
SHOW SHARES;

-- Remove a share
DROP SHARE external;
```

## Transactions

```sql
BEGIN;
-- ... operations ...
COMMIT;
-- or
ROLLBACK;
```
