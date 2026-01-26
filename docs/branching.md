# Branching & Merging

CommitDB uses Git for storage, giving you powerful version control features.

## Creating Branches

```sql
-- Create a new branch from current state
CREATE BRANCH feature_x

-- View all branches
SHOW BRANCHES
```

## Switching Branches

```sql
-- Switch to a branch
CHECKOUT feature_x

-- Switch back to main
CHECKOUT master
```

## Making Changes on a Branch

Changes on a branch are isolated until merged:

```sql
-- On feature_x branch
INSERT INTO mydb.users (id, name) VALUES (100, 'NewUser');
ALTER TABLE mydb.users ADD COLUMN email STRING;

-- These changes are NOT visible on master
```

## Merging

### Auto-Merge (Last-Writer-Wins)

```sql
-- Merge feature branch into current branch
MERGE feature_x
```

Conflicts are automatically resolved using Last-Writer-Wins strategy.

### Manual Conflict Resolution

```sql
-- Start merge with manual resolution
MERGE feature_x WITH MANUAL RESOLUTION

-- View pending conflicts
SHOW MERGE CONFLICTS

-- Resolve each conflict
RESOLVE CONFLICT mydb.users.1 USING HEAD    -- Keep current branch value
RESOLVE CONFLICT mydb.users.1 USING SOURCE  -- Keep feature branch value
RESOLVE CONFLICT mydb.users.1 USING '{"custom":"value"}'  -- Custom value

-- Complete the merge
COMMIT MERGE

-- Or abort
ABORT MERGE
```

## Snapshots & Restore

### Creating Snapshots

Snapshots are Git tags that mark a specific point in time:

```go
// Create a named snapshot at current state
persistence.Snapshot("v1.0.0", nil)

// Create a snapshot at a specific transaction
persistence.Snapshot("before-migration", &transaction)
```

### Restoring Data

```go
// Recover to a named snapshot (resets all data)
persistence.Recover("v1.0.0")

// Restore to a specific transaction
persistence.Restore(transaction, nil, nil)

// Restore only a specific database
db := "mydb"
persistence.Restore(transaction, &db, nil)

// Restore only a specific table
table := "users"
persistence.Restore(transaction, &db, &table)
```

## Viewing History

```sql
-- View transaction log
SHOW LOG

-- View log for specific database
SHOW LOG FOR mydb

-- View log for specific table
SHOW LOG FOR mydb.users
```
