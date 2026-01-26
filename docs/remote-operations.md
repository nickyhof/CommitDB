# Remote Operations

CommitDB can sync with remote Git repositories (GitHub, GitLab, etc.).

## Configuring Remotes

```sql
-- Add a remote
REMOTE ADD origin https://github.com/user/repo.git

-- Add with authentication
REMOTE ADD origin https://github.com/user/repo.git WITH TOKEN 'ghp_xxx'
REMOTE ADD origin git@github.com:user/repo.git WITH SSH '/path/to/key'

-- Remove a remote
REMOTE REMOVE origin

-- View remotes
SHOW REMOTES
```

## Authentication Methods

=== "Token (HTTPS)"

    ```sql
    REMOTE ADD origin https://github.com/user/repo.git WITH TOKEN 'ghp_xxx'
    ```

=== "SSH Key"

    ```sql
    REMOTE ADD origin git@github.com:user/repo.git WITH SSH '/path/to/id_rsa'
    ```

=== "Basic Auth"

    ```sql
    REMOTE ADD origin https://github.com/user/repo.git WITH AUTH 'username:password'
    ```

## Push & Pull

```sql
-- Push current branch to remote
PUSH origin master

-- Pull updates from remote
PULL origin master

-- Fetch without merging
FETCH origin
```

## Sync Workflow Example

```sql
-- Initial setup
REMOTE ADD origin https://github.com/user/mydb.git WITH TOKEN 'ghp_xxx'

-- Push local changes
PUSH origin master

-- Later, pull updates
PULL origin master

-- Work on a feature
CREATE BRANCH feature
CHECKOUT feature
-- ... make changes ...
CHECKOUT master
MERGE feature

-- Push the merged changes
PUSH origin master
```

## Go API

```go
// Configure remote
persistence.AddRemote("origin", "https://github.com/user/repo.git", &ps.RemoteAuth{
    Token: "ghp_xxx",
})

// Push
persistence.Push("origin", "master")

// Pull
persistence.Pull("origin", "master")

// Fetch
persistence.Fetch("origin")
```
