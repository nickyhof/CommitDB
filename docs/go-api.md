# Go API

Use CommitDB as an embedded library in your Go applications.

## Installation

```bash
go get github.com/nickyhof/CommitDB
```

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/nickyhof/CommitDB/op"
)

func main() {
    // Create an in-memory engine
    engine := op.NewEngine(op.Memory, "")
    
    // Or file-based persistence
    // engine := op.NewEngine(op.FileMode, "/path/to/data")
    
    // Execute queries
    engine.Query("CREATE DATABASE myapp")
    engine.Query("CREATE TABLE myapp.users (id INT, name STRING)")
    engine.Query("INSERT INTO myapp.users VALUES (1, 'Alice')")
    
    // Query data
    result, err := engine.Query("SELECT * FROM myapp.users")
    if err != nil {
        panic(err)
    }
    
    fmt.Println(result.Columns) // [id name]
    fmt.Println(result.Rows)    // [[1 Alice]]
}
```

## Engine Modes

```go
// In-memory (no persistence)
engine := op.NewEngine(op.Memory, "")

// File-based with Git persistence
engine := op.NewEngine(op.FileMode, "/path/to/data")
```

## Working with Results

```go
result, err := engine.Query("SELECT id, name FROM myapp.users")
if err != nil {
    // Handle error
}

// Column names
for _, col := range result.Columns {
    fmt.Println(col)
}

// Row data
for _, row := range result.Rows {
    id := row[0].(int64)
    name := row[1].(string)
    fmt.Printf("User %d: %s\n", id, name)
}

// Affected rows (for INSERT/UPDATE/DELETE)
fmt.Println(result.AffectedRows)
```

## Persistence Layer

For direct access to Git-backed storage:

```go
import "github.com/nickyhof/CommitDB/ps"

// Create persistence
persistence := ps.NewPersistence("/path/to/data")

// Branching
persistence.CreateBranch("feature-x")
persistence.Checkout("feature-x")

// Snapshots
persistence.Snapshot("v1.0.0", nil)
persistence.Recover("v1.0.0")

// Remotes
persistence.AddRemote("origin", "https://github.com/user/repo.git", &ps.RemoteAuth{
    Token: "ghp_xxx",
})
persistence.Push("origin", "master")
persistence.Pull("origin", "master")
```

## Thread Safety

The engine is thread-safe using RWMutex:

```go
engine := op.NewEngine(op.FileMode, "/path/to/data")

// Safe for concurrent reads
go func() {
    result, _ := engine.Query("SELECT * FROM myapp.users")
}()

go func() {
    result, _ := engine.Query("SELECT * FROM myapp.orders")
}()
```

## Transactions

```go
// Start transaction
engine.Query("BEGIN")

// Operations
engine.Query("INSERT INTO myapp.users VALUES (1, 'Alice')")
engine.Query("INSERT INTO myapp.orders VALUES (100, 1)")

// Commit or rollback
engine.Query("COMMIT")
// or: engine.Query("ROLLBACK")
```
