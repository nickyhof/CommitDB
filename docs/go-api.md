# Go API

[![Go Reference](https://pkg.go.dev/badge/github.com/nickyhof/CommitDB/v2.svg)](https://pkg.go.dev/github.com/nickyhof/CommitDB/v2)

Use CommitDB as an embedded library in your Go applications.

## Installation

```bash
go get github.com/nickyhof/CommitDB/v2
```

## Quick Start

```go
package main

import (
    "fmt"
    
    "github.com/nickyhof/CommitDB/v2"
    "github.com/nickyhof/CommitDB/v2/internal/core"
    "github.com/nickyhof/CommitDB/v2/internal/persistence"
)

func main() {
    // Create in-memory persistence
    p, _ := persistence.NewMemoryPersistence()
    
    // Or file-based persistence
    // p, _ := persistence.NewFilePersistence("/path/to/data", nil)
    
    // Open CommitDB instance
    instance := commitdb.Open(&p)
    
    // Create engine with identity (for Git commits)
    engine := instance.Engine(core.Identity{
        Name:  "MyApp",
        Email: "app@example.com",
    })
    
    // Execute queries
    engine.Execute("CREATE DATABASE myapp")
    engine.Execute("CREATE TABLE myapp.users (id INT PRIMARY KEY, name STRING)")
    engine.Execute("INSERT INTO myapp.users (id, name) VALUES (1, 'Alice')")
    
    // Query data
    result, err := engine.Execute("SELECT * FROM myapp.users")
    if err != nil {
        panic(err)
    }
    
    // Display results
    result.Display()
}
```

## Persistence Options

```go
import "github.com/nickyhof/CommitDB/v2/internal/persistence"

// In-memory (no file persistence)
p, _ := persistence.NewMemoryPersistence()

// File-based with Git persistence
p, _ := persistence.NewFilePersistence("/path/to/data", nil)

// Clone from remote Git repository
gitUrl := "https://github.com/user/repo.git"
p, _ := persistence.NewFilePersistence("/path/to/data", &gitUrl)
```

## Working with Results

```go
result, err := engine.Execute("SELECT id, name FROM myapp.users")
if err != nil {
    // Handle error
}

// Check result type
switch r := result.(type) {
case engine.QueryResult:
    // Column names
    for _, col := range r.Columns {
        fmt.Println(col)
    }
    
    // Row data (each row is []string)
    for _, row := range r.Data {
        fmt.Printf("ID: %s, Name: %s\n", row[0], row[1])
    }
    
    fmt.Printf("Read %d records in %.2fms\n", r.RecordsRead, r.ExecutionTimeMs)
    
case engine.CommitResult:
    fmt.Printf("Written: %d, Deleted: %d\n", r.RecordsWritten, r.RecordsDeleted)
}
```

## Thread Safety

The engine is thread-safe. Each connection should create its own engine instance from the shared persistence:

```go
instance := commitdb.Open(&p)

// Safe for concurrent use - each goroutine gets its own engine
go func() {
    e := instance.Engine(core.Identity{Name: "Worker1", Email: "w1@example.com"})
    result, _ := e.Execute("SELECT * FROM myapp.users")
}()

go func() {
    e := instance.Engine(core.Identity{Name: "Worker2", Email: "w2@example.com"})
    result, _ := e.Execute("SELECT * FROM myapp.orders")
}()
```

## Transactions

```go
// Start transaction
engine.Execute("BEGIN")

// Operations within transaction
engine.Execute("INSERT INTO myapp.users (id, name) VALUES (1, 'Alice')")
engine.Execute("INSERT INTO myapp.orders (id, user_id) VALUES (100, 1)")

// Commit or rollback
engine.Execute("COMMIT")
// or: engine.Execute("ROLLBACK")
```

## Branching

```go
// Create and switch to a new branch
engine.Execute("CREATE BRANCH feature-x")
engine.Execute("SWITCH BRANCH feature-x")

// Make changes on the branch
engine.Execute("INSERT INTO myapp.users (id, name) VALUES (2, 'Bob')")

// Switch back and merge
engine.Execute("SWITCH BRANCH main")
engine.Execute("MERGE BRANCH feature-x")
```
