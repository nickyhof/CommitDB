// Package engine provides the SQL execution engine for CommitDB.
//
// The Engine type is the main entry point for executing SQL statements.
// It parses SQL, executes queries, and returns results.
//
// # Engine Usage
//
//	engine := instance.Engine(core.Identity{Name: "App", Email: "app@example.com"})
//	result, err := engine.Execute("SELECT * FROM mydb.users")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	result.Display()
//
// # Result Types
//
// There are two result types:
//   - QueryResult: Returned by SELECT statements
//   - CommitResult: Returned by INSERT, UPDATE, DELETE, CREATE, DROP
//
// QueryResult contains columns, data rows, and execution metrics.
// CommitResult contains counts of affected objects and the transaction ID.
package engine
