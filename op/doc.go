// Package op provides high-level operations for working with CommitDB databases and tables.
//
// The op package sits between the SQL engine (db/) and the persistence layer (ps/),
// providing convenient abstractions for common database operations.
//
// # DatabaseOp
//
// DatabaseOp wraps database-level operations:
//
//	dbOp, err := op.GetDatabase("mydb", persistence)
//	tables := dbOp.TableNames()           // List all tables
//	dbOp.DropDatabase(identity)           // Drop database
//	dbOp.Restore(transaction)             // Restore to point in time
//
// # TableOp
//
// TableOp wraps table-level operations for CRUD and scanning:
//
//	tableOp, err := op.GetTable("mydb", "users", persistence)
//
//	// Read operations
//	value, exists := tableOp.Get("key")           // Get raw bytes
//	str, exists := tableOp.GetString("key")       // Get as string
//	num, exists, _ := tableOp.GetInt("key")       // Get as int
//	keys := tableOp.Keys()                        // List all keys
//	count := tableOp.Count()                      // Count records
//
//	// Write operations
//	tableOp.Put("key", []byte("value"), identity)
//	tableOp.PutAll(map[string][]byte{...}, identity)
//	tableOp.Delete("key", identity)
//
//	// Scanning with optional filter
//	for key, value := range tableOp.Scan() {
//	    // process all records
//	}
//	for key, value := range tableOp.ScanWithFilter(func(k string, v []byte) bool {
//	    return strings.HasPrefix(k, "user_")
//	}) {
//	    // process filtered records
//	}
//
// # Architecture
//
// The layering is:
//
//	SQL Parser (sql/)
//	     ↓
//	SQL Engine (db/)
//	     ↓
//	Operations (op/)     ← This package
//	     ↓
//	Persistence (ps/)
//	     ↓
//	Git Storage (go-git)
package op
