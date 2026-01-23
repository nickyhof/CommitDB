// Package ps provides the persistence layer for CommitDB.
//
// The persistence layer is backed by Git, using go-git for storage.
// Every write operation creates a Git commit, providing full version
// control and history tracking.
//
// # Memory Persistence
//
// For testing or ephemeral databases:
//
//	persistence, err := ps.NewMemoryPersistence()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// # File Persistence
//
// For persistent storage:
//
//	persistence, err := ps.NewFilePersistence("/path/to/data", nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// # Transaction Batching
//
// For improved write performance, use TransactionBuilder:
//
//	txn, _ := persistence.BeginTransaction()
//	txn.AddWrite("db", "table", "key1", data1)
//	txn.AddWrite("db", "table", "key2", data2)
//	result, _ := txn.Commit(identity)
//
// # Indexing
//
// The persistence layer supports B-tree indexes for faster queries:
//
//	im := ps.NewIndexManager(persistence)
//	im.CreateIndex("idx_name", "db", "table", "column", false)
//	keys := im.Lookup("db", "table", "column", "value")
package ps
