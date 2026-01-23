package ps

import (
	"testing"

	"github.com/nickyhof/CommitDB/core"
)

func TestTransactionBuilder(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}

	// Setup database and table
	persistence.CreateDatabase(core.Database{Name: "testdb"}, identity)
	persistence.CreateTable(core.Table{
		Database: "testdb",
		Name:     "users",
		Columns:  []core.Column{{Name: "id", Type: core.IntType}},
	}, identity)

	// Begin transaction
	txn, err := persistence.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Add writes
	err = txn.AddWrite("testdb", "users", "1", []byte(`{"id":"1"}`))
	if err != nil {
		t.Fatalf("Failed to add write: %v", err)
	}

	err = txn.AddWrite("testdb", "users", "2", []byte(`{"id":"2"}`))
	if err != nil {
		t.Fatalf("Failed to add write: %v", err)
	}

	if txn.OperationCount() != 2 {
		t.Errorf("Expected 2 operations, got %d", txn.OperationCount())
	}

	// Commit
	result, err := txn.Commit(identity)
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	if result.Id == "" {
		t.Error("Expected transaction ID to be set")
	}

	// Verify records exist
	_, exists := persistence.GetRecord("testdb", "users", "1")
	if !exists {
		t.Error("Expected record 1 to exist after commit")
	}

	_, exists = persistence.GetRecord("testdb", "users", "2")
	if !exists {
		t.Error("Expected record 2 to exist after commit")
	}
}

func TestTransactionBuilderRollback(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	// Begin transaction
	txn, err := persistence.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Add write
	txn.AddWrite("testdb", "users", "1", []byte(`{"id":"1"}`))

	// Rollback
	txn.Rollback()

	if txn.OperationCount() != 0 {
		t.Error("Expected 0 operations after rollback")
	}
}

func TestTransactionBuilderDelete(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}

	// Setup
	persistence.CreateDatabase(core.Database{Name: "testdb"}, identity)
	persistence.CreateTable(core.Table{
		Database: "testdb",
		Name:     "users",
		Columns:  []core.Column{{Name: "id"}},
	}, identity)
	persistence.SaveRecord("testdb", "users", map[string][]byte{"1": []byte(`{"id":"1"}`)}, identity)

	// Begin transaction and add delete
	txn, err := persistence.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	err = txn.AddDelete("testdb", "users", "1")
	if err != nil {
		t.Fatalf("Failed to add delete: %v", err)
	}

	// Commit
	_, err = txn.Commit(identity)
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Verify deleted
	_, exists := persistence.GetRecord("testdb", "users", "1")
	if exists {
		t.Error("Expected record to be deleted after commit")
	}
}

func TestTransactionBuilderEmptyCommit(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}

	txn, err := persistence.BeginTransaction()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Commit with no operations should fail
	_, err = txn.Commit(identity)
	if err == nil {
		t.Error("Expected error when committing empty transaction")
	}
}

func TestTransactionBuilderNotStarted(t *testing.T) {
	txn := &TransactionBuilder{}

	err := txn.AddWrite("db", "table", "key", []byte("data"))
	if err == nil {
		t.Error("Expected error when adding to unstarted transaction")
	}

	err = txn.AddDelete("db", "table", "key")
	if err == nil {
		t.Error("Expected error when deleting from unstarted transaction")
	}
}
