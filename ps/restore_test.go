package ps

import (
	"testing"

	"github.com/nickyhof/CommitDB/core"
)

func TestSnapshot(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}

	// Create some data to snapshot
	_, err = persistence.CreateDatabase(core.Database{Name: "testdb"}, identity)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Create snapshot at HEAD
	err = persistence.Snapshot("v1.0.0", nil)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Verify tag exists by trying to recover to it
	err = persistence.Recover("v1.0.0")
	if err != nil {
		t.Fatalf("Failed to recover to snapshot: %v", err)
	}
}

func TestSnapshotAtTransaction(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}

	// Create database and get transaction
	txn, err := persistence.CreateDatabase(core.Database{Name: "testdb"}, identity)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Create more data
	_, err = persistence.CreateTable(core.Table{
		Database: "testdb",
		Name:     "users",
		Columns:  []core.Column{{Name: "id", Type: core.IntType}},
	}, identity)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create snapshot at specific transaction (before table was created)
	err = persistence.Snapshot("before-table", &txn)
	if err != nil {
		t.Fatalf("Failed to create snapshot at transaction: %v", err)
	}

	// Recover to the snapshot
	err = persistence.Recover("before-table")
	if err != nil {
		t.Fatalf("Failed to recover to snapshot: %v", err)
	}

	// Table should not exist after recovery
	_, err = persistence.GetTable("testdb", "users")
	if err == nil {
		t.Error("Expected table to not exist after recovery to pre-table snapshot")
	}
}

func TestRecover(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}

	// Create initial state
	_, err = persistence.CreateDatabase(core.Database{Name: "testdb"}, identity)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Snapshot initial state
	err = persistence.Snapshot("initial", nil)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Make changes
	_, err = persistence.CreateTable(core.Table{
		Database: "testdb",
		Name:     "users",
		Columns:  []core.Column{{Name: "id", Type: core.IntType}},
	}, identity)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Verify table exists
	_, err = persistence.GetTable("testdb", "users")
	if err != nil {
		t.Fatalf("Table should exist: %v", err)
	}

	// Recover to initial state
	err = persistence.Recover("initial")
	if err != nil {
		t.Fatalf("Failed to recover: %v", err)
	}

	// Table should be gone
	_, err = persistence.GetTable("testdb", "users")
	if err == nil {
		t.Error("Table should not exist after recovery")
	}
}

func TestRecoverNonExistentSnapshot(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	// Try to recover to non-existent snapshot
	err = persistence.Recover("nonexistent")
	if err == nil {
		t.Error("Expected error when recovering to non-existent snapshot")
	}
}

func TestRestore(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}

	// Create initial state and capture transaction
	initialTxn, err := persistence.CreateDatabase(core.Database{Name: "testdb"}, identity)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Make changes
	_, err = persistence.CreateTable(core.Table{
		Database: "testdb",
		Name:     "users",
		Columns:  []core.Column{{Name: "id", Type: core.IntType}},
	}, identity)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Verify table exists
	_, err = persistence.GetTable("testdb", "users")
	if err != nil {
		t.Fatalf("Table should exist: %v", err)
	}

	// Restore to initial transaction
	err = persistence.Restore(initialTxn, nil, nil)
	if err != nil {
		t.Fatalf("Failed to restore: %v", err)
	}

	// Table should be gone
	_, err = persistence.GetTable("testdb", "users")
	if err == nil {
		t.Error("Table should not exist after restore")
	}
}

func TestRestoreWithDatabaseFilter(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}

	// Create database and table
	_, err = persistence.CreateDatabase(core.Database{Name: "testdb"}, identity)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	tableTxn, err := persistence.CreateTable(core.Table{
		Database: "testdb",
		Name:     "users",
		Columns:  []core.Column{{Name: "id", Type: core.IntType}},
	}, identity)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Add record after table creation
	records := map[string][]byte{"1": []byte(`{"id":"1"}`)}
	_, err = persistence.SaveRecord("testdb", "users", records, identity)
	if err != nil {
		t.Fatalf("Failed to save record: %v", err)
	}

	// Restore to just after table creation (before record)
	db := "testdb"
	err = persistence.Restore(tableTxn, &db, nil)
	if err != nil {
		t.Fatalf("Failed to restore with database filter: %v", err)
	}

	// Record should not exist after restore
	_, exists := persistence.GetRecord("testdb", "users", "1")
	if exists {
		t.Error("Record should not exist after restore to pre-record transaction")
	}
}

func TestLatestTransaction(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}

	// Initially should be empty
	txn := persistence.LatestTransaction()
	if txn.Id != "" {
		t.Error("Expected empty transaction for fresh repo")
	}

	// Create a commit
	createdTxn, err := persistence.CreateDatabase(core.Database{Name: "testdb"}, identity)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Latest should match
	latestTxn := persistence.LatestTransaction()
	if latestTxn.Id != createdTxn.Id {
		t.Errorf("Expected latest transaction %s, got %s", createdTxn.Id, latestTxn.Id)
	}
}
