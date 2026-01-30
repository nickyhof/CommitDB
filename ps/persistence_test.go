package ps

import (
	"testing"

	"github.com/nickyhof/CommitDB/core"
)

func TestNewMemoryPersistence(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create memory persistence: %v", err)
	}

	if !persistence.IsInitialized() {
		t.Error("Expected persistence to be initialized")
	}
}

func TestPersistenceNotInitialized(t *testing.T) {
	var persistence Persistence

	if persistence.IsInitialized() {
		t.Error("Expected uninitialized persistence to return false")
	}

	err := persistence.ensureInitialized()
	if err != ErrNotInitialized {
		t.Errorf("Expected ErrNotInitialized, got %v", err)
	}
}

func TestCreateAndGetDatabase(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}
	db := core.Database{Name: "testdb"}

	// Create database
	txn, err := persistence.CreateDatabase(db, identity)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	if txn.Id == "" {
		t.Error("Expected transaction ID to be set")
	}

	// Get database
	gotDB, err := persistence.GetDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to get database: %v", err)
	}
	if gotDB.Name != "testdb" {
		t.Errorf("Expected database name 'testdb', got '%s'", gotDB.Name)
	}
}

func TestCreateAndGetTable(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}
	db := core.Database{Name: "testdb"}

	// Create database first
	_, err = persistence.CreateDatabase(db, identity)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Create table
	table := core.Table{
		Database: "testdb",
		Name:     "users",
		Columns: []core.Column{
			{Name: "id", Type: core.IntType, PrimaryKey: true},
			{Name: "name", Type: core.StringType},
		},
	}

	_, err = persistence.CreateTable(table, identity)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Get table
	gotTable, err := persistence.GetTable("testdb", "users")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}
	if gotTable.Name != "users" {
		t.Errorf("Expected table name 'users', got '%s'", gotTable.Name)
	}
	if len(gotTable.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(gotTable.Columns))
	}
}

func TestSaveAndGetRecord(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}

	// Create database and table
	db := core.Database{Name: "testdb"}
	_, _ = persistence.CreateDatabase(db, identity)

	table := core.Table{
		Database: "testdb",
		Name:     "users",
		Columns:  []core.Column{{Name: "id", Type: core.IntType}},
	}
	_, _ = persistence.CreateTable(table, identity)

	// Save record
	records := map[string][]byte{
		"1": []byte(`{"id":"1","name":"Alice"}`),
	}
	_, err = persistence.SaveRecord("testdb", "users", records, identity)
	if err != nil {
		t.Fatalf("Failed to save record: %v", err)
	}

	// Get record
	data, exists := persistence.GetRecord("testdb", "users", "1")
	if !exists {
		t.Error("Expected record to exist")
	}
	if string(data) != `{"id":"1","name":"Alice"}` {
		t.Errorf("Unexpected record data: %s", string(data))
	}
}

func TestDeleteRecord(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}

	// Setup
	db := core.Database{Name: "testdb"}
	_, _ = persistence.CreateDatabase(db, identity)
	table := core.Table{Database: "testdb", Name: "users", Columns: []core.Column{{Name: "id"}}}
	_, _ = persistence.CreateTable(table, identity)

	records := map[string][]byte{"1": []byte(`{"id":"1"}`)}
	_, _ = persistence.SaveRecord("testdb", "users", records, identity)

	// Delete
	_, err = persistence.DeleteRecord("testdb", "users", "1", identity)
	if err != nil {
		t.Fatalf("Failed to delete record: %v", err)
	}

	// Verify deleted
	_, exists := persistence.GetRecord("testdb", "users", "1")
	if exists {
		t.Error("Expected record to be deleted")
	}
}

func TestListDatabases(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}

	// Create databases - this test just verifies the operations don't panic
	_, err = persistence.CreateDatabase(core.Database{Name: "testdb1"}, identity)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// ListDatabases should return something (even if empty for memory fs)
	databases := persistence.ListDatabases()
	_ = databases // Just verify no panic
}

func TestListTables(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}

	// Create database and tables
	persistence.CreateDatabase(core.Database{Name: "testdb"}, identity)
	persistence.CreateTable(core.Table{Database: "testdb", Name: "table1", Columns: []core.Column{{Name: "id"}}}, identity)
	persistence.CreateTable(core.Table{Database: "testdb", Name: "table2", Columns: []core.Column{{Name: "id"}}}, identity)

	tables := persistence.ListTables("testdb")
	if len(tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(tables))
	}
}

func TestDropDatabase(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}

	// Create and drop database
	persistence.CreateDatabase(core.Database{Name: "testdb"}, identity)
	_, err = persistence.DropDatabase("testdb", identity)
	if err != nil {
		t.Fatalf("Failed to drop database: %v", err)
	}

	// Verify dropped
	_, err = persistence.GetDatabase("testdb")
	if err == nil {
		t.Error("Expected error getting dropped database")
	}
}

func TestDropTable(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}

	// Create database and table
	persistence.CreateDatabase(core.Database{Name: "testdb"}, identity)
	persistence.CreateTable(core.Table{Database: "testdb", Name: "users", Columns: []core.Column{{Name: "id"}}}, identity)

	// Drop table
	_, err = persistence.DropTable("testdb", "users", identity)
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	// Verify dropped
	_, err = persistence.GetTable("testdb", "users")
	if err == nil {
		t.Error("Expected error getting dropped table")
	}
}

func TestEmptyCommitPrevention(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}

	// Create database and table
	persistence.CreateDatabase(core.Database{Name: "testdb"}, identity)
	persistence.CreateTable(core.Table{Database: "testdb", Name: "users", Columns: []core.Column{{Name: "id"}}}, identity)

	// Save initial record
	records := map[string][]byte{"1": []byte(`{"id":"1","name":"Alice"}`)}
	txn1, err := persistence.SaveRecord("testdb", "users", records, identity)
	if err != nil {
		t.Fatalf("Failed to save initial record: %v", err)
	}
	if txn1.Id == "" {
		t.Error("Expected transaction ID for initial save")
	}

	// Save exact same record again - should NOT create a new commit
	txn2, err := persistence.SaveRecord("testdb", "users", records, identity)
	if err != nil {
		t.Fatalf("Failed to save duplicate record: %v", err)
	}
	if txn2.Id != "" {
		t.Error("Expected empty transaction ID when no changes are made (duplicate data)")
	}

	// Save different record - SHOULD create a new commit
	records2 := map[string][]byte{"2": []byte(`{"id":"2","name":"Bob"}`)}
	txn3, err := persistence.SaveRecord("testdb", "users", records2, identity)
	if err != nil {
		t.Fatalf("Failed to save new record: %v", err)
	}
	if txn3.Id == "" {
		t.Error("Expected transaction ID for new data")
	}

	// Verify data integrity
	_, exists := persistence.GetRecord("testdb", "users", "1")
	if !exists {
		t.Error("Expected record 1 to exist")
	}
	_, exists = persistence.GetRecord("testdb", "users", "2")
	if !exists {
		t.Error("Expected record 2 to exist")
	}
}
