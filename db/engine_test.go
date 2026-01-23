package db

import (
	"testing"

	"github.com/nickyhof/CommitDB/core"
	"github.com/nickyhof/CommitDB/ps"
)

func setupTestEngine(t *testing.T) *Engine {
	persistence, err := ps.NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "test", Email: "test@test.com"}
	engine := NewEngine(&persistence, identity)

	// Create test database and table
	_, _ = engine.Execute("CREATE DATABASE testdb")
	_, _ = engine.Execute("CREATE TABLE testdb.users (id INT PRIMARY KEY, name STRING, age INT)")

	return engine
}

func insertTestData(t *testing.T, engine *Engine) {
	_, _ = engine.Execute("INSERT INTO testdb.users (id, name, age) VALUES (1, 'Alice', 30)")
	_, _ = engine.Execute("INSERT INTO testdb.users (id, name, age) VALUES (2, 'Bob', 25)")
	_, _ = engine.Execute("INSERT INTO testdb.users (id, name, age) VALUES (3, 'Charlie', 35)")
}

func TestEngineSelect(t *testing.T) {
	engine := setupTestEngine(t)
	insertTestData(t, engine)

	result, err := engine.Execute("SELECT * FROM testdb.users")
	if err != nil {
		t.Fatalf("Failed to execute SELECT: %v", err)
	}

	qr := result.(QueryResult)
	if qr.RecordsRead != 3 {
		t.Errorf("Expected 3 records, got %d", qr.RecordsRead)
	}
}

func TestEngineSelectWithWhere(t *testing.T) {
	engine := setupTestEngine(t)
	insertTestData(t, engine)

	result, err := engine.Execute("SELECT * FROM testdb.users WHERE age > 28")
	if err != nil {
		t.Fatalf("Failed to execute SELECT: %v", err)
	}

	qr := result.(QueryResult)
	if qr.RecordsRead != 2 {
		t.Errorf("Expected 2 records with age > 28, got %d", qr.RecordsRead)
	}
}

func TestEngineSelectOrderBy(t *testing.T) {
	engine := setupTestEngine(t)
	insertTestData(t, engine)

	result, err := engine.Execute("SELECT * FROM testdb.users ORDER BY age DESC")
	if err != nil {
		t.Fatalf("Failed to execute SELECT: %v", err)
	}

	qr := result.(QueryResult)
	if len(qr.Data) < 1 {
		t.Fatal("Expected at least 1 row")
	}

	// Charlie (35) should be first
	firstRow := qr.Data[0]
	found := false
	for _, val := range firstRow {
		if val == "Charlie" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected Charlie to be first with ORDER BY age DESC")
	}
}

func TestEngineSelectLimit(t *testing.T) {
	engine := setupTestEngine(t)
	insertTestData(t, engine)

	result, err := engine.Execute("SELECT * FROM testdb.users LIMIT 2")
	if err != nil {
		t.Fatalf("Failed to execute SELECT: %v", err)
	}

	qr := result.(QueryResult)
	if len(qr.Data) != 2 {
		t.Errorf("Expected 2 records with LIMIT 2, got %d", len(qr.Data))
	}
}

func TestEngineCount(t *testing.T) {
	engine := setupTestEngine(t)
	insertTestData(t, engine)

	result, err := engine.Execute("SELECT COUNT(*) FROM testdb.users")
	if err != nil {
		t.Fatalf("Failed to execute COUNT: %v", err)
	}

	qr := result.(QueryResult)
	if len(qr.Data) != 1 || len(qr.Data[0]) != 1 {
		t.Fatal("Expected single count result")
	}
	if qr.Data[0][0] != "3" {
		t.Errorf("Expected count of 3, got %s", qr.Data[0][0])
	}
}

func TestEngineUpdate(t *testing.T) {
	engine := setupTestEngine(t)
	insertTestData(t, engine)

	// Update by primary key
	_, err := engine.Execute("UPDATE testdb.users SET age = 31 WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to execute UPDATE: %v", err)
	}

	// Verify update
	result, _ := engine.Execute("SELECT * FROM testdb.users WHERE id = 1")
	qr := result.(QueryResult)

	// Find age column value
	ageIdx := -1
	for i, col := range qr.Columns {
		if col == "age" {
			ageIdx = i
			break
		}
	}

	if ageIdx >= 0 && len(qr.Data) > 0 {
		if qr.Data[0][ageIdx] != "31" {
			t.Errorf("Expected age to be updated to 31, got %s", qr.Data[0][ageIdx])
		}
	}
}

func TestEngineDelete(t *testing.T) {
	engine := setupTestEngine(t)
	insertTestData(t, engine)

	// Delete by primary key
	_, err := engine.Execute("DELETE FROM testdb.users WHERE id = 2")
	if err != nil {
		t.Fatalf("Failed to execute DELETE: %v", err)
	}

	// Verify deletion
	result, _ := engine.Execute("SELECT COUNT(*) FROM testdb.users")
	qr := result.(QueryResult)
	if qr.Data[0][0] != "2" {
		t.Errorf("Expected 2 records after delete, got %s", qr.Data[0][0])
	}
}

func TestEngineDistinct(t *testing.T) {
	engine := setupTestEngine(t)

	// Insert duplicates
	_, _ = engine.Execute("INSERT INTO testdb.users (id, name, age) VALUES (1, 'Alice', 30)")
	_, _ = engine.Execute("INSERT INTO testdb.users (id, name, age) VALUES (2, 'Alice', 30)")
	_, _ = engine.Execute("INSERT INTO testdb.users (id, name, age) VALUES (3, 'Bob', 25)")

	result, err := engine.Execute("SELECT DISTINCT name FROM testdb.users")
	if err != nil {
		t.Fatalf("Failed to execute DISTINCT: %v", err)
	}

	qr := result.(QueryResult)
	if len(qr.Data) != 2 {
		t.Errorf("Expected 2 distinct names, got %d", len(qr.Data))
	}
}

func TestEngineShowDatabases(t *testing.T) {
	engine := setupTestEngine(t)

	result, err := engine.Execute("SHOW DATABASES")
	if err != nil {
		t.Fatalf("Failed to execute SHOW DATABASES: %v", err)
	}

	qr := result.(QueryResult)
	if qr.RecordsRead < 1 {
		t.Error("Expected at least 1 database")
	}
}

func TestEngineShowTables(t *testing.T) {
	engine := setupTestEngine(t)

	result, err := engine.Execute("SHOW TABLES IN testdb")
	if err != nil {
		t.Fatalf("Failed to execute SHOW TABLES: %v", err)
	}

	qr := result.(QueryResult)
	if qr.RecordsRead < 1 {
		t.Error("Expected at least 1 table")
	}
}

func TestEngineDescribe(t *testing.T) {
	engine := setupTestEngine(t)

	result, err := engine.Execute("DESCRIBE testdb.users")
	if err != nil {
		t.Fatalf("Failed to execute DESCRIBE: %v", err)
	}

	qr := result.(QueryResult)
	if qr.RecordsRead != 3 {
		t.Errorf("Expected 3 columns in DESCRIBE, got %d", qr.RecordsRead)
	}
}

func TestEngineBeginCommit(t *testing.T) {
	engine := setupTestEngine(t)

	_, err := engine.Execute("BEGIN")
	if err != nil {
		t.Fatalf("Failed to execute BEGIN: %v", err)
	}

	_, err = engine.Execute("COMMIT")
	if err != nil {
		t.Fatalf("Failed to execute COMMIT: %v", err)
	}
}

func TestEngineRollback(t *testing.T) {
	engine := setupTestEngine(t)

	_, err := engine.Execute("ROLLBACK")
	if err != nil {
		t.Fatalf("Failed to execute ROLLBACK: %v", err)
	}
}

func TestEngineCreateDropIndex(t *testing.T) {
	engine := setupTestEngine(t)

	_, err := engine.Execute("CREATE INDEX idx_name ON testdb.users(name)")
	if err != nil {
		t.Fatalf("Failed to CREATE INDEX: %v", err)
	}

	_, _ = engine.Execute("DROP INDEX idx_name ON testdb.users")
}
