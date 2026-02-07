package engine

import (
	"testing"

	"github.com/nickyhof/CommitDB/v2/core"
	"github.com/nickyhof/CommitDB/v2/persistence"
)

func setupTestEngine(t *testing.T) *Engine {
	persistence, err := persistence.NewMemoryPersistence()
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

func TestEngineUpdateWithNonPKWhere(t *testing.T) {
	engine := setupTestEngine(t)
	insertTestData(t, engine)

	// Update by non-PK column
	result, err := engine.Execute("UPDATE testdb.users SET name = 'Senior' WHERE age > 28")
	if err != nil {
		t.Fatalf("Failed to execute UPDATE with non-PK WHERE: %v", err)
	}

	cr := result.(CommitResult)
	if cr.RecordsWritten != 2 {
		t.Errorf("Expected 2 records updated (Alice=30, Charlie=35), got %d", cr.RecordsWritten)
	}

	// Verify updates
	selectResult, _ := engine.Execute("SELECT * FROM testdb.users WHERE name = 'Senior'")
	qr := selectResult.(QueryResult)
	if qr.RecordsRead != 2 {
		t.Errorf("Expected 2 records with name='Senior', got %d", qr.RecordsRead)
	}
}

func TestEngineDeleteWithNonPKWhere(t *testing.T) {
	engine := setupTestEngine(t)
	insertTestData(t, engine)

	// Delete by non-PK column
	result, err := engine.Execute("DELETE FROM testdb.users WHERE age < 30")
	if err != nil {
		t.Fatalf("Failed to execute DELETE with non-PK WHERE: %v", err)
	}

	cr := result.(CommitResult)
	if cr.RecordsDeleted != 1 {
		t.Errorf("Expected 1 record deleted (Bob=25), got %d", cr.RecordsDeleted)
	}

	// Verify deletion
	selectResult, _ := engine.Execute("SELECT COUNT(*) FROM testdb.users")
	qr := selectResult.(QueryResult)
	if qr.Data[0][0] != "2" {
		t.Errorf("Expected 2 records after delete, got %s", qr.Data[0][0])
	}
}

func TestEngineDropTable(t *testing.T) {
	engine := setupTestEngine(t)

	// Drop existing table
	_, err := engine.Execute("DROP TABLE testdb.users")
	if err != nil {
		t.Fatalf("Failed to DROP TABLE: %v", err)
	}

	// Verify table is gone
	result, _ := engine.Execute("SHOW TABLES IN testdb")
	qr := result.(QueryResult)
	found := false
	for _, row := range qr.Data {
		if row[0] == "users" {
			found = true
		}
	}
	if found {
		t.Error("Table 'users' should not exist after DROP")
	}
}

func TestEngineDropTableIfExists(t *testing.T) {
	engine := setupTestEngine(t)

	// Drop non-existent table with IF EXISTS should not error
	_, err := engine.Execute("DROP TABLE IF EXISTS testdb.nonexistent")
	if err != nil {
		t.Errorf("DROP TABLE IF EXISTS should not error: %v", err)
	}
}

func TestEngineDropDatabase(t *testing.T) {
	engine := setupTestEngine(t)

	// Create a second database to drop
	_, _ = engine.Execute("CREATE DATABASE dropme")

	_, err := engine.Execute("DROP DATABASE dropme")
	if err != nil {
		t.Fatalf("Failed to DROP DATABASE: %v", err)
	}

	// Verify database is gone
	result, _ := engine.Execute("SHOW DATABASES")
	qr := result.(QueryResult)
	for _, row := range qr.Data {
		if row[0] == "dropme" {
			t.Error("Database 'dropme' should not exist after DROP")
		}
	}
}

func TestEngineDropDatabaseIfExists(t *testing.T) {
	engine := setupTestEngine(t)

	_, err := engine.Execute("DROP DATABASE IF EXISTS nonexistent")
	if err != nil {
		t.Errorf("DROP DATABASE IF EXISTS should not error: %v", err)
	}
}

func TestEngineAlterTableAddColumn(t *testing.T) {
	engine := setupTestEngine(t)
	insertTestData(t, engine)

	// Add a column
	_, err := engine.Execute("ALTER TABLE testdb.users ADD COLUMN email STRING")
	if err != nil {
		t.Fatalf("Failed to ALTER TABLE ADD COLUMN: %v", err)
	}

	// Verify new column appears in DESCRIBE
	result, _ := engine.Execute("DESCRIBE testdb.users")
	qr := result.(QueryResult)
	found := false
	for _, row := range qr.Data {
		if row[0] == "email" {
			found = true
		}
	}
	if !found {
		t.Error("Expected 'email' column after ALTER TABLE ADD COLUMN")
	}
}

func TestEngineAlterTableDropColumn(t *testing.T) {
	engine := setupTestEngine(t)
	insertTestData(t, engine)

	// Drop a column
	_, err := engine.Execute("ALTER TABLE testdb.users DROP COLUMN age")
	if err != nil {
		t.Fatalf("Failed to ALTER TABLE DROP COLUMN: %v", err)
	}

	// Verify column is gone
	result, _ := engine.Execute("DESCRIBE testdb.users")
	qr := result.(QueryResult)
	for _, row := range qr.Data {
		if row[0] == "age" {
			t.Error("Column 'age' should not exist after DROP COLUMN")
		}
	}
}

func TestEngineAlterTableRenameColumn(t *testing.T) {
	engine := setupTestEngine(t)
	insertTestData(t, engine)

	_, err := engine.Execute("ALTER TABLE testdb.users RENAME COLUMN name TO username")
	if err != nil {
		t.Fatalf("Failed to ALTER TABLE RENAME COLUMN: %v", err)
	}

	// Verify renamed column
	result, _ := engine.Execute("DESCRIBE testdb.users")
	qr := result.(QueryResult)
	foundOld := false
	foundNew := false
	for _, row := range qr.Data {
		if row[0] == "name" {
			foundOld = true
		}
		if row[0] == "username" {
			foundNew = true
		}
	}
	if foundOld {
		t.Error("Old column 'name' should not exist after RENAME")
	}
	if !foundNew {
		t.Error("New column 'username' should exist after RENAME")
	}
}

func TestEngineViews(t *testing.T) {
	engine := setupTestEngine(t)
	insertTestData(t, engine)

	// Create a view
	_, err := engine.Execute("CREATE VIEW testdb.adults AS SELECT * FROM testdb.users WHERE age > 28")
	if err != nil {
		t.Fatalf("Failed to CREATE VIEW: %v", err)
	}

	// Query the view
	result, err := engine.Execute("SELECT * FROM testdb.adults")
	if err != nil {
		t.Fatalf("Failed to SELECT from view: %v", err)
	}
	qr := result.(QueryResult)
	if qr.RecordsRead != 2 {
		t.Errorf("Expected 2 adults (Alice=30, Charlie=35), got %d", qr.RecordsRead)
	}

	// SHOW VIEWS
	result, err = engine.Execute("SHOW VIEWS IN testdb")
	if err != nil {
		t.Fatalf("Failed to SHOW VIEWS: %v", err)
	}
	qr = result.(QueryResult)
	if len(qr.Data) != 1 {
		t.Errorf("Expected 1 view, got %d", len(qr.Data))
	}

	// DROP VIEW
	_, err = engine.Execute("DROP VIEW testdb.adults")
	if err != nil {
		t.Fatalf("Failed to DROP VIEW: %v", err)
	}
}

func TestEngineMaterializedView(t *testing.T) {
	engine := setupTestEngine(t)
	insertTestData(t, engine)

	// Create materialized view
	_, err := engine.Execute("CREATE MATERIALIZED VIEW testdb.young AS SELECT * FROM testdb.users WHERE age < 35")
	if err != nil {
		t.Fatalf("Failed to CREATE MATERIALIZED VIEW: %v", err)
	}

	// Query it
	result, err := engine.Execute("SELECT * FROM testdb.young")
	if err != nil {
		t.Fatalf("Failed to SELECT from materialized view: %v", err)
	}
	qr := result.(QueryResult)
	if qr.RecordsRead != 2 {
		t.Errorf("Expected 2 young users (Alice=30, Bob=25), got %d", qr.RecordsRead)
	}

	// Insert new data — materialized view should NOT auto-update
	engine.Execute("INSERT INTO testdb.users (id, name, age) VALUES (4, 'Diana', 28)")

	result, _ = engine.Execute("SELECT * FROM testdb.young")
	qr = result.(QueryResult)
	if qr.RecordsRead != 2 {
		t.Errorf("Materialized view should still have 2 rows before REFRESH, got %d", qr.RecordsRead)
	}

	// REFRESH
	_, err = engine.Execute("REFRESH VIEW testdb.young")
	if err != nil {
		t.Fatalf("Failed to REFRESH VIEW: %v", err)
	}

	result, _ = engine.Execute("SELECT * FROM testdb.young")
	qr = result.(QueryResult)
	if qr.RecordsRead != 3 {
		t.Errorf("Expected 3 rows after REFRESH, got %d", qr.RecordsRead)
	}

	// Cleanup
	engine.Execute("DROP VIEW testdb.young")
}

func TestEngineShowIndexes(t *testing.T) {
	engine := setupTestEngine(t)

	_, _ = engine.Execute("CREATE INDEX idx_name ON testdb.users(name)")

	result, err := engine.Execute("SHOW INDEXES ON testdb.users")
	if err != nil {
		t.Fatalf("Failed to SHOW INDEXES: %v", err)
	}
	qr := result.(QueryResult)
	if len(qr.Data) < 1 {
		t.Error("Expected at least 1 index")
	}
}

func TestEngineBranching(t *testing.T) {
	engine := setupTestEngine(t)
	insertTestData(t, engine)

	// Create branch
	_, err := engine.Execute("CREATE BRANCH feature")
	if err != nil {
		t.Fatalf("Failed to CREATE BRANCH: %v", err)
	}

	// Show branches
	result, err := engine.Execute("SHOW BRANCHES")
	if err != nil {
		t.Fatalf("Failed to SHOW BRANCHES: %v", err)
	}
	qr := result.(QueryResult)
	if len(qr.Data) < 2 {
		t.Errorf("Expected at least 2 branches (main + feature), got %d", len(qr.Data))
	}

	// Checkout branch
	_, err = engine.Execute("CHECKOUT feature")
	if err != nil {
		t.Fatalf("Failed to CHECKOUT: %v", err)
	}

	// Data should still be accessible on the new branch
	result, err = engine.Execute("SELECT COUNT(*) FROM testdb.users")
	if err != nil {
		t.Fatalf("Failed to SELECT on feature branch: %v", err)
	}
	qr = result.(QueryResult)
	if qr.Data[0][0] != "3" {
		t.Errorf("Expected 3 rows on feature branch, got %s", qr.Data[0][0])
	}
}

func TestEngineAllDataTypes(t *testing.T) {
	engine := setupTestEngine(t)

	// Create table with all supported data types
	_, err := engine.Execute(`CREATE TABLE testdb.all_types (
		id INT PRIMARY KEY,
		str_col STRING,
		int_col INT,
		float_col FLOAT,
		bool_col BOOL,
		text_col TEXT,
		date_col DATE,
		ts_col TIMESTAMP,
		json_col JSON
	)`)
	if err != nil {
		t.Fatalf("Failed to create all-types table: %v", err)
	}

	// Verify schema has all types
	result, _ := engine.Execute("DESCRIBE testdb.all_types")
	qr := result.(QueryResult)
	if len(qr.Data) != 9 {
		t.Fatalf("Expected 9 columns, got %d", len(qr.Data))
	}

	// Insert a row with all types populated
	_, err = engine.Execute(`INSERT INTO testdb.all_types (id, str_col, int_col, float_col, bool_col, text_col, date_col, ts_col, json_col) VALUES (
		1,
		'hello',
		42,
		3.14,
		true,
		'This is a long text field with lots of content.',
		'2025-06-15',
		'2025-06-15 10:30:00',
		'{"key":"value","num":123}'
	)`)
	if err != nil {
		t.Fatalf("Failed to INSERT all types: %v", err)
	}

	// Read it back and verify
	result, err = engine.Execute("SELECT * FROM testdb.all_types WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to SELECT: %v", err)
	}
	qr = result.(QueryResult)
	if qr.RecordsRead != 1 {
		t.Fatalf("Expected 1 row, got %d", qr.RecordsRead)
	}

	// Verify each column value survived the roundtrip
	row := qr.Data[0]
	checks := []struct {
		idx      int
		name     string
		expected string
	}{
		{0, "id", "1"},
		{1, "str_col", "hello"},
		{2, "int_col", "42"},
		{3, "float_col", "3.14"},
		{4, "bool_col", "true"},
		{5, "text_col", "This is a long text field with lots of content."},
		{6, "date_col", "2025-06-15"},
		{7, "ts_col", "2025-06-15 10:30:00"},
	}
	for _, c := range checks {
		if row[c.idx] != c.expected {
			t.Errorf("Column %s: expected '%s', got '%s'", c.name, c.expected, row[c.idx])
		}
	}
	// JSON column - just verify it's non-empty
	if len(row[8]) == 0 {
		t.Error("JSON column should be non-empty")
	}
}

func TestEngineDataTypeValidation(t *testing.T) {
	engine := setupTestEngine(t)

	engine.Execute(`CREATE TABLE testdb.validated (
		id INT PRIMARY KEY,
		d DATE,
		ts TIMESTAMP,
		j JSON
	)`)

	// Invalid DATE format
	_, err := engine.Execute("INSERT INTO testdb.validated (id, d, ts, j) VALUES (1, 'not-a-date', '2025-01-01 00:00:00', '{}')")
	if err == nil {
		t.Error("Expected error for invalid DATE format")
	}

	// Invalid TIMESTAMP format
	_, err = engine.Execute("INSERT INTO testdb.validated (id, d, ts, j) VALUES (2, '2025-01-01', 'not-a-timestamp', '{}')")
	if err == nil {
		t.Error("Expected error for invalid TIMESTAMP format")
	}

	// Invalid JSON format
	_, err = engine.Execute("INSERT INTO testdb.validated (id, d, ts, j) VALUES (3, '2025-01-01', '2025-01-01 00:00:00', 'not json')")
	if err == nil {
		t.Error("Expected error for invalid JSON format")
	}

	// Valid row should succeed
	_, err = engine.Execute("INSERT INTO testdb.validated (id, d, ts, j) VALUES (4, '2025-01-01', '2025-01-01 12:00:00', '{\"ok\":true}')")
	if err != nil {
		t.Fatalf("Valid insert should succeed: %v", err)
	}
}

func TestEngineDataTypeAliases(t *testing.T) {
	engine := setupTestEngine(t)

	// Test SQL type aliases: INTEGER, DOUBLE, REAL, BOOLEAN, DATETIME
	_, err := engine.Execute(`CREATE TABLE testdb.aliases (
		id INTEGER PRIMARY KEY,
		name STRING,
		score DOUBLE,
		rating REAL,
		active BOOLEAN,
		created DATETIME
	)`)
	if err != nil {
		t.Fatalf("Failed to create table with type aliases: %v", err)
	}

	// Insert and verify roundtrip
	_, err = engine.Execute("INSERT INTO testdb.aliases (id, name, score, rating, active, created) VALUES (1, 'test', 9.5, 4.2, false, '2025-03-01 08:00:00')")
	if err != nil {
		t.Fatalf("Failed to INSERT with aliased types: %v", err)
	}

	result, _ := engine.Execute("SELECT * FROM testdb.aliases WHERE id = 1")
	qr := result.(QueryResult)
	if qr.RecordsRead != 1 {
		t.Errorf("Expected 1 row, got %d", qr.RecordsRead)
	}
	row := qr.Data[0]
	if row[1] != "test" {
		t.Errorf("STRING: expected 'test', got '%s'", row[1])
	}
	if row[2] != "9.5" {
		t.Errorf("DOUBLE: expected '9.5', got '%s'", row[2])
	}
	if row[3] != "4.2" {
		t.Errorf("REAL: expected '4.2', got '%s'", row[3])
	}
	if row[4] != "false" {
		t.Errorf("BOOLEAN: expected 'false', got '%s'", row[4])
	}
}
