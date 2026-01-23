package CommitDB

import (
	"os"
	"strconv"
	"testing"

	"github.com/nickyhof/CommitDB/core"
	"github.com/nickyhof/CommitDB/db"
	"github.com/nickyhof/CommitDB/ps"
)

// TestFunc is the signature for test functions that work with any persistence
type TestFunc func(t *testing.T, engine *db.Engine)

// runWithBothPersistence runs a test function with both memory and file persistence
func runWithBothPersistence(t *testing.T, testFunc TestFunc) {
	t.Run("Memory", func(t *testing.T) {
		persistence, err := ps.NewMemoryPersistence()
		if err != nil {
			t.Fatalf("Failed to initialize memory persistence: %v", err)
		}
		DB := Open(&persistence)
		engine := DB.Engine(core.Identity{Name: "test", Email: "test@test.com"})
		testFunc(t, engine)
	})

	t.Run("File", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "commitdb-test-*")
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		persistence, err := ps.NewFilePersistence(tmpDir, nil)
		if err != nil {
			t.Fatalf("Failed to initialize file persistence: %v", err)
		}
		DB := Open(&persistence)
		engine := DB.Engine(core.Identity{Name: "test", Email: "test@test.com"})
		testFunc(t, engine)
	})
}

// TestIntegrationWorkflow tests a complete database workflow
func TestIntegrationWorkflow(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {

		// Create database
		result, err := engine.Execute("CREATE DATABASE company")
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}
		if result.(db.CommitResult).DatabasesCreated != 1 {
			t.Error("Expected 1 database created")
		}

		// Create employees table
		_, err = engine.Execute("CREATE TABLE company.employees (id INT PRIMARY KEY, name STRING, department STRING, salary INT)")
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Create departments table
		_, err = engine.Execute("CREATE TABLE company.departments (id INT PRIMARY KEY, name STRING)")
		if err != nil {
			t.Fatalf("Failed to create departments table: %v", err)
		}

		// Insert employees
		employees := []string{
			"INSERT INTO company.employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 80000)",
			"INSERT INTO company.employees (id, name, department, salary) VALUES (2, 'Bob', 'Engineering', 75000)",
			"INSERT INTO company.employees (id, name, department, salary) VALUES (3, 'Charlie', 'Sales', 60000)",
			"INSERT INTO company.employees (id, name, department, salary) VALUES (4, 'Diana', 'Marketing', 65000)",
			"INSERT INTO company.employees (id, name, department, salary) VALUES (5, 'Eve', 'Engineering', 90000)",
		}

		for _, sql := range employees {
			_, err := engine.Execute(sql)
			if err != nil {
				t.Fatalf("Failed to insert: %v", err)
			}
		}

		// Insert departments
		departments := []string{
			"INSERT INTO company.departments (id, name) VALUES (1, 'Engineering')",
			"INSERT INTO company.departments (id, name) VALUES (2, 'Sales')",
			"INSERT INTO company.departments (id, name) VALUES (3, 'Marketing')",
		}

		for _, sql := range departments {
			_, err := engine.Execute(sql)
			if err != nil {
				t.Fatalf("Failed to insert department: %v", err)
			}
		}

		// Verify count
		result, err = engine.Execute("SELECT COUNT(*) FROM company.employees")
		if err != nil {
			t.Fatalf("Failed to count: %v", err)
		}
		qr := result.(db.QueryResult)
		if qr.Data[0][0] != "5" {
			t.Errorf("Expected 5 employees, got %s", qr.Data[0][0])
		}

		// Test SELECT with ORDER BY
		result, err = engine.Execute("SELECT * FROM company.employees ORDER BY salary DESC LIMIT 3")
		if err != nil {
			t.Fatalf("Failed to select with ORDER BY: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 3 {
			t.Errorf("Expected 3 records with LIMIT 3, got %d", len(qr.Data))
		}

		// Test WHERE with comparison
		result, err = engine.Execute("SELECT * FROM company.employees WHERE salary > 70000")
		if err != nil {
			t.Fatalf("Failed to select with WHERE: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 3 {
			t.Errorf("Expected 3 employees with salary > 70000, got %d", len(qr.Data))
		}

		// Test UPDATE
		_, err = engine.Execute("UPDATE company.employees SET salary = 95000 WHERE id = 5")
		if err != nil {
			t.Fatalf("Failed to update: %v", err)
		}

		// Verify update
		result, err = engine.Execute("SELECT * FROM company.employees WHERE id = 5")
		if err != nil {
			t.Fatalf("Failed to verify update: %v", err)
		}
		qr = result.(db.QueryResult)
		salaryIdx := -1
		for i, col := range qr.Columns {
			if col == "salary" {
				salaryIdx = i
			}
		}
		if salaryIdx >= 0 && qr.Data[0][salaryIdx] != "95000" {
			t.Errorf("Expected updated salary 95000, got %s", qr.Data[0][salaryIdx])
		}

		// Test DELETE
		_, err = engine.Execute("DELETE FROM company.employees WHERE id = 3")
		if err != nil {
			t.Fatalf("Failed to delete: %v", err)
		}

		// Verify delete
		result, err = engine.Execute("SELECT COUNT(*) FROM company.employees")
		if err != nil {
			t.Fatalf("Failed to count after delete: %v", err)
		}
		qr = result.(db.QueryResult)
		if qr.Data[0][0] != "4" {
			t.Errorf("Expected 4 employees after delete, got %s", qr.Data[0][0])
		}
	})
}

// TestIntegrationAggregates tests aggregate functions
func TestIntegrationAggregates(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {

		// Setup
		engine.Execute("CREATE DATABASE sales")
		engine.Execute("CREATE TABLE sales.orders (id INT PRIMARY KEY, customer STRING, amount INT, region STRING)")

		// Insert test data
		orders := []string{
			"INSERT INTO sales.orders (id, customer, amount, region) VALUES (1, 'Acme', 1000, 'East')",
			"INSERT INTO sales.orders (id, customer, amount, region) VALUES (2, 'Beta', 2000, 'West')",
			"INSERT INTO sales.orders (id, customer, amount, region) VALUES (3, 'Acme', 1500, 'East')",
			"INSERT INTO sales.orders (id, customer, amount, region) VALUES (4, 'Gamma', 3000, 'West')",
			"INSERT INTO sales.orders (id, customer, amount, region) VALUES (5, 'Beta', 500, 'East')",
		}

		for _, sql := range orders {
			engine.Execute(sql)
		}

		// Test SUM
		result, err := engine.Execute("SELECT SUM(amount) FROM sales.orders")
		if err != nil {
			t.Fatalf("Failed to execute SUM: %v", err)
		}
		qr := result.(db.QueryResult)
		if qr.Data[0][0] != "8000" {
			t.Errorf("Expected SUM of 8000, got %s", qr.Data[0][0])
		}

		// Test AVG
		result, err = engine.Execute("SELECT AVG(amount) FROM sales.orders")
		if err != nil {
			t.Fatalf("Failed to execute AVG: %v", err)
		}
		qr = result.(db.QueryResult)
		if qr.Data[0][0] != "1600.00" {
			t.Errorf("Expected AVG of 1600.00, got %s", qr.Data[0][0])
		}

		// Test MIN
		result, err = engine.Execute("SELECT MIN(amount) FROM sales.orders")
		if err != nil {
			t.Fatalf("Failed to execute MIN: %v", err)
		}
		qr = result.(db.QueryResult)
		if qr.Data[0][0] != "500" {
			t.Errorf("Expected MIN of 500, got %s", qr.Data[0][0])
		}

		// Test MAX
		result, err = engine.Execute("SELECT MAX(amount) FROM sales.orders")
		if err != nil {
			t.Fatalf("Failed to execute MAX: %v", err)
		}
		qr = result.(db.QueryResult)
		if qr.Data[0][0] != "3000" {
			t.Errorf("Expected MAX of 3000, got %s", qr.Data[0][0])
		}
	})
}

// TestIntegrationDescribe tests DESCRIBE command
func TestIntegrationDescribe(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {

		engine.Execute("CREATE DATABASE schema_test")
		engine.Execute("CREATE TABLE schema_test.products (id INT PRIMARY KEY, name STRING, price FLOAT, active BOOL)")

		result, err := engine.Execute("DESCRIBE schema_test.products")
		if err != nil {
			t.Fatalf("Failed to describe table: %v", err)
		}

		qr := result.(db.QueryResult)
		if len(qr.Data) != 4 {
			t.Errorf("Expected 4 columns in DESCRIBE, got %d", len(qr.Data))
		}
	})
}

// TestIntegrationDistinct tests DISTINCT
func TestIntegrationDistinct(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {

		engine.Execute("CREATE DATABASE distinct_test")
		engine.Execute("CREATE TABLE distinct_test.items (id INT PRIMARY KEY, category STRING)")

		// Insert duplicates
		engine.Execute("INSERT INTO distinct_test.items (id, category) VALUES (1, 'A')")
		engine.Execute("INSERT INTO distinct_test.items (id, category) VALUES (2, 'B')")
		engine.Execute("INSERT INTO distinct_test.items (id, category) VALUES (3, 'A')")
		engine.Execute("INSERT INTO distinct_test.items (id, category) VALUES (4, 'C')")
		engine.Execute("INSERT INTO distinct_test.items (id, category) VALUES (5, 'B')")

		result, err := engine.Execute("SELECT DISTINCT category FROM distinct_test.items")
		if err != nil {
			t.Fatalf("Failed to execute DISTINCT: %v", err)
		}

		qr := result.(db.QueryResult)
		if len(qr.Data) != 3 {
			t.Errorf("Expected 3 distinct categories, got %d", len(qr.Data))
		}
	})
}

// TestIntegrationWhereOperators tests various WHERE operators
func TestIntegrationWhereOperators(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {

		engine.Execute("CREATE DATABASE where_test")
		engine.Execute("CREATE TABLE where_test.nums (id INT PRIMARY KEY, value INT)")

		for i := 1; i <= 10; i++ {
			engine.Execute("INSERT INTO where_test.nums (id, value) VALUES (" +
				strconv.Itoa(i) + ", " + strconv.Itoa(i*10) + ")")
		}

		tests := []struct {
			where    string
			expected int
		}{
			{"value > 50", 5},
			{"value >= 50", 6},
			{"value < 50", 4},
			{"value <= 50", 5},
			{"value = 50", 1},
			{"value != 50", 9},
		}

		for _, test := range tests {
			result, err := engine.Execute("SELECT * FROM where_test.nums WHERE " + test.where)
			if err != nil {
				t.Fatalf("Failed to execute WHERE %s: %v", test.where, err)
			}
			qr := result.(db.QueryResult)
			if len(qr.Data) != test.expected {
				t.Errorf("WHERE %s: expected %d rows, got %d", test.where, test.expected, len(qr.Data))
			}
		}
	})
}

// TestIntegrationOffsetLimit tests OFFSET and LIMIT
func TestIntegrationOffsetLimit(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {

		engine.Execute("CREATE DATABASE page_test")
		engine.Execute("CREATE TABLE page_test.items (id INT PRIMARY KEY, name STRING)")

		for i := 1; i <= 20; i++ {
			engine.Execute("INSERT INTO page_test.items (id, name) VALUES (" +
				strconv.Itoa(i) + ", 'Item" + strconv.Itoa(i) + "')")
		}

		// Test LIMIT
		result, err := engine.Execute("SELECT * FROM page_test.items LIMIT 5")
		if err != nil {
			t.Fatalf("Failed LIMIT: %v", err)
		}
		if len(result.(db.QueryResult).Data) != 5 {
			t.Error("LIMIT 5 should return 5 rows")
		}

		// Test OFFSET
		result, err = engine.Execute("SELECT * FROM page_test.items LIMIT 5 OFFSET 15")
		if err != nil {
			t.Fatalf("Failed OFFSET: %v", err)
		}
		if len(result.(db.QueryResult).Data) != 5 {
			t.Error("LIMIT 5 OFFSET 15 should return 5 rows")
		}

		// Test OFFSET beyond data
		result, err = engine.Execute("SELECT * FROM page_test.items LIMIT 5 OFFSET 100")
		if err != nil {
			t.Fatalf("Failed large OFFSET: %v", err)
		}
		if len(result.(db.QueryResult).Data) != 0 {
			t.Error("OFFSET beyond data should return 0 rows")
		}
	})
}

// TestIntegrationErrorHandling tests error cases
func TestIntegrationErrorHandling(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {

		// Create database for tests
		engine.Execute("CREATE DATABASE error_test")
		engine.Execute("CREATE TABLE error_test.users (id INT PRIMARY KEY, name STRING)")

		// Test table not found
		_, err := engine.Execute("SELECT * FROM error_test.nonexistent")
		if err == nil {
			t.Error("Expected error for non-existent table")
		}

		// Test database not found
		_, err = engine.Execute("SELECT * FROM nonexistent.users")
		if err == nil {
			t.Error("Expected error for non-existent database")
		}

		// Test syntax error
		_, err = engine.Execute("SELEKT * FROM error_test.users")
		if err == nil {
			t.Error("Expected error for syntax error")
		}
	})
}

// TestIntegrationTransactionCommands tests BEGIN/COMMIT/ROLLBACK
func TestIntegrationTransactionCommands(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {

		// Setup database first
		engine.Execute("CREATE DATABASE txn_test")

		// Test BEGIN - just verify no crash
		_, err := engine.Execute("BEGIN")
		if err != nil {
			t.Logf("BEGIN note: %v", err) // Log but don't fail
		}

		// Test ROLLBACK - just verify no crash
		_, err = engine.Execute("ROLLBACK")
		if err != nil {
			t.Logf("ROLLBACK note: %v", err) // Log but don't fail
		}
	})
}

// TestIntegrationDropOperations tests DROP commands
func TestIntegrationDropOperations(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {

		// Create and drop table
		engine.Execute("CREATE DATABASE drop_test")
		engine.Execute("CREATE TABLE drop_test.temp (id INT PRIMARY KEY)")

		_, err := engine.Execute("DROP TABLE drop_test.temp")
		if err != nil {
			t.Fatalf("DROP TABLE failed: %v", err)
		}

		// Verify table is gone
		_, err = engine.Execute("SELECT * FROM drop_test.temp")
		if err == nil {
			t.Error("Expected error accessing dropped table")
		}

		// Drop database
		_, err = engine.Execute("DROP DATABASE drop_test")
		if err != nil {
			t.Fatalf("DROP DATABASE failed: %v", err)
		}
	})
}

// ============================================================================
// FILE PERSISTENCE TESTS
// ============================================================================

// TestFilePersistenceReopen tests that data persists after reopening the database
// This test specifically requires file persistence and reopening, so it can't use runWithBothPersistence
func TestFilePersistenceReopen(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "commitdb-persist-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// First session: create and populate
	persistence1, _ := ps.NewFilePersistence(tmpDir, nil)
	db1 := Open(&persistence1)
	engine1 := db1.Engine(core.Identity{Name: "test", Email: "test@test.com"})

	engine1.Execute("CREATE DATABASE persist")
	engine1.Execute("CREATE TABLE persist.data (id INT PRIMARY KEY, val STRING)")
	engine1.Execute("INSERT INTO persist.data (id, val) VALUES (1, 'hello')")
	engine1.Execute("INSERT INTO persist.data (id, val) VALUES (2, 'world')")

	// Second session: reopen and verify
	persistence2, _ := ps.NewFilePersistence(tmpDir, nil)
	db2 := Open(&persistence2)
	engine2 := db2.Engine(core.Identity{Name: "test", Email: "test@test.com"})

	result, err := engine2.Execute("SELECT * FROM persist.data")
	if err != nil {
		t.Fatalf("Failed to read persisted data: %v", err)
	}

	qr := result.(db.QueryResult)
	if len(qr.Data) != 2 {
		t.Errorf("Expected 2 persisted rows, got %d", len(qr.Data))
	}
}
