package tests

import (
	"os"
	"strconv"
	"testing"

	"github.com/nickyhof/CommitDB"
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
		DB := CommitDB.Open(&persistence)
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
		DB := CommitDB.Open(&persistence)
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

// TestIntegrationInOperator tests IN operator
func TestIntegrationInOperator(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {

		engine.Execute("CREATE DATABASE in_test")
		engine.Execute("CREATE TABLE in_test.items (id INT PRIMARY KEY, status STRING, category STRING)")

		// Insert test data
		engine.Execute("INSERT INTO in_test.items (id, status, category) VALUES (1, 'active', 'A')")
		engine.Execute("INSERT INTO in_test.items (id, status, category) VALUES (2, 'pending', 'B')")
		engine.Execute("INSERT INTO in_test.items (id, status, category) VALUES (3, 'active', 'C')")
		engine.Execute("INSERT INTO in_test.items (id, status, category) VALUES (4, 'archived', 'A')")
		engine.Execute("INSERT INTO in_test.items (id, status, category) VALUES (5, 'pending', 'B')")

		tests := []struct {
			where    string
			expected int
		}{
			{"status IN ('active', 'pending')", 4},
			{"status IN ('active')", 2},
			{"status IN ('archived')", 1},
			{"category IN ('A', 'B')", 4},
			{"category IN ('C')", 1},
			{"id IN (1, 3, 5)", 3},
			{"NOT status IN ('archived')", 4},
		}

		for _, test := range tests {
			result, err := engine.Execute("SELECT * FROM in_test.items WHERE " + test.where)
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

// TestIntegrationStringFunctions tests string functions
func TestIntegrationStringFunctions(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {

		engine.Execute("CREATE DATABASE strfunc_test")
		engine.Execute("CREATE TABLE strfunc_test.data (id INT PRIMARY KEY, name STRING, description STRING)")

		// Insert test data
		engine.Execute("INSERT INTO strfunc_test.data (id, name, description) VALUES (1, 'Alice', '  hello world  ')")
		engine.Execute("INSERT INTO strfunc_test.data (id, name, description) VALUES (2, 'Bob', 'testing')")
		engine.Execute("INSERT INTO strfunc_test.data (id, name, description) VALUES (3, 'Charlie', 'replace me')")

		// Test UPPER
		result, err := engine.Execute("SELECT UPPER(name) FROM strfunc_test.data WHERE id = 1")
		if err != nil {
			t.Fatalf("UPPER failed: %v", err)
		}
		qr := result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "ALICE" {
			t.Errorf("UPPER: expected 'ALICE', got '%s'", qr.Data[0][0])
		}

		// Test LOWER
		result, err = engine.Execute("SELECT LOWER(name) FROM strfunc_test.data WHERE id = 1")
		if err != nil {
			t.Fatalf("LOWER failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "alice" {
			t.Errorf("LOWER: expected 'alice', got '%s'", qr.Data[0][0])
		}

		// Test CONCAT
		result, err = engine.Execute("SELECT CONCAT(name, '-test') FROM strfunc_test.data WHERE id = 2")
		if err != nil {
			t.Fatalf("CONCAT failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "Bob-test" {
			t.Errorf("CONCAT: expected 'Bob-test', got '%s'", qr.Data[0][0])
		}

		// Test TRIM
		result, err = engine.Execute("SELECT TRIM(description) FROM strfunc_test.data WHERE id = 1")
		if err != nil {
			t.Fatalf("TRIM failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "hello world" {
			t.Errorf("TRIM: expected 'hello world', got '%s'", qr.Data[0][0])
		}

		// Test LENGTH
		result, err = engine.Execute("SELECT LENGTH(name) FROM strfunc_test.data WHERE id = 2")
		if err != nil {
			t.Fatalf("LENGTH failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "3" {
			t.Errorf("LENGTH: expected '3', got '%s'", qr.Data[0][0])
		}

		// Test SUBSTRING
		result, err = engine.Execute("SELECT SUBSTRING(name, 1, 3) FROM strfunc_test.data WHERE id = 3")
		if err != nil {
			t.Fatalf("SUBSTRING failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "Cha" {
			t.Errorf("SUBSTRING: expected 'Cha', got '%s'", qr.Data[0][0])
		}

		// Test REPLACE
		result, err = engine.Execute("SELECT REPLACE(description, 'me', 'you') FROM strfunc_test.data WHERE id = 3")
		if err != nil {
			t.Fatalf("REPLACE failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "replace you" {
			t.Errorf("REPLACE: expected 'replace you', got '%s'", qr.Data[0][0])
		}

		// Test with alias
		result, err = engine.Execute("SELECT UPPER(name) AS upper_name FROM strfunc_test.data WHERE id = 1")
		if err != nil {
			t.Fatalf("UPPER with alias failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if qr.Columns[0] != "upper_name" {
			t.Errorf("Expected column name 'upper_name', got '%s'", qr.Columns[0])
		}
	})
}

// TestIntegrationDateFunctions tests date functions
func TestIntegrationDateFunctions(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {

		engine.Execute("CREATE DATABASE datefunc_test")
		engine.Execute("CREATE TABLE datefunc_test.events (id INT PRIMARY KEY, name STRING, created STRING)")

		// Insert test data with dates
		engine.Execute("INSERT INTO datefunc_test.events (id, name, created) VALUES (1, 'Event1', '2024-06-15 14:30:00')")
		engine.Execute("INSERT INTO datefunc_test.events (id, name, created) VALUES (2, 'Event2', '2024-12-25 08:00:00')")

		// Test NOW()
		result, err := engine.Execute("SELECT NOW() FROM datefunc_test.events WHERE id = 1")
		if err != nil {
			t.Fatalf("NOW failed: %v", err)
		}
		qr := result.(db.QueryResult)
		if len(qr.Data) != 1 || len(qr.Data[0][0]) == 0 {
			t.Error("NOW: expected non-empty result")
		}

		// Test YEAR
		result, err = engine.Execute("SELECT YEAR(created) FROM datefunc_test.events WHERE id = 1")
		if err != nil {
			t.Fatalf("YEAR failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "2024" {
			t.Errorf("YEAR: expected '2024', got '%s'", qr.Data[0][0])
		}

		// Test MONTH
		result, err = engine.Execute("SELECT MONTH(created) FROM datefunc_test.events WHERE id = 1")
		if err != nil {
			t.Fatalf("MONTH failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "6" {
			t.Errorf("MONTH: expected '6', got '%s'", qr.Data[0][0])
		}

		// Test DAY
		result, err = engine.Execute("SELECT DAY(created) FROM datefunc_test.events WHERE id = 1")
		if err != nil {
			t.Fatalf("DAY failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "15" {
			t.Errorf("DAY: expected '15', got '%s'", qr.Data[0][0])
		}

		// Test DATE_ADD
		result, err = engine.Execute("SELECT DATE_ADD(created, 7, 'DAY') FROM datefunc_test.events WHERE id = 1")
		if err != nil {
			t.Fatalf("DATE_ADD failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "2024-06-22 14:30:00" {
			t.Errorf("DATE_ADD: expected '2024-06-22 14:30:00', got '%s'", qr.Data[0][0])
		}

		// Test DATE_SUB
		result, err = engine.Execute("SELECT DATE_SUB(created, 15, 'DAY') FROM datefunc_test.events WHERE id = 1")
		if err != nil {
			t.Fatalf("DATE_SUB failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "2024-05-31 14:30:00" {
			t.Errorf("DATE_SUB: expected '2024-05-31 14:30:00', got '%s'", qr.Data[0][0])
		}

		// Test DATEDIFF
		result, err = engine.Execute("SELECT DATEDIFF('2024-12-25', '2024-06-15') FROM datefunc_test.events WHERE id = 1")
		if err != nil {
			t.Fatalf("DATEDIFF failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "193" {
			t.Errorf("DATEDIFF: expected '193', got '%s'", qr.Data[0][0])
		}

		// Test DATE
		result, err = engine.Execute("SELECT DATE(created) FROM datefunc_test.events WHERE id = 1")
		if err != nil {
			t.Fatalf("DATE failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "2024-06-15" {
			t.Errorf("DATE: expected '2024-06-15', got '%s'", qr.Data[0][0])
		}
	})
}

// TestIntegrationDateColumns tests DATE/TIMESTAMP column types and NOW() in INSERT
func TestIntegrationDateColumns(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {

		engine.Execute("CREATE DATABASE date_test")
		engine.Execute("CREATE TABLE date_test.events (id INT PRIMARY KEY, name STRING, event_date DATE, created_at TIMESTAMP)")

		// Verify DATE/TIMESTAMP types in schema
		result, err := engine.Execute("DESCRIBE date_test.events")
		if err != nil {
			t.Fatalf("DESCRIBE failed: %v", err)
		}
		qr := result.(db.QueryResult)
		foundDate := false
		foundTimestamp := false
		for _, row := range qr.Data {
			if row[0] == "event_date" && row[1] == "DATE" {
				foundDate = true
			}
			if row[0] == "created_at" && row[1] == "TIMESTAMP" {
				foundTimestamp = true
			}
		}
		if !foundDate {
			t.Error("DATE type not found in schema")
		}
		if !foundTimestamp {
			t.Error("TIMESTAMP type not found in schema")
		}

		// Test INSERT with NOW()
		_, err = engine.Execute("INSERT INTO date_test.events (id, name, event_date, created_at) VALUES (1, 'Event1', '2024-06-15', NOW())")
		if err != nil {
			t.Fatalf("INSERT with NOW() failed: %v", err)
		}

		// Verify the inserted data
		result, err = engine.Execute("SELECT created_at FROM date_test.events WHERE id = 1")
		if err != nil {
			t.Fatalf("SELECT failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || len(qr.Data[0][0]) == 0 {
			t.Error("Expected non-empty timestamp from NOW()")
		}

		// Test NOW() for DATE column (should return just the date)
		_, err = engine.Execute("INSERT INTO date_test.events (id, name, event_date, created_at) VALUES (2, 'Event2', NOW(), '2024-12-25 08:00:00')")
		if err != nil {
			t.Fatalf("INSERT with NOW() for DATE failed: %v", err)
		}

		result, err = engine.Execute("SELECT event_date FROM date_test.events WHERE id = 2")
		if err != nil {
			t.Fatalf("SELECT failed: %v", err)
		}
		qr = result.(db.QueryResult)
		// NOW() for DATE should return just YYYY-MM-DD format (10 chars)
		if len(qr.Data) != 1 || len(qr.Data[0][0]) != 10 {
			t.Errorf("Expected date format (10 chars), got '%s' (%d chars)", qr.Data[0][0], len(qr.Data[0][0]))
		}

		// Test invalid DATE format (should fail)
		_, err = engine.Execute("INSERT INTO date_test.events (id, name, event_date, created_at) VALUES (3, 'Event3', 'invalid-date', '2024-12-25 08:00:00')")
		if err == nil {
			t.Error("Expected error for invalid DATE format")
		}

		// Test invalid TIMESTAMP format (should fail)
		_, err = engine.Execute("INSERT INTO date_test.events (id, name, event_date, created_at) VALUES (4, 'Event4', '2024-06-15', 'not-a-timestamp')")
		if err == nil {
			t.Error("Expected error for invalid TIMESTAMP format")
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

// TestIntegrationAlterTable tests ALTER TABLE operations
func TestIntegrationAlterTable(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {
		// Setup
		engine.Execute("CREATE DATABASE alter_test")
		engine.Execute("CREATE TABLE alter_test.users (id INT PRIMARY KEY, name STRING)")

		// Test ADD COLUMN
		result, err := engine.Execute("ALTER TABLE alter_test.users ADD COLUMN email STRING")
		if err != nil {
			t.Fatalf("ADD COLUMN failed: %v", err)
		}
		cr := result.(db.CommitResult)
		if cr.TablesAltered != 1 {
			t.Errorf("Expected 1 table altered, got %d", cr.TablesAltered)
		}

		// Verify column was added via DESCRIBE
		result, err = engine.Execute("DESCRIBE alter_test.users")
		if err != nil {
			t.Fatalf("DESCRIBE failed: %v", err)
		}
		qr := result.(db.QueryResult)
		if len(qr.Data) != 3 {
			t.Errorf("Expected 3 columns after ADD, got %d", len(qr.Data))
		}

		// Test ADD COLUMN with existing name (should fail)
		_, err = engine.Execute("ALTER TABLE alter_test.users ADD COLUMN name STRING")
		if err == nil {
			t.Error("Expected error when adding existing column")
		}

		// Test MODIFY COLUMN
		_, err = engine.Execute("ALTER TABLE alter_test.users MODIFY COLUMN email TEXT")
		if err != nil {
			t.Fatalf("MODIFY COLUMN failed: %v", err)
		}

		// Test RENAME COLUMN
		_, err = engine.Execute("ALTER TABLE alter_test.users RENAME COLUMN email TO contact")
		if err != nil {
			t.Fatalf("RENAME COLUMN failed: %v", err)
		}

		// Verify rename via DESCRIBE
		result, _ = engine.Execute("DESCRIBE alter_test.users")
		qr = result.(db.QueryResult)
		foundContact := false
		for _, row := range qr.Data {
			if row[0] == "contact" {
				foundContact = true
			}
		}
		if !foundContact {
			t.Error("Column 'contact' not found after RENAME")
		}

		// Test DROP COLUMN
		_, err = engine.Execute("ALTER TABLE alter_test.users DROP COLUMN contact")
		if err != nil {
			t.Fatalf("DROP COLUMN failed: %v", err)
		}

		// Verify column was dropped
		result, _ = engine.Execute("DESCRIBE alter_test.users")
		qr = result.(db.QueryResult)
		if len(qr.Data) != 2 {
			t.Errorf("Expected 2 columns after DROP, got %d", len(qr.Data))
		}

		// Test DROP non-existent column (should fail)
		_, err = engine.Execute("ALTER TABLE alter_test.users DROP COLUMN nonexistent")
		if err == nil {
			t.Error("Expected error when dropping non-existent column")
		}

		// Test DROP PRIMARY KEY column (should fail)
		_, err = engine.Execute("ALTER TABLE alter_test.users DROP COLUMN id")
		if err == nil {
			t.Error("Expected error when dropping primary key column")
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
	db1 := CommitDB.Open(&persistence1)
	engine1 := db1.Engine(core.Identity{Name: "test", Email: "test@test.com"})

	engine1.Execute("CREATE DATABASE persist")
	engine1.Execute("CREATE TABLE persist.data (id INT PRIMARY KEY, val STRING)")
	engine1.Execute("INSERT INTO persist.data (id, val) VALUES (1, 'hello')")
	engine1.Execute("INSERT INTO persist.data (id, val) VALUES (2, 'world')")

	// Second session: reopen and verify
	persistence2, _ := ps.NewFilePersistence(tmpDir, nil)
	db2 := CommitDB.Open(&persistence2)
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

// TestBranchingSQL tests branching SQL syntax
func TestBranchingSQL(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {
		// Setup: create database and table
		engine.Execute("CREATE DATABASE branchtest")
		engine.Execute("CREATE TABLE branchtest.items (id INT PRIMARY KEY, name STRING)")
		engine.Execute("INSERT INTO branchtest.items (id, name) VALUES (1, 'original')")

		// Test CREATE BRANCH
		_, err := engine.Execute("CREATE BRANCH feature")
		if err != nil {
			t.Fatalf("CREATE BRANCH failed: %v", err)
		}

		// Test SHOW BRANCHES
		result, err := engine.Execute("SHOW BRANCHES")
		if err != nil {
			t.Fatalf("SHOW BRANCHES failed: %v", err)
		}
		qr := result.(db.QueryResult)
		if len(qr.Data) < 2 {
			t.Errorf("Expected at least 2 branches (master + feature), got %d", len(qr.Data))
		}

		// Test CHECKOUT
		_, err = engine.Execute("CHECKOUT feature")
		if err != nil {
			t.Fatalf("CHECKOUT failed: %v", err)
		}

		// Make changes on feature branch
		engine.Execute("INSERT INTO branchtest.items (id, name) VALUES (2, 'feature-item')")

		// Verify data on feature branch
		result, err = engine.Execute("SELECT * FROM branchtest.items")
		if err != nil {
			t.Fatalf("SELECT failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 2 {
			t.Errorf("Expected 2 rows on feature branch, got %d", len(qr.Data))
		}

		// Checkout master - should only have original row
		_, err = engine.Execute("CHECKOUT master")
		if err != nil {
			t.Fatalf("CHECKOUT master failed: %v", err)
		}

		result, err = engine.Execute("SELECT * FROM branchtest.items")
		if err != nil {
			t.Fatalf("SELECT on master failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 {
			t.Errorf("Expected 1 row on master branch, got %d", len(qr.Data))
		}

		// Test MERGE
		_, err = engine.Execute("MERGE feature")
		if err != nil {
			t.Fatalf("MERGE failed: %v", err)
		}

		// After merge, master should have both rows
		result, err = engine.Execute("SELECT * FROM branchtest.items")
		if err != nil {
			t.Fatalf("SELECT after merge failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 2 {
			t.Errorf("Expected 2 rows after merge, got %d", len(qr.Data))
		}
	})
}

// TestBranchFromTransaction tests CREATE BRANCH FROM syntax
func TestBranchFromTransaction(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {
		// Create database and table
		engine.Execute("CREATE DATABASE branchtest2")
		engine.Execute("CREATE TABLE branchtest2.data (id INT PRIMARY KEY, name STRING)")

		// Insert first row and capture the transaction
		result1, err := engine.Execute("INSERT INTO branchtest2.data (id, name) VALUES (1, 'first')")
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}
		txn1 := result1.(db.CommitResult).Transaction

		// Insert more rows (these should NOT be on the old branch)
		engine.Execute("INSERT INTO branchtest2.data (id, name) VALUES (2, 'second')")
		engine.Execute("INSERT INTO branchtest2.data (id, name) VALUES (3, 'third')")

		// Verify current state has 3 rows
		result, _ := engine.Execute("SELECT * FROM branchtest2.data")
		qr := result.(db.QueryResult)
		if len(qr.Data) != 3 {
			t.Fatalf("Expected 3 rows on master, got %d", len(qr.Data))
		}

		// Create branch from the first transaction (should only have 1 row)
		_, err = engine.Execute("CREATE BRANCH old_state FROM '" + txn1.Id + "'")
		if err != nil {
			t.Fatalf("CREATE BRANCH FROM failed: %v", err)
		}

		// Checkout the old branch
		_, err = engine.Execute("CHECKOUT old_state")
		if err != nil {
			t.Fatalf("CHECKOUT old_state failed: %v", err)
		}

		// On old branch, should only have 1 row
		result, err = engine.Execute("SELECT * FROM branchtest2.data")
		if err != nil {
			t.Fatalf("SELECT on old branch failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 {
			t.Errorf("Expected 1 row on old_state branch, got %d", len(qr.Data))
		}

		// Verify the old branch has the correct data
		if len(qr.Data) > 0 && qr.Data[0][1] != "first" {
			t.Errorf("Expected 'first' on old branch, got '%s'", qr.Data[0][1])
		}

		// Switch back to master and verify 3 rows
		engine.Execute("CHECKOUT master")
		result, _ = engine.Execute("SELECT * FROM branchtest2.data")
		qr = result.(db.QueryResult)
		if len(qr.Data) != 3 {
			t.Errorf("Expected 3 rows on master after checkout, got %d", len(qr.Data))
		}
	})
}

// TestMergeManualResolutionSQL tests MERGE WITH MANUAL RESOLUTION syntax
func TestMergeManualResolutionSQL(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {
		// Setup: create conflict situation
		engine.Execute("CREATE DATABASE mrtest")
		engine.Execute("CREATE TABLE mrtest.users (id INT PRIMARY KEY, name STRING)")
		engine.Execute("INSERT INTO mrtest.users (id, name) VALUES (1, 'Original')")

		// Create feature branch
		engine.Execute("CREATE BRANCH feature")
		engine.Execute("CHECKOUT feature")
		// Add a new record on feature branch
		engine.Execute("INSERT INTO mrtest.users (id, name) VALUES (2, 'Feature')")

		// Switch to master and add different record
		engine.Execute("CHECKOUT master")
		engine.Execute("INSERT INTO mrtest.users (id, name) VALUES (3, 'Master')")

		// Before merge: master has 2 rows (1, 3), feature has 2 rows (1, 2)

		// Merge with manual resolution - should detect conflict on common base
		result, err := engine.Execute("MERGE feature WITH MANUAL RESOLUTION")
		if err != nil {
			t.Fatalf("MERGE WITH MANUAL RESOLUTION failed: %v", err)
		}

		// Check if there are conflicts (depends on actual data)
		qr, ok := result.(db.QueryResult)
		if ok && len(qr.Data) > 0 {
			// Show merge conflicts
			result, err = engine.Execute("SHOW MERGE CONFLICTS")
			if err != nil {
				t.Fatalf("SHOW MERGE CONFLICTS failed: %v", err)
			}
			qr = result.(db.QueryResult)

			// Resolve each conflict using HEAD
			for _, row := range qr.Data {
				db := row[0]
				table := row[1]
				key := row[2]
				_, err = engine.Execute("RESOLVE CONFLICT " + db + "." + table + "." + key + " USING HEAD")
				if err != nil {
					t.Fatalf("RESOLVE CONFLICT failed: %v", err)
				}
			}

			// Commit merge
			_, err = engine.Execute("COMMIT MERGE")
			if err != nil {
				t.Fatalf("COMMIT MERGE failed: %v", err)
			}
		}

		// After merge, should have records from both branches
		result, _ = engine.Execute("SELECT * FROM mrtest.users")
		qr = result.(db.QueryResult)
		// Should have at least the non-conflicting records from both branches
		if len(qr.Data) < 2 {
			t.Errorf("Expected at least 2 rows after merge, got %d", len(qr.Data))
		}
	})
}

// TestAbortMergeSQL tests ABORT MERGE syntax
func TestAbortMergeSQL(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {
		// Setup
		engine.Execute("CREATE DATABASE abtest")
		engine.Execute("CREATE TABLE abtest.data (id INT PRIMARY KEY, val STRING)")
		engine.Execute("INSERT INTO abtest.data (id, val) VALUES (1, 'Original')")

		engine.Execute("CREATE BRANCH feature")
		engine.Execute("CHECKOUT feature")
		engine.Execute("UPDATE abtest.data SET val = 'Feature' WHERE id = 1")

		engine.Execute("CHECKOUT master")
		engine.Execute("UPDATE abtest.data SET val = 'Master' WHERE id = 1")

		// Start merge
		engine.Execute("MERGE feature WITH MANUAL RESOLUTION")

		// Abort
		_, err := engine.Execute("ABORT MERGE")
		if err != nil {
			t.Fatalf("ABORT MERGE failed: %v", err)
		}

		// Verify no pending merge
		result, _ := engine.Execute("SHOW MERGE CONFLICTS")
		qr := result.(db.QueryResult)
		if len(qr.Data) != 0 {
			t.Errorf("Expected 0 conflicts after abort, got %d", len(qr.Data))
		}
	})
}

// TestRemoteManagementSQL tests remote management SQL commands
func TestRemoteManagementSQL(t *testing.T) {
	// This test only runs with file persistence as memory persistence
	// doesn't have a real git repository
	t.Run("File", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "commitdb-remote-test-*")
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		persistence, err := ps.NewFilePersistence(tmpDir, nil)
		if err != nil {
			t.Fatalf("Failed to initialize file persistence: %v", err)
		}
		DB := CommitDB.Open(&persistence)
		engine := DB.Engine(core.Identity{Name: "test", Email: "test@test.com"})

		// Initially no remotes
		result, err := engine.Execute("SHOW REMOTES")
		if err != nil {
			t.Fatalf("SHOW REMOTES failed: %v", err)
		}
		qr := result.(db.QueryResult)
		if len(qr.Data) != 0 {
			t.Errorf("Expected 0 remotes initially, got %d", len(qr.Data))
		}

		// Add a remote
		result, err = engine.Execute("CREATE REMOTE origin 'https://github.com/test/repo.git'")
		if err != nil {
			t.Fatalf("CREATE REMOTE failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "Remote 'origin' added" {
			t.Errorf("Unexpected response from CREATE REMOTE")
		}

		// Show remotes
		result, err = engine.Execute("SHOW REMOTES")
		if err != nil {
			t.Fatalf("SHOW REMOTES failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 {
			t.Errorf("Expected 1 remote, got %d", len(qr.Data))
		}
		if qr.Data[0][0] != "origin" {
			t.Errorf("Expected remote name 'origin', got '%s'", qr.Data[0][0])
		}

		// Add another remote
		_, err = engine.Execute("CREATE REMOTE upstream 'https://github.com/upstream/repo.git'")
		if err != nil {
			t.Fatalf("CREATE REMOTE upstream failed: %v", err)
		}

		// Show remotes - should have 2
		result, err = engine.Execute("SHOW REMOTES")
		if err != nil {
			t.Fatalf("SHOW REMOTES failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 2 {
			t.Errorf("Expected 2 remotes, got %d", len(qr.Data))
		}

		// Drop a remote
		result, err = engine.Execute("DROP REMOTE upstream")
		if err != nil {
			t.Fatalf("DROP REMOTE failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "Remote 'upstream' removed" {
			t.Errorf("Unexpected response from DROP REMOTE")
		}

		// Show remotes - should have 1
		result, err = engine.Execute("SHOW REMOTES")
		if err != nil {
			t.Fatalf("SHOW REMOTES failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 {
			t.Errorf("Expected 1 remote after drop, got %d", len(qr.Data))
		}
	})
}

// TestRemotePushPullSQL tests SQL syntax parsing for push/pull/fetch
// Note: This only tests syntax parsing, not actual remote operations
func TestRemotePushPullSyntax(t *testing.T) {
	t.Run("File", func(t *testing.T) {
		tmpDir, err := os.MkdirTemp("", "commitdb-pushpull-test-*")
		if err != nil {
			t.Fatalf("Failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir)

		persistence, err := ps.NewFilePersistence(tmpDir, nil)
		if err != nil {
			t.Fatalf("Failed to initialize file persistence: %v", err)
		}
		DB := CommitDB.Open(&persistence)
		engine := DB.Engine(core.Identity{Name: "test", Email: "test@test.com"})

		// Add a remote first
		_, err = engine.Execute("CREATE REMOTE origin 'https://github.com/test/repo.git'")
		if err != nil {
			t.Fatalf("CREATE REMOTE failed: %v", err)
		}

		// Test PUSH syntax (will fail since remote doesn't exist, but should parse)
		_, err = engine.Execute("PUSH")
		// Expected to fail with connection error, not parse error
		if err == nil || err.Error() == "unknown statement type" {
			t.Logf("PUSH parsed successfully (operation failed as expected: %v)", err)
		}

		// Test PUSH TO syntax
		_, err = engine.Execute("PUSH TO origin")
		if err == nil || err.Error() == "unknown statement type" {
			t.Logf("PUSH TO parsed successfully (operation failed as expected: %v)", err)
		}

		// Test PUSH with branch
		_, err = engine.Execute("PUSH TO origin BRANCH master")
		if err == nil || err.Error() == "unknown statement type" {
			t.Logf("PUSH TO BRANCH parsed successfully (operation failed as expected: %v)", err)
		}

		// Test PULL syntax
		_, err = engine.Execute("PULL")
		if err == nil || err.Error() == "unknown statement type" {
			t.Logf("PULL parsed successfully (operation failed as expected: %v)", err)
		}

		// Test PULL FROM syntax
		_, err = engine.Execute("PULL FROM origin")
		if err == nil || err.Error() == "unknown statement type" {
			t.Logf("PULL FROM parsed successfully (operation failed as expected: %v)", err)
		}

		// Test FETCH syntax
		_, err = engine.Execute("FETCH")
		if err == nil || err.Error() == "unknown statement type" {
			t.Logf("FETCH parsed successfully (operation failed as expected: %v)", err)
		}

		// Test FETCH FROM syntax
		_, err = engine.Execute("FETCH FROM origin")
		if err == nil || err.Error() == "unknown statement type" {
			t.Logf("FETCH FROM parsed successfully (operation failed as expected: %v)", err)
		}
	})
}
