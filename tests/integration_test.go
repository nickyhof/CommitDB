package tests

import (
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/storage/filesystem"
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

		// Test GROUP BY with column and COUNT(*)
		result, err = engine.Execute("SELECT region, COUNT(*) FROM sales.orders GROUP BY region")
		if err != nil {
			t.Fatalf("Failed to execute GROUP BY with column + COUNT: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 2 {
			t.Errorf("Expected 2 groups (East, West), got %d", len(qr.Data))
		}

		// Test GROUP BY with column and SUM aggregate
		result, err = engine.Execute("SELECT region, SUM(amount) FROM sales.orders GROUP BY region")
		if err != nil {
			t.Fatalf("Failed to execute GROUP BY with column + SUM: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 2 {
			t.Errorf("Expected 2 groups, got %d", len(qr.Data))
		}

		// Test GROUP BY with column and multiple aggregates
		result, err = engine.Execute("SELECT region, COUNT(*), AVG(amount) FROM sales.orders GROUP BY region")
		if err != nil {
			t.Fatalf("Failed to execute GROUP BY with column + multiple aggregates: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 2 {
			t.Errorf("Expected 2 groups, got %d", len(qr.Data))
		}
		// Verify columns include region and aggregates
		if len(qr.Columns) < 2 {
			t.Errorf("Expected at least 2 columns (region + aggregates), got %d", len(qr.Columns))
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

// TestIntegrationJsonType tests JSON column type and JSON functions
func TestIntegrationJsonType(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {

		engine.Execute("CREATE DATABASE json_test")
		engine.Execute("CREATE TABLE json_test.documents (id INT PRIMARY KEY, name STRING, data JSON)")

		// Verify JSON type in schema
		result, err := engine.Execute("DESCRIBE json_test.documents")
		if err != nil {
			t.Fatalf("DESCRIBE failed: %v", err)
		}
		qr := result.(db.QueryResult)
		foundJson := false
		for _, row := range qr.Data {
			if row[0] == "data" && row[1] == "JSON" {
				foundJson = true
			}
		}
		if !foundJson {
			t.Error("JSON type not found in schema")
		}

		// Insert valid JSON
		_, err = engine.Execute(`INSERT INTO json_test.documents (id, name, data) VALUES (1, 'Doc1', '{"name":"Alice","age":30,"tags":["admin","user"]}')`)
		if err != nil {
			t.Fatalf("INSERT with valid JSON failed: %v", err)
		}

		// Insert another JSON document
		_, err = engine.Execute(`INSERT INTO json_test.documents (id, name, data) VALUES (2, 'Doc2', '{"name":"Bob","age":25}')`)
		if err != nil {
			t.Fatalf("INSERT second JSON failed: %v", err)
		}

		// Test JSON_EXTRACT with path
		result, err = engine.Execute(`SELECT JSON_EXTRACT(data, '$.name') FROM json_test.documents WHERE id = 1`)
		if err != nil {
			t.Fatalf("JSON_EXTRACT failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "Alice" {
			t.Errorf("JSON_EXTRACT: expected 'Alice', got '%s'", qr.Data[0][0])
		}

		// Test JSON_EXTRACT nested
		result, err = engine.Execute(`SELECT JSON_EXTRACT(data, '$.age') FROM json_test.documents WHERE id = 1`)
		if err != nil {
			t.Fatalf("JSON_EXTRACT age failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "30" {
			t.Errorf("JSON_EXTRACT age: expected '30', got '%s'", qr.Data[0][0])
		}

		// Test JSON_KEYS
		result, err = engine.Execute(`SELECT JSON_KEYS(data) FROM json_test.documents WHERE id = 2`)
		if err != nil {
			t.Fatalf("JSON_KEYS failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "age,name" {
			t.Errorf("JSON_KEYS: expected 'age,name', got '%s'", qr.Data[0][0])
		}

		// Test JSON_LENGTH
		result, err = engine.Execute(`SELECT JSON_LENGTH(data) FROM json_test.documents WHERE id = 1`)
		if err != nil {
			t.Fatalf("JSON_LENGTH failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "3" {
			t.Errorf("JSON_LENGTH: expected '3', got '%s'", qr.Data[0][0])
		}

		// Test JSON_TYPE
		result, err = engine.Execute(`SELECT JSON_TYPE(data) FROM json_test.documents WHERE id = 1`)
		if err != nil {
			t.Fatalf("JSON_TYPE failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "object" {
			t.Errorf("JSON_TYPE: expected 'object', got '%s'", qr.Data[0][0])
		}

		// Test JSON_CONTAINS
		result, err = engine.Execute(`SELECT JSON_CONTAINS(data, 'Alice') FROM json_test.documents WHERE id = 1`)
		if err != nil {
			t.Fatalf("JSON_CONTAINS failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "1" {
			t.Errorf("JSON_CONTAINS: expected '1', got '%s'", qr.Data[0][0])
		}

		// Test invalid JSON format (should fail)
		_, err = engine.Execute(`INSERT INTO json_test.documents (id, name, data) VALUES (3, 'Doc3', 'not-valid-json')`)
		if err == nil {
			t.Error("Expected error for invalid JSON format")
		}
	})
}

// TestIntegrationBulkInsert tests bulk INSERT with multiple value rows
func TestIntegrationBulkInsert(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {

		engine.Execute("CREATE DATABASE bulk_test")
		engine.Execute("CREATE TABLE bulk_test.items (id INT PRIMARY KEY, name STRING, value INT)")

		// Test bulk insert with multiple rows
		result, err := engine.Execute("INSERT INTO bulk_test.items (id, name, value) VALUES (1, 'Item1', 100), (2, 'Item2', 200), (3, 'Item3', 300)")
		if err != nil {
			t.Fatalf("Bulk INSERT failed: %v", err)
		}
		cr := result.(db.CommitResult)
		if cr.RecordsWritten != 3 {
			t.Errorf("Expected 3 records written, got %d", cr.RecordsWritten)
		}

		// Verify all rows were inserted
		result, err = engine.Execute("SELECT * FROM bulk_test.items ORDER BY id ASC")
		if err != nil {
			t.Fatalf("SELECT after bulk INSERT failed: %v", err)
		}
		qr := result.(db.QueryResult)
		if len(qr.Data) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(qr.Data))
		}
		if qr.Data[0][1] != "Item1" || qr.Data[1][1] != "Item2" || qr.Data[2][1] != "Item3" {
			t.Errorf("Unexpected data: %v", qr.Data)
		}

		// Test bulk insert with single row (backward compatibility)
		result, err = engine.Execute("INSERT INTO bulk_test.items (id, name, value) VALUES (4, 'Item4', 400)")
		if err != nil {
			t.Fatalf("Single row INSERT failed: %v", err)
		}
		cr = result.(db.CommitResult)
		if cr.RecordsWritten != 1 {
			t.Errorf("Expected 1 record written, got %d", cr.RecordsWritten)
		}
	})
}

// TestIntegrationCopyInto tests COPY INTO for bulk CSV import/export
func TestIntegrationCopyInto(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {

		engine.Execute("CREATE DATABASE copy_test")
		engine.Execute("CREATE TABLE copy_test.users (id INT PRIMARY KEY, name STRING, email STRING)")

		// Insert some data
		engine.Execute("INSERT INTO copy_test.users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')")
		engine.Execute("INSERT INTO copy_test.users (id, name, email) VALUES (2, 'Bob', 'bob@test.com')")
		engine.Execute("INSERT INTO copy_test.users (id, name, email) VALUES (3, 'Charlie', 'charlie@test.com')")

		// Test export to CSV file
		exportPath := t.TempDir() + "/export.csv"
		result, err := engine.Execute("COPY INTO '" + exportPath + "' FROM copy_test.users")
		if err != nil {
			t.Fatalf("COPY INTO file failed: %v", err)
		}
		cr := result.(db.CommitResult)
		if cr.RecordsWritten != 3 {
			t.Errorf("Expected 3 records exported, got %d", cr.RecordsWritten)
		}

		// Verify CSV file exists and has content
		content, err := os.ReadFile(exportPath)
		if err != nil {
			t.Fatalf("Failed to read exported file: %v", err)
		}
		if !strings.Contains(string(content), "Alice") {
			t.Error("Exported CSV should contain 'Alice'")
		}
		if !strings.Contains(string(content), "id,name,email") {
			t.Error("Exported CSV should contain header")
		}

		// Create a new table for import test
		engine.Execute("CREATE TABLE copy_test.imported (id INT PRIMARY KEY, name STRING, email STRING)")

		// Test import from CSV file
		result, err = engine.Execute("COPY INTO copy_test.imported FROM '" + exportPath + "'")
		if err != nil {
			t.Fatalf("COPY INTO table failed: %v", err)
		}
		cr = result.(db.CommitResult)
		if cr.RecordsWritten != 3 {
			t.Errorf("Expected 3 records imported, got %d", cr.RecordsWritten)
		}

		// Verify imported data
		result, err = engine.Execute("SELECT * FROM copy_test.imported")
		if err != nil {
			t.Fatalf("SELECT after import failed: %v", err)
		}
		qr := result.(db.QueryResult)
		if len(qr.Data) != 3 {
			t.Errorf("Expected 3 rows after import, got %d", len(qr.Data))
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

// TestRemotePushPullSQL tests actual push/pull/fetch operations using a local bare repo
func TestRemotePushPullSyntax(t *testing.T) {
	t.Run("File", func(t *testing.T) {
		// Create a bare git repo using go-git directly
		remoteDir, err := os.MkdirTemp("", "commitdb-bare-remote-*")
		if err != nil {
			t.Fatalf("Failed to create remote temp dir: %v", err)
		}
		defer os.RemoveAll(remoteDir)

		// Initialize bare repo using go-git filesystem storage (no worktree = bare)
		bareFS := osfs.New(remoteDir)
		bareStorer := filesystem.NewStorage(bareFS, cache.NewObjectLRUDefault())
		_, err = git.Init(bareStorer)
		if err != nil {
			t.Fatalf("Failed to init bare repo: %v", err)
		}

		// Create the main CommitDB repo
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

		// Create some data to push
		_, err = engine.Execute("CREATE DATABASE testdb")
		if err != nil {
			t.Fatalf("CREATE DATABASE failed: %v", err)
		}
		_, err = engine.Execute("CREATE TABLE testdb.items (id INT PRIMARY KEY, name STRING)")
		if err != nil {
			t.Fatalf("CREATE TABLE failed: %v", err)
		}
		_, err = engine.Execute("INSERT INTO testdb.items (id, name) VALUES (1, 'Item1')")
		if err != nil {
			t.Fatalf("INSERT failed: %v", err)
		}

		// Add the local bare repo as a remote
		_, err = engine.Execute("CREATE REMOTE origin '" + remoteDir + "'")
		if err != nil {
			t.Fatalf("CREATE REMOTE failed: %v", err)
		}

		// Verify remote was added
		result, err := engine.Execute("SHOW REMOTES")
		if err != nil {
			t.Fatalf("SHOW REMOTES failed: %v", err)
		}
		qr := result.(db.QueryResult)
		if len(qr.Data) != 1 || qr.Data[0][0] != "origin" {
			t.Errorf("Expected remote 'origin', got: %v", qr.Data)
		}

		// Test PUSH to local bare repo
		_, err = engine.Execute("PUSH TO origin")
		if err != nil {
			t.Fatalf("PUSH TO origin failed: %v", err)
		}
		t.Log("PUSH TO origin succeeded")

		// Test FETCH from the bare remote
		_, err = engine.Execute("FETCH FROM origin")
		if err != nil {
			t.Fatalf("FETCH FROM origin failed: %v", err)
		}
		t.Log("FETCH FROM origin succeeded")

		// Create a second repo to test PULL
		tmpDir2, err := os.MkdirTemp("", "commitdb-pull-test-*")
		if err != nil {
			t.Fatalf("Failed to create second temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir2)

		persistence2, err := ps.NewFilePersistence(tmpDir2, nil)
		if err != nil {
			t.Fatalf("Failed to initialize second persistence: %v", err)
		}
		DB2 := CommitDB.Open(&persistence2)
		engine2 := DB2.Engine(core.Identity{Name: "test2", Email: "test2@test.com"})

		// Add the same remote
		_, err = engine2.Execute("CREATE REMOTE origin '" + remoteDir + "'")
		if err != nil {
			t.Fatalf("CREATE REMOTE in repo2 failed: %v", err)
		}

		// First fetch to get remote refs
		_, err = engine2.Execute("FETCH FROM origin")
		if err != nil {
			t.Fatalf("FETCH FROM origin in repo2 failed: %v", err)
		}
		t.Log("FETCH FROM origin in repo2 succeeded")

		// Test PULL from the remote
		_, err = engine2.Execute("PULL FROM origin")
		if err != nil {
			t.Fatalf("PULL FROM origin failed: %v", err)
		}
		t.Log("PULL FROM origin succeeded")

		// Verify data was pulled - the database should now exist
		result, err = engine2.Execute("SHOW DATABASES")
		if err != nil {
			t.Fatalf("SHOW DATABASES after PULL failed: %v", err)
		}
		qr = result.(db.QueryResult)
		found := false
		for _, row := range qr.Data {
			if row[0] == "testdb" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected to find 'testdb' after PULL, got: %v", qr.Data)
		}

		// Test DROP REMOTE
		_, err = engine.Execute("DROP REMOTE origin")
		if err != nil {
			t.Fatalf("DROP REMOTE failed: %v", err)
		}

		// Verify remote is removed
		result, err = engine.Execute("SHOW REMOTES")
		if err != nil {
			t.Fatalf("SHOW REMOTES after DROP failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 0 {
			t.Errorf("Expected 0 remotes after DROP, got %d", len(qr.Data))
		}
	})
}

// TestShareManagementSQL tests share-related SQL commands with actual git clone
func TestShareManagementSQL(t *testing.T) {
	t.Run("File", func(t *testing.T) {
		// Create a source CommitDB repo with sample data
		sourceDir, err := os.MkdirTemp("", "commitdb-share-source-*")
		if err != nil {
			t.Fatalf("Failed to create source temp dir: %v", err)
		}
		defer os.RemoveAll(sourceDir)

		// Initialize source repo with data
		sourcePersistence, err := ps.NewFilePersistence(sourceDir, nil)
		if err != nil {
			t.Fatalf("Failed to initialize source persistence: %v", err)
		}
		sourceDB := CommitDB.Open(&sourcePersistence)
		sourceEngine := sourceDB.Engine(core.Identity{Name: "source", Email: "source@test.com"})

		// Create test data in source repo
		_, err = sourceEngine.Execute("CREATE DATABASE sample")
		if err != nil {
			t.Fatalf("Failed to create database in source: %v", err)
		}
		_, err = sourceEngine.Execute("CREATE TABLE sample.users (id INT PRIMARY KEY, name STRING)")
		if err != nil {
			t.Fatalf("Failed to create table in source: %v", err)
		}
		_, err = sourceEngine.Execute("INSERT INTO sample.users (id, name) VALUES (1, 'Alice')")
		if err != nil {
			t.Fatalf("Failed to insert into source: %v", err)
		}
		_, err = sourceEngine.Execute("INSERT INTO sample.users (id, name) VALUES (2, 'Bob')")
		if err != nil {
			t.Fatalf("Failed to insert into source: %v", err)
		}

		// Create a bare repo to act as the share source
		bareDir, err := os.MkdirTemp("", "commitdb-share-bare-*")
		if err != nil {
			t.Fatalf("Failed to create bare temp dir: %v", err)
		}
		defer os.RemoveAll(bareDir)

		// Initialize bare repo using go-git
		bareFS := osfs.New(bareDir)
		bareStorer := filesystem.NewStorage(bareFS, cache.NewObjectLRUDefault())
		_, err = git.Init(bareStorer)
		if err != nil {
			t.Fatalf("Failed to init bare repo: %v", err)
		}

		// Add bare repo as remote and push source data to it
		_, err = sourceEngine.Execute("CREATE REMOTE origin '" + bareDir + "'")
		if err != nil {
			t.Fatalf("CREATE REMOTE in source failed: %v", err)
		}
		_, err = sourceEngine.Execute("PUSH TO origin")
		if err != nil {
			t.Fatalf("PUSH to bare repo failed: %v", err)
		}
		t.Log("Source data pushed to bare repo")

		// Create the main CommitDB repo that will use the share
		tmpDir, err := os.MkdirTemp("", "commitdb-share-test-*")
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

		// Test SHOW SHARES (should be empty)
		result, err := engine.Execute("SHOW SHARES")
		if err != nil {
			t.Fatalf("SHOW SHARES failed: %v", err)
		}
		qr := result.(db.QueryResult)
		if len(qr.Data) != 0 {
			t.Errorf("Expected 0 shares initially, got %d", len(qr.Data))
		}
		if len(qr.Columns) != 2 || qr.Columns[0] != "Name" || qr.Columns[1] != "URL" {
			t.Errorf("Expected columns [Name, URL], got %v", qr.Columns)
		}

		// Test CREATE SHARE - actually clones from the bare repo
		_, err = engine.Execute("CREATE SHARE external FROM '" + bareDir + "'")
		if err != nil {
			t.Fatalf("CREATE SHARE failed: %v", err)
		}
		t.Log("CREATE SHARE succeeded")

		// Test SHOW SHARES (should have 1 now)
		result, err = engine.Execute("SHOW SHARES")
		if err != nil {
			t.Fatalf("SHOW SHARES failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 {
			t.Errorf("Expected 1 share, got %d", len(qr.Data))
		}
		if len(qr.Data) > 0 && qr.Data[0][0] != "external" {
			t.Errorf("Expected share name 'external', got '%s'", qr.Data[0][0])
		}

		// Test SELECT from share using 3-level naming (share.database.table)
		result, err = engine.Execute("SELECT * FROM external.sample.users")
		if err != nil {
			t.Fatalf("SELECT from share failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 2 {
			t.Errorf("Expected 2 rows from share, got %d", len(qr.Data))
		}

		// Verify we can find Alice and Bob
		names := make(map[string]bool)
		nameIdx := -1
		for i, col := range qr.Columns {
			if col == "name" {
				nameIdx = i
				break
			}
		}
		if nameIdx >= 0 {
			for _, row := range qr.Data {
				names[row[nameIdx]] = true
			}
		}
		if !names["Alice"] || !names["Bob"] {
			t.Errorf("Expected to find Alice and Bob in share data, got: %v", names)
		}

		// Create local data to test JOIN with share
		_, err = engine.Execute("CREATE DATABASE local")
		if err != nil {
			t.Fatalf("CREATE DATABASE local failed: %v", err)
		}
		_, err = engine.Execute("CREATE TABLE local.orders (id INT PRIMARY KEY, user_id INT, product STRING)")
		if err != nil {
			t.Fatalf("CREATE TABLE orders failed: %v", err)
		}
		_, err = engine.Execute("INSERT INTO local.orders (id, user_id, product) VALUES (100, 1, 'Widget')")
		if err != nil {
			t.Fatalf("INSERT order 1 failed: %v", err)
		}
		_, err = engine.Execute("INSERT INTO local.orders (id, user_id, product) VALUES (101, 2, 'Gadget')")
		if err != nil {
			t.Fatalf("INSERT order 2 failed: %v", err)
		}

		// Test JOIN between local table and shared table using 3-level naming
		result, err = engine.Execute("SELECT o.product, u.name FROM local.orders o JOIN external.sample.users u ON o.user_id = u.id")
		if err != nil {
			t.Fatalf("JOIN with share failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 2 {
			t.Errorf("Expected 2 rows from JOIN, got %d", len(qr.Data))
		}
		t.Log("JOIN between local and share succeeded")

		// Test SYNC SHARE - should succeed now that the share exists
		_, err = engine.Execute("SYNC SHARE external")
		if err != nil {
			t.Fatalf("SYNC SHARE failed: %v", err)
		}
		t.Log("SYNC SHARE succeeded")

		// Test DROP SHARE
		_, err = engine.Execute("DROP SHARE external")
		if err != nil {
			t.Fatalf("DROP SHARE failed: %v", err)
		}

		// Verify share is removed
		result, err = engine.Execute("SHOW SHARES")
		if err != nil {
			t.Fatalf("SHOW SHARES after DROP failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 0 {
			t.Errorf("Expected 0 shares after DROP, got %d", len(qr.Data))
		}

		// Verify share directory is removed
		shareDir := tmpDir + "/.shares/external"
		if _, err := os.Stat(shareDir); !os.IsNotExist(err) {
			t.Error("Expected share directory to be removed")
		}
	})
}

// TestIntegrationViews tests regular (non-materialized) views
func TestIntegrationViews(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {
		// Setup: Create database and table with test data
		engine.Execute("CREATE DATABASE viewdb")
		engine.Execute("CREATE TABLE viewdb.users (id INT PRIMARY KEY, name STRING, active INT, city STRING)")
		engine.Execute("INSERT INTO viewdb.users (id, name, active, city) VALUES (1, 'Alice', 1, 'NYC')")
		engine.Execute("INSERT INTO viewdb.users (id, name, active, city) VALUES (2, 'Bob', 0, 'LA')")
		engine.Execute("INSERT INTO viewdb.users (id, name, active, city) VALUES (3, 'Charlie', 1, 'NYC')")
		engine.Execute("INSERT INTO viewdb.users (id, name, active, city) VALUES (4, 'Diana', 1, 'Chicago')")
		engine.Execute("INSERT INTO viewdb.users (id, name, active, city) VALUES (5, 'Eve', 0, 'NYC')")

		// Test CREATE VIEW
		_, err := engine.Execute("CREATE VIEW viewdb.active_users AS SELECT * FROM viewdb.users WHERE active = 1")
		if err != nil {
			t.Fatalf("CREATE VIEW failed: %v", err)
		}

		// Test SHOW VIEWS
		result, err := engine.Execute("SHOW VIEWS IN viewdb")
		if err != nil {
			t.Fatalf("SHOW VIEWS failed: %v", err)
		}
		qr := result.(db.QueryResult)
		if len(qr.Data) != 1 {
			t.Errorf("Expected 1 view, got %d", len(qr.Data))
		}

		// Test SELECT from view
		result, err = engine.Execute("SELECT * FROM viewdb.active_users")
		if err != nil {
			t.Fatalf("SELECT from view failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 3 {
			t.Errorf("Expected 3 active users, got %d", len(qr.Data))
		}

		// View should update when underlying data changes
		engine.Execute("INSERT INTO viewdb.users (id, name, active, city) VALUES (6, 'Frank', 1, 'LA')")
		result, err = engine.Execute("SELECT * FROM viewdb.active_users")
		if err != nil {
			t.Fatalf("SELECT from view after INSERT failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 4 {
			t.Errorf("Expected 4 active users after INSERT, got %d", len(qr.Data))
		}

		// Test DROP VIEW
		_, err = engine.Execute("DROP VIEW viewdb.active_users")
		if err != nil {
			t.Fatalf("DROP VIEW failed: %v", err)
		}

		// Verify view is gone
		result, err = engine.Execute("SHOW VIEWS IN viewdb")
		if err != nil {
			t.Fatalf("SHOW VIEWS after DROP failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 0 {
			t.Errorf("Expected 0 views after DROP, got %d", len(qr.Data))
		}

		// Test DROP VIEW IF EXISTS (no error on non-existent view)
		_, err = engine.Execute("DROP VIEW IF EXISTS viewdb.nonexistent")
		if err != nil {
			t.Errorf("DROP VIEW IF EXISTS should not error: %v", err)
		}
	})
}

// TestIntegrationMaterializedViews tests materialized views with caching
func TestIntegrationMaterializedViews(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {
		// Setup
		engine.Execute("CREATE DATABASE matdb")
		engine.Execute("CREATE TABLE matdb.orders (id INT PRIMARY KEY, product STRING, amount INT, region STRING)")
		engine.Execute("INSERT INTO matdb.orders (id, product, amount, region) VALUES (1, 'Widget', 100, 'East')")
		engine.Execute("INSERT INTO matdb.orders (id, product, amount, region) VALUES (2, 'Gadget', 200, 'West')")
		engine.Execute("INSERT INTO matdb.orders (id, product, amount, region) VALUES (3, 'Widget', 150, 'East')")

		// Create materialized view
		_, err := engine.Execute("CREATE MATERIALIZED VIEW matdb.east_orders AS SELECT * FROM matdb.orders WHERE region = 'East'")
		if err != nil {
			t.Fatalf("CREATE MATERIALIZED VIEW failed: %v", err)
		}

		// SHOW VIEWS should show it as materialized
		result, err := engine.Execute("SHOW VIEWS IN matdb")
		if err != nil {
			t.Fatalf("SHOW VIEWS failed: %v", err)
		}
		qr := result.(db.QueryResult)
		if len(qr.Data) != 1 {
			t.Fatalf("Expected 1 view, got %d", len(qr.Data))
		}
		// Check materialized column
		matIdx := -1
		for i, col := range qr.Columns {
			if col == "materialized" {
				matIdx = i
			}
		}
		if matIdx >= 0 && qr.Data[0][matIdx] != "YES" {
			t.Errorf("Expected materialized=YES, got %s", qr.Data[0][matIdx])
		}

		// Query materialized view - should return cached data
		result, err = engine.Execute("SELECT * FROM matdb.east_orders")
		if err != nil {
			t.Fatalf("SELECT from materialized view failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 2 {
			t.Errorf("Expected 2 east orders, got %d", len(qr.Data))
		}

		// Add new data - materialized view should NOT update automatically
		engine.Execute("INSERT INTO matdb.orders (id, product, amount, region) VALUES (4, 'Gizmo', 300, 'East')")

		// Query again - should still show old cached data (2 rows)
		result, err = engine.Execute("SELECT * FROM matdb.east_orders")
		if err != nil {
			t.Fatalf("SELECT from materialized view after INSERT failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 2 {
			t.Errorf("Materialized view should still have 2 rows before REFRESH, got %d", len(qr.Data))
		}

		// REFRESH VIEW to update cached data
		_, err = engine.Execute("REFRESH VIEW matdb.east_orders")
		if err != nil {
			t.Fatalf("REFRESH VIEW failed: %v", err)
		}

		// Now should show updated data
		result, err = engine.Execute("SELECT * FROM matdb.east_orders")
		if err != nil {
			t.Fatalf("SELECT from materialized view after REFRESH failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 3 {
			t.Errorf("Expected 3 east orders after REFRESH, got %d", len(qr.Data))
		}

		// DROP VIEW should work for materialized views too
		_, err = engine.Execute("DROP VIEW matdb.east_orders")
		if err != nil {
			t.Fatalf("DROP VIEW (materialized) failed: %v", err)
		}

		// Verify it's gone
		result, err = engine.Execute("SHOW VIEWS IN matdb")
		if err != nil {
			t.Fatalf("SHOW VIEWS after DROP failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 0 {
			t.Errorf("Expected 0 views after DROP, got %d", len(qr.Data))
		}
	})
}

// TestIntegrationViewErrors tests error handling for views
func TestIntegrationViewErrors(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {
		engine.Execute("CREATE DATABASE errdb")
		engine.Execute("CREATE TABLE errdb.data (id INT PRIMARY KEY, value INT)")
		engine.Execute("CREATE VIEW errdb.myview AS SELECT * FROM errdb.data")

		// REFRESH VIEW on non-materialized view should fail
		_, err := engine.Execute("REFRESH VIEW errdb.myview")
		if err == nil {
			t.Error("REFRESH VIEW on non-materialized view should error")
		}

		// DROP VIEW without IF EXISTS on non-existent view should fail
		_, err = engine.Execute("DROP VIEW errdb.nonexistent")
		if err == nil {
			t.Error("DROP VIEW on non-existent view should error")
		}
	})
}

// TestIntegrationTimeTravelQueries tests querying data at specific transactions
func TestIntegrationTimeTravelQueries(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {
		// Setup
		engine.Execute("CREATE DATABASE ttdb")
		engine.Execute("CREATE TABLE ttdb.users (id INT PRIMARY KEY, name STRING, status STRING)")

		// Insert first record and capture transaction
		result1, err := engine.Execute("INSERT INTO ttdb.users (id, name, status) VALUES (1, 'Alice', 'active')")
		if err != nil {
			t.Fatalf("First INSERT failed: %v", err)
		}
		txn1 := result1.(db.CommitResult).Transaction.Id
		if txn1 == "" {
			t.Fatal("Expected transaction ID from first INSERT")
		}

		// Insert second record and capture transaction
		result2, err := engine.Execute("INSERT INTO ttdb.users (id, name, status) VALUES (2, 'Bob', 'active')")
		if err != nil {
			t.Fatalf("Second INSERT failed: %v", err)
		}
		txn2 := result2.(db.CommitResult).Transaction.Id

		// Update Alice's status and capture transaction
		result3, err := engine.Execute("UPDATE ttdb.users SET status = 'inactive' WHERE id = 1")
		if err != nil {
			t.Fatalf("UPDATE failed: %v", err)
		}
		txn3 := result3.(db.CommitResult).Transaction.Id

		// Current state: Alice (inactive), Bob (active)
		result, err := engine.Execute("SELECT * FROM ttdb.users ORDER BY id")
		if err != nil {
			t.Fatalf("Current SELECT failed: %v", err)
		}
		qr := result.(db.QueryResult)
		if len(qr.Data) != 2 {
			t.Errorf("Expected 2 rows in current state, got %d", len(qr.Data))
		}

		// Query at transaction 1: Only Alice (active)
		result, err = engine.Execute("SELECT * FROM ttdb.users AS OF '" + txn1 + "'")
		if err != nil {
			t.Fatalf("Time-travel query to txn1 failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 {
			t.Errorf("Expected 1 row at txn1, got %d", len(qr.Data))
		}
		if len(qr.Data) > 0 && qr.Data[0][1] != "Alice" {
			t.Errorf("Expected Alice at txn1, got %s", qr.Data[0][1])
		}

		// Query at transaction 2: Alice and Bob (both active)
		result, err = engine.Execute("SELECT * FROM ttdb.users AS OF '" + txn2 + "' ORDER BY id")
		if err != nil {
			t.Fatalf("Time-travel query to txn2 failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 2 {
			t.Errorf("Expected 2 rows at txn2, got %d", len(qr.Data))
		}
		// At txn2, Alice should still be active
		if len(qr.Data) > 0 {
			// Find status column index
			statusIdx := -1
			for i, col := range qr.Columns {
				if col == "status" {
					statusIdx = i
					break
				}
			}
			if statusIdx >= 0 && qr.Data[0][statusIdx] != "active" {
				t.Errorf("Expected Alice to be 'active' at txn2, got %s", qr.Data[0][statusIdx])
			}
		}

		// Query at transaction 3: Alice (inactive), Bob (active)
		result, err = engine.Execute("SELECT * FROM ttdb.users AS OF '" + txn3 + "' ORDER BY id")
		if err != nil {
			t.Fatalf("Time-travel query to txn3 failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 2 {
			t.Errorf("Expected 2 rows at txn3, got %d", len(qr.Data))
		}

		// Test time-travel with WHERE clause
		result, err = engine.Execute("SELECT name FROM ttdb.users AS OF '" + txn2 + "' WHERE id = 1")
		if err != nil {
			t.Fatalf("Time-travel query with WHERE failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 {
			t.Errorf("Expected 1 row with WHERE, got %d", len(qr.Data))
		}
		if len(qr.Data) > 0 && qr.Data[0][0] != "Alice" {
			t.Errorf("Expected name 'Alice', got %s", qr.Data[0][0])
		}

		// Test error for invalid transaction
		_, err = engine.Execute("SELECT * FROM ttdb.users AS OF 'invalid_transaction_id'")
		if err == nil {
			t.Error("Expected error for invalid transaction ID")
		}

		// CRITICAL: Verify time-travel queries don't affect Git state
		// Run a time-travel query, then insert new data, then verify current state
		_, err = engine.Execute("SELECT * FROM ttdb.users AS OF '" + txn1 + "'")
		if err != nil {
			t.Fatalf("Time-travel before insert failed: %v", err)
		}

		// Insert new record AFTER time-travel query
		result4, err := engine.Execute("INSERT INTO ttdb.users (id, name, status) VALUES (3, 'Charlie', 'active')")
		if err != nil {
			t.Fatalf("INSERT after time-travel failed: %v", err)
		}
		txn4 := result4.(db.CommitResult).Transaction.Id
		if txn4 == "" {
			t.Error("Expected transaction ID from INSERT after time-travel")
		}

		// Verify current state has all 3 rows
		result, err = engine.Execute("SELECT * FROM ttdb.users ORDER BY id")
		if err != nil {
			t.Fatalf("SELECT after time-travel+insert failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 3 {
			t.Errorf("Expected 3 rows after time-travel+insert, got %d", len(qr.Data))
		}

		// Verify we can still time-travel to old transactions
		result, err = engine.Execute("SELECT * FROM ttdb.users AS OF '" + txn2 + "'")
		if err != nil {
			t.Fatalf("Time-travel after insert failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 2 {
			t.Errorf("Expected 2 rows at txn2 after current inserts, got %d", len(qr.Data))
		}

		// Verify we can time-travel to the newest transaction too
		result, err = engine.Execute("SELECT * FROM ttdb.users AS OF '" + txn4 + "'")
		if err != nil {
			t.Fatalf("Time-travel to new txn failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 3 {
			t.Errorf("Expected 3 rows at txn4, got %d", len(qr.Data))
		}
	})
}

// TestIntegrationTimeTravelOnViews tests time-travel queries on views
func TestIntegrationTimeTravelOnViews(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {
		// Setup
		engine.Execute("CREATE DATABASE viewttdb")
		engine.Execute("CREATE TABLE viewttdb.orders (id INT PRIMARY KEY, product STRING, amount INT)")

		// Insert initial data
		engine.Execute("INSERT INTO viewttdb.orders (id, product, amount) VALUES (1, 'Widget', 100)")
		engine.Execute("INSERT INTO viewttdb.orders (id, product, amount) VALUES (2, 'Gadget', 200)")

		// Create a regular view
		_, err := engine.Execute("CREATE VIEW viewttdb.all_orders AS SELECT * FROM viewttdb.orders")
		if err != nil {
			t.Fatalf("CREATE VIEW failed: %v", err)
		}

		// Capture transaction after view creation
		result, err := engine.Execute("INSERT INTO viewttdb.orders (id, product, amount) VALUES (3, 'Gizmo', 300)")
		if err != nil {
			t.Fatalf("INSERT after view failed: %v", err)
		}
		txnAfterView := result.(db.CommitResult).Transaction.Id

		// Add more data after
		engine.Execute("INSERT INTO viewttdb.orders (id, product, amount) VALUES (4, 'Doodad', 400)")

		// Current view should have 4 rows
		result, err = engine.Execute("SELECT * FROM viewttdb.all_orders")
		if err != nil {
			t.Fatalf("SELECT from view failed: %v", err)
		}
		qr := result.(db.QueryResult)
		if len(qr.Data) != 4 {
			t.Errorf("Expected 4 rows in current view, got %d", len(qr.Data))
		}

		// Time-travel on view should show state at txnAfterView (3 rows)
		result, err = engine.Execute("SELECT * FROM viewttdb.all_orders AS OF '" + txnAfterView + "'")
		if err != nil {
			t.Fatalf("Time-travel on view failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 3 {
			t.Errorf("Expected 3 rows at txnAfterView, got %d", len(qr.Data))
		}

		// Test view with AS OF in its definition
		result, err = engine.Execute("INSERT INTO viewttdb.orders (id, product, amount) VALUES (5, 'Thing', 500)")
		if err != nil {
			t.Fatalf("INSERT for snapshot view failed: %v", err)
		}
		txnForSnapshot := result.(db.CommitResult).Transaction.Id

		// Create a view that captures data at a specific transaction
		_, err = engine.Execute("CREATE VIEW viewttdb.snapshot_view AS SELECT * FROM viewttdb.orders AS OF '" + txnForSnapshot + "'")
		if err != nil {
			t.Fatalf("CREATE VIEW with AS OF failed: %v", err)
		}

		// Add more data after snapshot
		engine.Execute("INSERT INTO viewttdb.orders (id, product, amount) VALUES (6, 'Stuff', 600)")

		// Snapshot view should always return 5 rows (fixed at txnForSnapshot)
		result, err = engine.Execute("SELECT * FROM viewttdb.snapshot_view")
		if err != nil {
			t.Fatalf("SELECT from snapshot view failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 5 {
			t.Errorf("Expected 5 rows in snapshot view, got %d", len(qr.Data))
		}

		// Current table should have 6 rows
		result, err = engine.Execute("SELECT * FROM viewttdb.orders")
		if err != nil {
			t.Fatalf("SELECT from table failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 6 {
			t.Errorf("Expected 6 rows in current table, got %d", len(qr.Data))
		}
	})
}

// TestIntegrationTimeTravelOnMaterializedViews tests time-travel queries on materialized views
func TestIntegrationTimeTravelOnMaterializedViews(t *testing.T) {
	runWithBothPersistence(t, func(t *testing.T, engine *db.Engine) {
		// Setup
		engine.Execute("CREATE DATABASE matttdb")
		engine.Execute("CREATE TABLE matttdb.sales (id INT PRIMARY KEY, region STRING, amount INT)")

		// Insert initial data
		engine.Execute("INSERT INTO matttdb.sales (id, region, amount) VALUES (1, 'East', 100)")
		engine.Execute("INSERT INTO matttdb.sales (id, region, amount) VALUES (2, 'West', 200)")

		// Create materialized view (creates view definition + caches initial data)
		_, err := engine.Execute("CREATE MATERIALIZED VIEW matttdb.east_sales AS SELECT * FROM matttdb.sales WHERE region = 'East'")
		if err != nil {
			t.Fatalf("CREATE MATERIALIZED VIEW failed: %v", err)
		}

		// Verify materialized view has 1 row
		result, err := engine.Execute("SELECT * FROM matttdb.east_sales")
		if err != nil {
			t.Fatalf("SELECT from materialized view failed: %v", err)
		}
		qr := result.(db.QueryResult)
		if len(qr.Data) != 1 {
			t.Errorf("Expected 1 row in materialized view, got %d", len(qr.Data))
		}

		// Insert a marker record to get a transaction that's AFTER the initial mat view population
		result, err = engine.Execute("INSERT INTO matttdb.sales (id, region, amount) VALUES (99, 'Marker', 0)")
		if err != nil {
			t.Fatalf("INSERT marker failed: %v", err)
		}
		txnBeforeRefresh := result.(db.CommitResult).Transaction.Id

		// Add more East region data
		engine.Execute("INSERT INTO matttdb.sales (id, region, amount) VALUES (3, 'East', 300)")

		// Refresh materialized view
		result, err = engine.Execute("REFRESH VIEW matttdb.east_sales")
		if err != nil {
			t.Fatalf("REFRESH VIEW failed: %v", err)
		}
		txnAfterRefresh := result.(db.CommitResult).Transaction.Id

		// Materialized view now has 2 rows
		result, err = engine.Execute("SELECT * FROM matttdb.east_sales")
		if err != nil {
			t.Fatalf("SELECT after refresh failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 2 {
			t.Errorf("Expected 2 rows after refresh, got %d", len(qr.Data))
		}

		// Time-travel to before refresh should show 1 row (the txnBeforeRefresh is AFTER initial mat view data was cached)
		result, err = engine.Execute("SELECT * FROM matttdb.east_sales AS OF '" + txnBeforeRefresh + "'")
		if err != nil {
			t.Fatalf("Time-travel on materialized view failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 1 {
			t.Errorf("Expected 1 row at txnBeforeRefresh, got %d", len(qr.Data))
		}

		// Time-travel to after refresh should show 2 rows
		result, err = engine.Execute("SELECT * FROM matttdb.east_sales AS OF '" + txnAfterRefresh + "'")
		if err != nil {
			t.Fatalf("Time-travel to after refresh failed: %v", err)
		}
		qr = result.(db.QueryResult)
		if len(qr.Data) != 2 {
			t.Errorf("Expected 2 rows at txnAfterRefresh, got %d", len(qr.Data))
		}
	})
}
