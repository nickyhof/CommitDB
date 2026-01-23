package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/nickyhof/CommitDB"
	"github.com/nickyhof/CommitDB/core"
	"github.com/nickyhof/CommitDB/db"
	"github.com/nickyhof/CommitDB/ps"
)

func setupTestCLI(t *testing.T) *CLI {
	persistence, err := ps.NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	instance := CommitDB.Open(&persistence)
	engine := instance.Engine(core.Identity{
		Name:  "test",
		Email: "test@test.com",
	})

	return &CLI{
		engine:  engine,
		history: make([]string, 0),
	}
}

func TestCLIShowDatabasesEmpty(t *testing.T) {
	cli := setupTestCLI(t)

	// Capture output
	var buf bytes.Buffer

	// Execute SHOW DATABASES on empty DB - should not panic
	result, err := cli.engine.Execute("SHOW DATABASES")
	if err != nil {
		t.Fatalf("SHOW DATABASES failed: %v", err)
	}

	if result == nil {
		t.Error("Expected non-nil result")
	}

	_ = buf // Would use buf if we redirected stdout
}

func TestCLICreateAndShowDatabases(t *testing.T) {
	cli := setupTestCLI(t)

	// Create database
	_, err := cli.engine.Execute("CREATE DATABASE testdb")
	if err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	// Show databases
	result, err := cli.engine.Execute("SHOW DATABASES")
	if err != nil {
		t.Fatalf("SHOW DATABASES failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}
}

func TestCLICreateTableAndInsert(t *testing.T) {
	cli := setupTestCLI(t)

	// Setup
	cli.engine.Execute("CREATE DATABASE test")
	cli.engine.Execute("CREATE TABLE test.users (id INT PRIMARY KEY, name STRING)")

	// Insert
	_, err := cli.engine.Execute("INSERT INTO test.users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Select
	result, err := cli.engine.Execute("SELECT * FROM test.users")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}
}

func TestCLIAddToHistory(t *testing.T) {
	cli := setupTestCLI(t)

	cli.addToHistory("SELECT * FROM test")
	cli.addToHistory("INSERT INTO test VALUES (1)")

	if len(cli.history) != 2 {
		t.Errorf("Expected 2 history entries, got %d", len(cli.history))
	}

	// Adding duplicate of last command should not increase count
	cli.addToHistory("INSERT INTO test VALUES (1)")
	if len(cli.history) != 2 {
		t.Errorf("Expected 2 history entries after duplicate, got %d", len(cli.history))
	}
}

func TestCLIHistoryLimit(t *testing.T) {
	cli := setupTestCLI(t)

	// Add more than 1000 entries
	for i := 0; i < 1100; i++ {
		cli.addToHistory("SELECT " + string(rune(i)))
	}

	if len(cli.history) > 1000 {
		t.Errorf("Expected history to be limited to 1000, got %d", len(cli.history))
	}
}

func TestCLIGetPrompt(t *testing.T) {
	cli := setupTestCLI(t)

	// Normal prompt
	prompt := cli.getPrompt(false)
	if !strings.Contains(prompt, "commitdb") {
		t.Error("Expected prompt to contain 'commitdb'")
	}

	// Multi-line prompt
	prompt = cli.getPrompt(true)
	if !strings.Contains(prompt, "...>") {
		t.Error("Expected multi-line prompt to contain '...>'")
	}

	// With database context
	cli.database = "mydb"
	prompt = cli.getPrompt(false)
	if !strings.Contains(prompt, "mydb") {
		t.Error("Expected prompt to contain database name")
	}
}

func TestCLIHandleCommand(t *testing.T) {
	cli := setupTestCLI(t)

	tests := []struct {
		command  string
		expected bool // should return true (command handled)
	}{
		{".help", true},
		{".version", true},
		{".history", true},
		{".databases", true},
		{".unknown", true}, // Unknown commands are still handled (with error message)
	}

	for _, test := range tests {
		result := cli.handleCommand(test.command)
		if result != test.expected {
			t.Errorf("handleCommand(%s) = %v, expected %v", test.command, result, test.expected)
		}
	}
}

func TestCLIUseDatabase(t *testing.T) {
	cli := setupTestCLI(t)

	cli.handleCommand(".use testdb")

	if cli.database != "testdb" {
		t.Errorf("Expected database to be 'testdb', got '%s'", cli.database)
	}
}

func TestVersionVariable(t *testing.T) {
	// Test that Version variable exists and has a default value
	if Version == "" {
		t.Error("Version should not be empty")
	}
}

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"single statement", "SELECT * FROM test", 1},
		{"two statements", "SELECT * FROM a; SELECT * FROM b", 2},
		{"with semicolons", "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2);", 2},
		{"with comments", "-- comment\nSELECT * FROM test", 1},
		{"multiline", "CREATE TABLE t (\n  id INT,\n  name STRING\n);", 1},
		{"empty", "", 0},
		{"only semicolons", ";;;", 0},
		{"string with semicolon", "INSERT INTO t (s) VALUES ('a;b')", 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := splitStatements(test.input)
			if len(result) != test.expected {
				t.Errorf("splitStatements(%q) = %d statements, expected %d", test.input, len(result), test.expected)
			}
		})
	}
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		input    string
		max      int
		expected string
	}{
		{"short", 10, "short"},
		{"this is a long string", 10, "this is..."},
		{"exact", 5, "exact"},
		{"ab", 10, "ab"},
	}

	for _, test := range tests {
		result := truncate(test.input, test.max)
		if result != test.expected {
			t.Errorf("truncate(%q, %d) = %q, expected %q", test.input, test.max, result, test.expected)
		}
	}
}

func TestImportFile(t *testing.T) {
	cli := setupTestCLI(t)

	// Test importing the example file
	err := cli.importFile("../../examples/shop.sql")
	if err != nil {
		t.Fatalf("importFile failed: %v", err)
	}

	// Verify data was imported
	result, err := cli.engine.Execute("SELECT COUNT(*) FROM shop.products")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}

	// We inserted 5 products
	qr := result.(db.QueryResult)
	if qr.Data[0][0] != "5" {
		t.Errorf("Expected 5 products, got %s", qr.Data[0][0])
	}

	// Verify customers
	result, _ = cli.engine.Execute("SELECT COUNT(*) FROM shop.customers")
	qr = result.(db.QueryResult)
	if qr.Data[0][0] != "3" {
		t.Errorf("Expected 3 customers, got %s", qr.Data[0][0])
	}
}

func TestImportFileNotFound(t *testing.T) {
	cli := setupTestCLI(t)

	err := cli.importFile("nonexistent.sql")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

func TestImportCommand(t *testing.T) {
	cli := setupTestCLI(t)

	// Test .import command handling
	result := cli.handleCommand(".import")
	if !result {
		t.Error("Expected .import to be handled")
	}
}
