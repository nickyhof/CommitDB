package ps

import (
	"testing"

	"github.com/nickyhof/CommitDB/core"
)

func TestPlumbingSaveRecordDirect(t *testing.T) {
	p, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "Test User", Email: "test@example.com"}

	// Test single record
	records := map[string][]byte{
		"key1": []byte(`{"id": 1, "name": "test"}`),
	}

	txn, err := p.SaveRecordDirect("testdb", "testtable", records, identity)
	if err != nil {
		t.Fatalf("SaveRecordDirect failed: %v", err)
	}

	if txn.Id == "" {
		t.Error("Transaction ID should not be empty")
	}

	// Verify record can be read back
	data, exists := p.GetRecord("testdb", "testtable", "key1")
	if !exists {
		t.Fatal("Record should exist")
	}

	if string(data) != `{"id": 1, "name": "test"}` {
		t.Errorf("Data mismatch: got %s", string(data))
	}
}

func TestPlumbingSaveMultipleRecords(t *testing.T) {
	p, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "Test User", Email: "test@example.com"}

	// Test multiple records in one commit
	records := map[string][]byte{
		"key1": []byte(`{"id": 1}`),
		"key2": []byte(`{"id": 2}`),
		"key3": []byte(`{"id": 3}`),
	}

	txn, err := p.SaveRecordDirect("testdb", "testtable", records, identity)
	if err != nil {
		t.Fatalf("SaveRecordDirect failed: %v", err)
	}

	if txn.Id == "" {
		t.Error("Transaction ID should not be empty")
	}

	// Verify all records
	for key, expected := range records {
		data, exists := p.GetRecord("testdb", "testtable", key)
		if !exists {
			t.Errorf("Record %s should exist", key)
			continue
		}
		if string(data) != string(expected) {
			t.Errorf("Record %s mismatch: got %s, want %s", key, string(data), string(expected))
		}
	}
}

func TestPlumbingUpdateExistingRecord(t *testing.T) {
	p, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "Test User", Email: "test@example.com"}

	// Create initial record
	records := map[string][]byte{
		"key1": []byte(`{"id": 1, "version": "v1"}`),
	}
	_, err = p.SaveRecordDirect("testdb", "testtable", records, identity)
	if err != nil {
		t.Fatalf("First SaveRecordDirect failed: %v", err)
	}

	// Update record
	records["key1"] = []byte(`{"id": 1, "version": "v2"}`)
	_, err = p.SaveRecordDirect("testdb", "testtable", records, identity)
	if err != nil {
		t.Fatalf("Second SaveRecordDirect failed: %v", err)
	}

	// Verify update
	data, exists := p.GetRecord("testdb", "testtable", "key1")
	if !exists {
		t.Fatal("Record should exist")
	}

	if string(data) != `{"id": 1, "version": "v2"}` {
		t.Errorf("Data mismatch: got %s", string(data))
	}
}

func TestPlumbingMultipleTables(t *testing.T) {
	p, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	identity := core.Identity{Name: "Test User", Email: "test@example.com"}

	// Create records in table1
	_, err = p.SaveRecordDirect("db", "table1", map[string][]byte{"k1": []byte("v1")}, identity)
	if err != nil {
		t.Fatalf("SaveRecordDirect table1 failed: %v", err)
	}

	// Create records in table2
	_, err = p.SaveRecordDirect("db", "table2", map[string][]byte{"k2": []byte("v2")}, identity)
	if err != nil {
		t.Fatalf("SaveRecordDirect table2 failed: %v", err)
	}

	// Verify both tables have their records
	data1, exists1 := p.GetRecord("db", "table1", "k1")
	data2, exists2 := p.GetRecord("db", "table2", "k2")

	if !exists1 || string(data1) != "v1" {
		t.Error("table1 record missing or incorrect")
	}
	if !exists2 || string(data2) != "v2" {
		t.Error("table2 record missing or incorrect")
	}
}
