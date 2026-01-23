package ps

import (
	"testing"
)

func TestIndexManager(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	im := NewIndexManager(&persistence)

	// Create index
	idx, err := im.CreateIndex("idx_name", "testdb", "users", "name", false)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	if idx.Name != "idx_name" {
		t.Errorf("Expected index name 'idx_name', got '%s'", idx.Name)
	}
	if idx.Column != "name" {
		t.Errorf("Expected column 'name', got '%s'", idx.Column)
	}
}

func TestIndexManagerDuplicateIndex(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	im := NewIndexManager(&persistence)

	// Create first index
	_, err = im.CreateIndex("idx_name", "testdb", "users", "name", false)
	if err != nil {
		t.Fatalf("Failed to create first index: %v", err)
	}

	// Try to create duplicate
	_, err = im.CreateIndex("idx_name2", "testdb", "users", "name", false)
	if err == nil {
		t.Error("Expected error when creating duplicate index on same column")
	}
}

func TestIndexInsertAndLookup(t *testing.T) {
	idx := &Index{
		Name:    "idx_test",
		Column:  "name",
		Entries: make(map[string][]string),
	}

	// Insert entries
	idx.Insert("Alice", "1")
	idx.Insert("Bob", "2")
	idx.Insert("Alice", "3")

	// Lookup
	keys := idx.Lookup("Alice")
	if len(keys) != 2 {
		t.Errorf("Expected 2 keys for 'Alice', got %d", len(keys))
	}

	keys = idx.Lookup("Bob")
	if len(keys) != 1 {
		t.Errorf("Expected 1 key for 'Bob', got %d", len(keys))
	}

	keys = idx.Lookup("Charlie")
	if len(keys) != 0 {
		t.Errorf("Expected 0 keys for 'Charlie', got %d", len(keys))
	}
}

func TestIndexDelete(t *testing.T) {
	idx := &Index{
		Name:    "idx_test",
		Column:  "name",
		Entries: make(map[string][]string),
	}

	idx.Insert("Alice", "1")
	idx.Insert("Alice", "2")

	// Delete one entry
	idx.Delete("Alice", "1")

	keys := idx.Lookup("Alice")
	if len(keys) != 1 {
		t.Errorf("Expected 1 key after delete, got %d", len(keys))
	}
	if keys[0] != "2" {
		t.Errorf("Expected key '2', got '%s'", keys[0])
	}

	// Delete last entry
	idx.Delete("Alice", "2")
	keys = idx.Lookup("Alice")
	if len(keys) != 0 {
		t.Errorf("Expected 0 keys after deleting all, got %d", len(keys))
	}
}

func TestUniqueIndex(t *testing.T) {
	idx := &Index{
		Name:    "idx_unique",
		Column:  "email",
		Unique:  true,
		Entries: make(map[string][]string),
	}

	// First insert should succeed
	err := idx.Insert("alice@test.com", "1")
	if err != nil {
		t.Errorf("First insert should succeed: %v", err)
	}

	// Duplicate should fail
	err = idx.Insert("alice@test.com", "2")
	if err == nil {
		t.Error("Expected error when inserting duplicate into unique index")
	}
}

func TestIndexLookupRange(t *testing.T) {
	idx := &Index{
		Name:    "idx_test",
		Column:  "age",
		Entries: make(map[string][]string),
	}

	idx.Insert("20", "1")
	idx.Insert("25", "2")
	idx.Insert("30", "3")
	idx.Insert("35", "4")
	idx.Insert("40", "5")

	// Range lookup
	keys := idx.LookupRange("25", "35")
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys in range [25, 35], got %d", len(keys))
	}
}

func TestDropIndex(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	im := NewIndexManager(&persistence)

	// Create and drop index
	_, err = im.CreateIndex("idx_name", "testdb", "users", "name", false)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	err = im.DropIndex("testdb", "users", "name")
	if err != nil {
		t.Fatalf("Failed to drop index: %v", err)
	}

	// Verify dropped
	_, exists := im.GetIndex("testdb", "users", "name")
	if exists {
		t.Error("Expected index to be dropped")
	}
}

func TestDropNonExistentIndex(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	im := NewIndexManager(&persistence)

	err = im.DropIndex("testdb", "users", "nonexistent")
	if err == nil {
		t.Error("Expected error when dropping non-existent index")
	}
}
