package ops

import (
	"testing"

	"github.com/nickyhof/CommitDB/v2/core"
	"github.com/nickyhof/CommitDB/v2/persistence"
)

func setupTestPersistence(t *testing.T) *persistence.Persistence {
	t.Helper()
	p, err := persistence.NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}
	return &p
}

var testIdentity = core.Identity{Name: "test", Email: "test@test.com"}

// --- DatabaseOp Tests ---

func TestCreateDatabase(t *testing.T) {
	p := setupTestPersistence(t)

	txn, dbOp, err := CreateDatabase(core.Database{Name: "mydb"}, p, testIdentity)
	if err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}
	if txn == nil {
		t.Error("Expected non-nil transaction")
	}
	if dbOp.Database.Name != "mydb" {
		t.Errorf("Expected database name 'mydb', got '%s'", dbOp.Database.Name)
	}
}

func TestGetDatabase(t *testing.T) {
	p := setupTestPersistence(t)

	CreateDatabase(core.Database{Name: "mydb"}, p, testIdentity)

	dbOp, err := GetDatabase("mydb", p)
	if err != nil {
		t.Fatalf("GetDatabase failed: %v", err)
	}
	if dbOp.Database.Name != "mydb" {
		t.Errorf("Expected 'mydb', got '%s'", dbOp.Database.Name)
	}
}

func TestGetDatabaseNotFound(t *testing.T) {
	p := setupTestPersistence(t)

	_, err := GetDatabase("nonexistent", p)
	if err == nil {
		t.Error("Expected error for non-existent database")
	}
}

func TestDatabaseOpDropDatabase(t *testing.T) {
	p := setupTestPersistence(t)

	_, dbOp, _ := CreateDatabase(core.Database{Name: "dropme"}, p, testIdentity)

	_, err := dbOp.DropDatabase(testIdentity)
	if err != nil {
		t.Fatalf("DropDatabase failed: %v", err)
	}

	// Verify it's gone
	_, err = GetDatabase("dropme", p)
	if err == nil {
		t.Error("Database should not exist after drop")
	}
}

func TestDatabaseOpTableNames(t *testing.T) {
	p := setupTestPersistence(t)

	CreateDatabase(core.Database{Name: "mydb"}, p, testIdentity)

	table := core.Table{
		Database: "mydb",
		Name:     "users",
		Columns:  []core.Column{{Name: "id", Type: core.IntType, PrimaryKey: true}},
	}
	p.CreateTable(table, testIdentity)

	dbOp, _ := GetDatabase("mydb", p)
	names := dbOp.TableNames()
	if len(names) != 1 || names[0] != "users" {
		t.Errorf("Expected ['users'], got %v", names)
	}
}

// --- TableOp Tests ---

func setupTestTable(t *testing.T) (*persistence.Persistence, *TableOp) {
	t.Helper()
	p := setupTestPersistence(t)

	CreateDatabase(core.Database{Name: "testdb"}, p, testIdentity)

	table := core.Table{
		Database: "testdb",
		Name:     "items",
		Columns: []core.Column{
			{Name: "id", Type: core.IntType, PrimaryKey: true},
			{Name: "name", Type: core.StringType},
			{Name: "value", Type: core.IntType},
		},
	}
	p.CreateTable(table, testIdentity)

	op, err := GetTable("testdb", "items", p)
	if err != nil {
		t.Fatalf("GetTable failed: %v", err)
	}
	return p, op
}

func TestCreateTable(t *testing.T) {
	p := setupTestPersistence(t)
	CreateDatabase(core.Database{Name: "testdb"}, p, testIdentity)

	table := core.Table{
		Database: "testdb",
		Name:     "newtable",
		Columns:  []core.Column{{Name: "id", Type: core.IntType, PrimaryKey: true}},
	}
	txn, tableOp, err := CreateTable(table, p, testIdentity)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	if txn == nil {
		t.Error("Expected non-nil transaction")
	}
	if tableOp.Table.Name != "newtable" {
		t.Errorf("Expected table name 'newtable', got '%s'", tableOp.Table.Name)
	}
}

func TestGetTable(t *testing.T) {
	_, op := setupTestTable(t)

	if op.Table.Name != "items" {
		t.Errorf("Expected 'items', got '%s'", op.Table.Name)
	}
	if op.Table.Database != "testdb" {
		t.Errorf("Expected 'testdb', got '%s'", op.Table.Database)
	}
}

func TestGetTableNotFound(t *testing.T) {
	p := setupTestPersistence(t)
	CreateDatabase(core.Database{Name: "testdb"}, p, testIdentity)

	_, err := GetTable("testdb", "nonexistent", p)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

func TestPrimaryKey(t *testing.T) {
	_, op := setupTestTable(t)

	pk, err := op.PrimaryKey()
	if err != nil {
		t.Fatalf("PrimaryKey failed: %v", err)
	}
	if *pk != "id" {
		t.Errorf("Expected primary key 'id', got '%s'", *pk)
	}
}

func TestPrimaryKeyNotFound(t *testing.T) {
	p := setupTestPersistence(t)
	CreateDatabase(core.Database{Name: "testdb"}, p, testIdentity)

	// Table with no PK
	op := &TableOp{
		Table: core.Table{
			Database: "testdb",
			Name:     "nopk",
			Columns:  []core.Column{{Name: "col", Type: core.StringType}},
		},
		Persistence: p,
	}

	_, err := op.PrimaryKey()
	if err == nil {
		t.Error("Expected error for table with no primary key")
	}
}

func TestPutAndGet(t *testing.T) {
	_, op := setupTestTable(t)

	_, err := op.Put("1", []byte(`{"id":"1","name":"Alice","value":"100"}`), testIdentity)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	data, exists := op.Get("1")
	if !exists {
		t.Fatal("Expected record to exist")
	}
	if len(data) == 0 {
		t.Error("Expected non-empty data")
	}
}

func TestGetString(t *testing.T) {
	_, op := setupTestTable(t)

	op.Put("1", []byte(`{"id":"1","name":"Alice"}`), testIdentity)

	val, exists := op.GetString("1")
	if !exists {
		t.Fatal("Expected record to exist")
	}
	if val != `{"id":"1","name":"Alice"}` {
		t.Errorf("Unexpected value: %s", val)
	}
}

func TestGetStringNotFound(t *testing.T) {
	_, op := setupTestTable(t)

	_, exists := op.GetString("999")
	if exists {
		t.Error("Expected record to not exist")
	}
}

func TestDelete(t *testing.T) {
	_, op := setupTestTable(t)

	op.Put("1", []byte(`{"id":"1","name":"Alice"}`), testIdentity)

	_, err := op.Delete("1", testIdentity)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, exists := op.Get("1")
	if exists {
		t.Error("Record should not exist after delete")
	}
}

func TestCount(t *testing.T) {
	_, op := setupTestTable(t)

	if op.Count() != 0 {
		t.Errorf("Expected 0 count, got %d", op.Count())
	}

	op.Put("1", []byte(`{"id":"1"}`), testIdentity)
	op.Put("2", []byte(`{"id":"2"}`), testIdentity)

	if op.Count() != 2 {
		t.Errorf("Expected 2 count, got %d", op.Count())
	}
}

func TestKeys(t *testing.T) {
	_, op := setupTestTable(t)

	op.Put("a", []byte(`{"id":"a"}`), testIdentity)
	op.Put("b", []byte(`{"id":"b"}`), testIdentity)

	keys := op.Keys()
	if len(keys) != 2 {
		t.Errorf("Expected 2 keys, got %d", len(keys))
	}
}

func TestScan(t *testing.T) {
	_, op := setupTestTable(t)

	op.Put("1", []byte(`{"id":"1","name":"Alice"}`), testIdentity)
	op.Put("2", []byte(`{"id":"2","name":"Bob"}`), testIdentity)

	count := 0
	for range op.Scan() {
		count++
	}
	if count != 2 {
		t.Errorf("Expected 2 rows from Scan, got %d", count)
	}
}

func TestDropTable(t *testing.T) {
	p, op := setupTestTable(t)

	_, err := op.DropTable(testIdentity)
	if err != nil {
		t.Fatalf("DropTable failed: %v", err)
	}

	_, err = GetTable("testdb", "items", p)
	if err == nil {
		t.Error("Table should not exist after drop")
	}
}

func TestPutAll(t *testing.T) {
	_, op := setupTestTable(t)

	records := map[string][]byte{
		"1": []byte(`{"id":"1","name":"Alice"}`),
		"2": []byte(`{"id":"2","name":"Bob"}`),
	}

	_, err := op.PutAll(records, testIdentity)
	if err != nil {
		t.Fatalf("PutAll failed: %v", err)
	}

	if op.Count() != 2 {
		t.Errorf("Expected 2 records after PutAll, got %d", op.Count())
	}
}

func TestGetInt(t *testing.T) {
	_, op := setupTestTable(t)

	// GetInt calls Atoi on the raw stored bytes, so store a plain integer
	op.Put("42", []byte("100"), testIdentity)

	val, exists, err := op.GetInt("42")
	if err != nil {
		t.Fatalf("GetInt failed: %v", err)
	}
	if !exists {
		t.Fatal("Expected record to exist")
	}
	if val != 100 {
		t.Errorf("Expected 100, got %d", val)
	}
}

func TestGetIntNotFound(t *testing.T) {
	_, op := setupTestTable(t)

	_, exists, _ := op.GetInt("999")
	if exists {
		t.Error("Expected record to not exist")
	}
}

func TestScanWithFilter(t *testing.T) {
	_, op := setupTestTable(t)

	op.Put("1", []byte(`{"id":"1","name":"Alice"}`), testIdentity)
	op.Put("2", []byte(`{"id":"2","name":"Bob"}`), testIdentity)
	op.Put("3", []byte(`{"id":"3","name":"Charlie"}`), testIdentity)

	// Filter to only key "2"
	count := 0
	for range op.ScanWithFilter(func(key string, value []byte) bool {
		return key == "2"
	}) {
		count++
	}
	if count != 1 {
		t.Errorf("Expected 1 row from filtered Scan, got %d", count)
	}
}

func TestCopyFrom(t *testing.T) {
	p := setupTestPersistence(t)

	CreateDatabase(core.Database{Name: "srcdb"}, p, testIdentity)
	CreateDatabase(core.Database{Name: "dstdb"}, p, testIdentity)

	srcTable := core.Table{
		Database: "srcdb",
		Name:     "source",
		Columns: []core.Column{
			{Name: "id", Type: core.IntType, PrimaryKey: true},
			{Name: "name", Type: core.StringType},
		},
	}
	dstTable := core.Table{
		Database: "dstdb",
		Name:     "dest",
		Columns: []core.Column{
			{Name: "id", Type: core.IntType, PrimaryKey: true},
			{Name: "name", Type: core.StringType},
		},
	}
	p.CreateTable(srcTable, testIdentity)
	p.CreateTable(dstTable, testIdentity)

	srcOp, _ := GetTable("srcdb", "source", p)
	dstOp, _ := GetTable("dstdb", "dest", p)

	// Insert data into source
	srcOp.Put("1", []byte(`{"id":"1","name":"Alice"}`), testIdentity)
	srcOp.Put("2", []byte(`{"id":"2","name":"Bob"}`), testIdentity)

	// Copy source -> dest
	_, err := dstOp.CopyFrom(srcOp, testIdentity)
	if err != nil {
		t.Fatalf("CopyFrom failed: %v", err)
	}

	if dstOp.Count() != 2 {
		t.Errorf("Expected 2 records in dest after CopyFrom, got %d", dstOp.Count())
	}
}
