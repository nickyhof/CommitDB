package ps

import (
	"encoding/json"
	"fmt"
	"iter"
	"strings"

	"github.com/nickyhof/CommitDB/core"
)

func (persistence *Persistence) CreateDatabase(database core.Database, identity core.Identity) (txn Transaction, err error) {
	path := fmt.Sprintf("%s.database", database.Name)

	dataBytes, err := json.Marshal(database)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to marshal database: %w", err)
	}

	// Use low-level plumbing API
	return persistence.WriteFileDirect(path, dataBytes, identity, "Creating database")
}

func (persistence *Persistence) GetDatabase(name string) (d *core.Database, err error) {
	path := fmt.Sprintf("%s.database", name)

	data, err := persistence.ReadFileDirect(path)
	if err != nil {
		return nil, fmt.Errorf("database %s does not exist: %w", name, err)
	}

	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("failed to unmarshal database: %w", err)
	}

	return d, nil
}

func (persistence *Persistence) DropDatabase(name string, identity core.Identity) (txn Transaction, err error) {
	paths := []string{
		fmt.Sprintf("%s.database", name),
		name, // Database directory
	}

	// Use low-level plumbing API
	return persistence.DeletePathDirect(paths, identity, "Dropping database")
}

func (persistence *Persistence) CreateTable(table core.Table, identity core.Identity) (txn Transaction, err error) {
	path := fmt.Sprintf("%s/%s.table", table.Database, table.Name)

	dataBytes, err := json.Marshal(table)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to marshal table: %w", err)
	}

	// Use low-level plumbing API
	return persistence.WriteFileDirect(path, dataBytes, identity, "Creating table")
}

func (persistence *Persistence) GetTable(database string, table string) (t *core.Table, err error) {
	path := fmt.Sprintf("%s/%s.table", database, table)

	data, err := persistence.ReadFileDirect(path)
	if err != nil {
		return nil, fmt.Errorf("table %s.%s does not exist: %w", database, table, err)
	}

	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("failed to unmarshal table: %w", err)
	}

	return t, nil
}

// UpdateTable updates an existing table's schema
func (persistence *Persistence) UpdateTable(table core.Table, identity core.Identity, message string) (txn Transaction, err error) {
	path := fmt.Sprintf("%s/%s.table", table.Database, table.Name)

	dataBytes, err := json.Marshal(table)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to marshal table: %w", err)
	}

	// Use low-level plumbing API
	return persistence.WriteFileDirect(path, dataBytes, identity, message)
}

func (persistence *Persistence) DropTable(database string, table string, identity core.Identity) (txn Transaction, err error) {
	paths := []string{
		fmt.Sprintf("%s/%s.table", database, table),
		fmt.Sprintf("%s/%s", database, table), // Table data directory
	}

	// Use low-level plumbing API
	return persistence.DeletePathDirect(paths, identity, "Dropping table")
}

func (persistence *Persistence) SaveRecord(database string, table string, records map[string][]byte, identity core.Identity) (txn Transaction, err error) {
	// Use low-level plumbing API for better performance
	return persistence.SaveRecordDirect(database, table, records, identity)
}

func (persistence *Persistence) DeleteRecord(database string, table string, key string, identity core.Identity) (txn Transaction, err error) {
	// Use low-level plumbing API for better performance
	return persistence.DeleteRecordDirect(database, table, key, identity)
}

func (persistence *Persistence) GetRecord(database string, table string, key string) (data []byte, exists bool) {
	// Use low-level plumbing API
	return persistence.GetRecordDirect(database, table, key)
}

func (persistence *Persistence) ListDatabases() []string {
	entries, err := persistence.ListEntriesDirect(".")
	if err != nil {
		return nil
	}

	// Use a set to avoid duplicates (database can exist as both .database file and directory)
	databaseSet := make(map[string]bool)
	for _, entry := range entries {
		if entry.IsDir && entry.Name != ".git" && entry.Name != ".shares" {
			databaseSet[entry.Name] = true
		} else if !entry.IsDir && strings.HasSuffix(entry.Name, ".database") {
			// Extract database name from .database file
			dbName := strings.TrimSuffix(entry.Name, ".database")
			databaseSet[dbName] = true
		}
	}

	var databases []string
	for db := range databaseSet {
		databases = append(databases, db)
	}

	return databases
}

func (persistence *Persistence) ListTables(database string) []string {
	entries, err := persistence.ListEntriesDirect(database)
	if err != nil {
		return nil
	}

	var tables []string
	for _, entry := range entries {
		if !entry.IsDir && strings.HasSuffix(entry.Name, ".table") {
			tables = append(tables, strings.TrimSuffix(entry.Name, ".table"))
		}
	}

	return tables
}

func (persistence *Persistence) ListRecordKeys(database string, table string) []string {
	path := fmt.Sprintf("%s/%s", database, table)

	entries, err := persistence.ListEntriesDirect(path)
	if err != nil {
		return nil
	}

	var keys []string
	for _, entry := range entries {
		if !entry.IsDir {
			keys = append(keys, entry.Name)
		}
	}

	return keys
}

func (persistence *Persistence) Scan(database string, table string, filterExpr *func(key string, value []byte) bool) iter.Seq2[string, []byte] {
	keys := persistence.ListRecordKeys(database, table)

	currentIndex := 0

	return func(yield func(key string, value []byte) bool) {
		for currentIndex < len(keys) {
			key := keys[currentIndex]
			value, _ := persistence.GetRecord(database, table, key)

			currentIndex++

			if filterExpr != nil && !(*filterExpr)(key, value) {
				continue
			}

			if !yield(key, value) {
				return
			}
		}
	}
}

// CopyRecords copies all records from source table to destination table in a single atomic transaction.
// This is memory-efficient: records are streamed row-by-row from source and written to dest without loading all into memory.
func (persistence *Persistence) CopyRecords(srcDatabase, srcTable, dstDatabase, dstTable string, identity core.Identity) (txn Transaction, err error) {
	// Use low-level plumbing API for better performance
	return persistence.CopyRecordsDirect(srcDatabase, srcTable, dstDatabase, dstTable, identity)
}
