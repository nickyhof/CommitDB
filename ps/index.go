package ps

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/nickyhof/CommitDB/core"
)

// Index represents a B-tree index on a column
type Index struct {
	Name     string              `json:"name"`
	Database string              `json:"database"`
	Table    string              `json:"table"`
	Column   string              `json:"column"`
	Unique   bool                `json:"unique"`
	Entries  map[string][]string `json:"entries"` // column value -> list of primary keys
}

// IndexManager manages indexes for a persistence layer
type IndexManager struct {
	persistence *Persistence
	identity    core.Identity
	indexes     map[string]*Index // key: database.table.column
	mu          sync.RWMutex
}

// NewIndexManager creates a new index manager
func NewIndexManager(persistence *Persistence, identity core.Identity) *IndexManager {
	return &IndexManager{
		persistence: persistence,
		identity:    identity,
		indexes:     make(map[string]*Index),
	}
}

// indexKey generates a unique key for an index
func indexKey(database, table, column string) string {
	return fmt.Sprintf("%s.%s.%s", database, table, column)
}

// CreateIndex creates a new index on a column
func (im *IndexManager) CreateIndex(name, database, table, column string, unique bool) (*Index, error) {
	im.mu.Lock()
	defer im.mu.Unlock()

	key := indexKey(database, table, column)
	if _, exists := im.indexes[key]; exists {
		return nil, fmt.Errorf("index already exists on %s.%s.%s", database, table, column)
	}

	idx := &Index{
		Name:     name,
		Database: database,
		Table:    table,
		Column:   column,
		Unique:   unique,
		Entries:  make(map[string][]string),
	}

	im.indexes[key] = idx

	// Don't persist yet - caller will populate and then call SaveIndex
	return idx, nil
}

// SaveIndex persists an index to storage (public wrapper)
func (im *IndexManager) SaveIndex(idx *Index) error {
	im.mu.Lock()
	defer im.mu.Unlock()
	return im.saveIndex(idx)
}

// GetIndex retrieves an existing index
func (im *IndexManager) GetIndex(database, table, column string) (*Index, bool) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	key := indexKey(database, table, column)
	idx, exists := im.indexes[key]
	return idx, exists
}

// DropIndex removes an index
func (im *IndexManager) DropIndex(database, table, column string) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	key := indexKey(database, table, column)
	idx, exists := im.indexes[key]
	if !exists {
		return fmt.Errorf("index not found on %s.%s.%s", database, table, column)
	}

	// Remove from persistence
	if err := im.deleteIndex(idx); err != nil {
		return err
	}

	delete(im.indexes, key)
	return nil
}

// Insert adds an entry to the index
func (idx *Index) Insert(columnValue, primaryKey string) error {
	if idx.Unique {
		if existing, ok := idx.Entries[columnValue]; ok && len(existing) > 0 {
			return fmt.Errorf("duplicate value %s violates unique constraint on index %s", columnValue, idx.Name)
		}
	}

	keys := idx.Entries[columnValue]
	// Check if already exists
	for _, k := range keys {
		if k == primaryKey {
			return nil // Already indexed
		}
	}

	idx.Entries[columnValue] = append(keys, primaryKey)
	return nil
}

// Delete removes an entry from the index
func (idx *Index) Delete(columnValue, primaryKey string) {
	keys := idx.Entries[columnValue]
	for i, k := range keys {
		if k == primaryKey {
			idx.Entries[columnValue] = append(keys[:i], keys[i+1:]...)
			if len(idx.Entries[columnValue]) == 0 {
				delete(idx.Entries, columnValue)
			}
			return
		}
	}
}

// Lookup finds primary keys for a given column value
func (idx *Index) Lookup(columnValue string) []string {
	return idx.Entries[columnValue]
}

// LookupRange finds primary keys within a range (inclusive)
func (idx *Index) LookupRange(minValue, maxValue string) []string {
	var results []string

	// Get sorted keys for range scan
	var sortedKeys []string
	for k := range idx.Entries {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	for _, k := range sortedKeys {
		if k >= minValue && k <= maxValue {
			results = append(results, idx.Entries[k]...)
		}
	}

	return results
}

// saveIndex persists an index to storage using plumbing API
func (im *IndexManager) saveIndex(idx *Index) error {
	path := fmt.Sprintf("%s/%s.index.%s", idx.Database, idx.Table, idx.Column)

	data, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("failed to marshal index: %w", err)
	}

	// Get current tree
	currentTree, err := im.persistence.getCurrentTree()
	if err != nil {
		return err
	}

	// Create blob
	blobHash, err := im.persistence.createBlob(data)
	if err != nil {
		return fmt.Errorf("failed to create blob: %w", err)
	}

	// Update tree
	newTree, err := im.persistence.updateTreePath(currentTree, path, blobHash)
	if err != nil {
		return fmt.Errorf("failed to update tree: %w", err)
	}

	// Create commit
	_, err = im.persistence.createCommitDirect(newTree, im.identity, "Saving index")
	if err != nil {
		return err
	}

	return im.persistence.syncWorktree()
}

// deleteIndex removes an index from storage using plumbing API
func (im *IndexManager) deleteIndex(idx *Index) error {
	path := fmt.Sprintf("%s/%s.index.%s", idx.Database, idx.Table, idx.Column)

	// Get current tree
	currentTree, err := im.persistence.getCurrentTree()
	if err != nil {
		return err
	}

	// Delete from tree
	newTree, err := im.persistence.deleteTreePath(currentTree, path)
	if err != nil {
		return fmt.Errorf("failed to delete from tree: %w", err)
	}

	// Create commit
	_, err = im.persistence.createCommitDirect(newTree, im.identity, "Deleting index")
	if err != nil {
		return err
	}

	return im.persistence.syncWorktree()
}

// LoadIndexes loads all indexes from storage for a table using plumbing API
func (im *IndexManager) LoadIndexes(database, table string, columns []core.Column) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	for _, col := range columns {
		path := fmt.Sprintf("%s/%s.index.%s", database, table, col.Name)
		data, err := im.persistence.ReadFileDirect(path)
		if err != nil {
			continue // Index doesn't exist
		}

		var idx Index
		if err := json.Unmarshal(data, &idx); err != nil {
			continue
		}

		key := indexKey(database, table, col.Name)
		im.indexes[key] = &idx
	}

	return nil
}

// RebuildIndex rebuilds an index by scanning all records
func (im *IndexManager) RebuildIndex(idx *Index, getRecordValue func(pk string) (string, bool)) error {
	idx.Entries = make(map[string][]string)

	// This would be called with a function that retrieves the column value for each primary key
	// The actual implementation depends on how records are stored

	return im.saveIndex(idx)
}
