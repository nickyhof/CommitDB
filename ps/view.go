package ps

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nickyhof/CommitDB/core"
)

// CreateView stores a view definition
func (persistence *Persistence) CreateView(view core.View, identity core.Identity) (txn Transaction, err error) {
	path := fmt.Sprintf(".commitdb/views/%s/%s.json", view.Database, view.Name)

	dataBytes, err := json.MarshalIndent(view, "", "  ")
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to marshal view: %w", err)
	}

	return persistence.WriteFileDirect(path, dataBytes, identity, fmt.Sprintf("Creating view %s.%s", view.Database, view.Name))
}

// GetView retrieves a view definition
func (persistence *Persistence) GetView(database, name string) (*core.View, error) {
	path := fmt.Sprintf(".commitdb/views/%s/%s.json", database, name)

	data, err := persistence.ReadFileDirect(path)
	if err != nil {
		return nil, fmt.Errorf("view %s.%s does not exist: %w", database, name, err)
	}

	var view core.View
	if err := json.Unmarshal(data, &view); err != nil {
		return nil, fmt.Errorf("failed to unmarshal view: %w", err)
	}

	return &view, nil
}

// ListViews returns all views in a database
func (persistence *Persistence) ListViews(database string) ([]core.View, error) {
	path := fmt.Sprintf(".commitdb/views/%s", database)

	entries, err := persistence.ListEntriesDirect(path)
	if err != nil {
		// No views directory yet - return empty list
		return []core.View{}, nil
	}

	var views []core.View
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name, ".json") {
			continue
		}

		viewName := strings.TrimSuffix(entry.Name, ".json")
		view, err := persistence.GetView(database, viewName)
		if err != nil {
			continue // Skip invalid views
		}
		views = append(views, *view)
	}

	return views, nil
}

// DropView removes a view definition
func (persistence *Persistence) DropView(database, name string, identity core.Identity) (txn Transaction, err error) {
	paths := []string{
		fmt.Sprintf(".commitdb/views/%s/%s.json", database, name),
	}

	return persistence.DeletePathDirect(paths, identity, fmt.Sprintf("Dropping view %s.%s", database, name))
}

// UpdateView updates a view definition (used for refresh timestamps)
func (persistence *Persistence) UpdateView(view core.View, identity core.Identity) (txn Transaction, err error) {
	path := fmt.Sprintf(".commitdb/views/%s/%s.json", view.Database, view.Name)

	dataBytes, err := json.MarshalIndent(view, "", "  ")
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to marshal view: %w", err)
	}

	return persistence.WriteFileDirect(path, dataBytes, identity, fmt.Sprintf("Updating view %s.%s", view.Database, view.Name))
}

// Materialized view data storage

// WriteMaterializedViewData stores cached data for a materialized view
func (persistence *Persistence) WriteMaterializedViewData(database, viewName string, rows []map[string]string, identity core.Identity) (txn Transaction, err error) {
	basePath := fmt.Sprintf(".commitdb/materialized/%s/%s", database, viewName)

	// Write all rows as a single JSON array
	dataPath := fmt.Sprintf("%s/data.json", basePath)
	dataBytes, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to marshal rows: %w", err)
	}

	return persistence.WriteFileDirect(dataPath, dataBytes, identity, fmt.Sprintf("Refreshing materialized view %s.%s", database, viewName))
}

// ReadMaterializedViewData reads cached data for a materialized view
func (persistence *Persistence) ReadMaterializedViewData(database, viewName string) ([]map[string]string, error) {
	dataPath := fmt.Sprintf(".commitdb/materialized/%s/%s/data.json", database, viewName)

	data, err := persistence.ReadFileDirect(dataPath)
	if err != nil {
		return nil, fmt.Errorf("materialized view data not found: %w", err)
	}

	var rows []map[string]string
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("failed to unmarshal materialized view data: %w", err)
	}

	return rows, nil
}

// DeleteMaterializedViewData removes cached data for a materialized view
func (persistence *Persistence) DeleteMaterializedViewData(database, viewName string, identity core.Identity) (txn Transaction, err error) {
	path := fmt.Sprintf(".commitdb/materialized/%s/%s", database, viewName)

	return persistence.DeletePathDirect([]string{path}, identity, fmt.Sprintf("Deleting materialized view data %s.%s", database, viewName))
}
