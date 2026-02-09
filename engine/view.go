// View operations: CREATE/DROP VIEW, materialized views, and time-travel queries.
package engine

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/nickyhof/CommitDB/v2/core"
	"github.com/nickyhof/CommitDB/v2/internal/sql"
	"github.com/nickyhof/CommitDB/v2/persistence"
)

func (engine *Engine) executeCreateViewStatement(statement sql.CreateViewStatement) (Result, error) {
	startTime := time.Now()

	// Create view definition
	view := core.View{
		Database:     statement.Database,
		Name:         statement.ViewName,
		Query:        statement.SelectQuery,
		Materialized: statement.Materialized,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	// Store the view definition
	txn, err := engine.Persistence.CreateView(view, engine.Identity)
	if err != nil {
		return nil, fmt.Errorf("failed to create view: %w", err)
	}

	// If materialized, run the query and cache results
	if statement.Materialized {
		if err := engine.refreshMaterializedView(statement.Database, statement.ViewName, statement.SelectQuery); err != nil {
			return nil, fmt.Errorf("failed to populate materialized view: %w", err)
		}
	}

	return CommitResult{
		Transaction:     txn,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeDropViewStatement(statement sql.DropViewStatement) (Result, error) {
	startTime := time.Now()

	// Check if view exists
	view, err := engine.Persistence.GetView(statement.Database, statement.ViewName)
	if err != nil {
		if statement.IfExists {
			return CommitResult{
				ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
			}, nil
		}
		return nil, fmt.Errorf("view %s.%s does not exist", statement.Database, statement.ViewName)
	}

	// Drop view definition
	txn, err := engine.Persistence.DropView(statement.Database, statement.ViewName, engine.Identity)
	if err != nil {
		return nil, fmt.Errorf("failed to drop view: %w", err)
	}

	// If materialized, also delete cached data
	if view.Materialized {
		_, _ = engine.Persistence.DeleteMaterializedViewData(statement.Database, statement.ViewName, engine.Identity)
	}

	return CommitResult{
		Transaction:     txn,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeShowViewsStatement(statement sql.ShowViewsStatement) (QueryResult, error) {
	startTime := time.Now()

	views, err := engine.Persistence.ListViews(statement.Database)
	if err != nil {
		return QueryResult{}, err
	}

	columns := []string{"name", "materialized", "query"}
	var data [][]string

	for _, view := range views {
		materialized := "NO"
		if view.Materialized {
			materialized = "YES"
		}
		data = append(data, []string{view.Name, materialized, view.Query})
	}

	return QueryResult{
		Columns:         columns,
		Data:            data,
		RecordsRead:     len(data),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeRefreshViewStatement(statement sql.RefreshViewStatement) (Result, error) {
	startTime := time.Now()

	// Get view definition
	view, err := engine.Persistence.GetView(statement.Database, statement.ViewName)
	if err != nil {
		return nil, fmt.Errorf("view %s.%s does not exist", statement.Database, statement.ViewName)
	}

	if !view.Materialized {
		return nil, fmt.Errorf("view %s.%s is not a materialized view", statement.Database, statement.ViewName)
	}

	// Refresh the cached data
	if err := engine.refreshMaterializedView(statement.Database, statement.ViewName, view.Query); err != nil {
		return nil, err
	}

	// Update view timestamp
	view.UpdatedAt = time.Now()
	_, _ = engine.Persistence.UpdateView(*view, engine.Identity)

	return CommitResult{
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) refreshMaterializedView(database, viewName, query string) error {
	// Execute the query
	result, err := engine.Execute(query)
	if err != nil {
		return fmt.Errorf("failed to execute view query: %w", err)
	}

	queryResult, ok := result.(QueryResult)
	if !ok {
		return fmt.Errorf("view query must be a SELECT statement")
	}

	// Convert Data ([][]string) to rows ([]map[string]string) for storage
	rows := make([]map[string]string, len(queryResult.Data))
	for i, row := range queryResult.Data {
		rowMap := make(map[string]string)
		for j, col := range queryResult.Columns {
			if j < len(row) {
				rowMap[col] = row[j]
			}
		}
		rows[i] = rowMap
	}

	// Store cached data
	_, err = engine.Persistence.WriteMaterializedViewData(database, viewName, rows, engine.Identity)
	if err != nil {
		return fmt.Errorf("failed to store materialized view data: %w", err)
	}

	return nil
}

// executeViewQuery executes a query against a regular (non-materialized) view
func (engine *Engine) executeViewQuery(view *core.View, originalStatement sql.SelectStatement, startTime time.Time) (QueryResult, error) {
	// Execute the view's underlying query
	result, err := engine.Execute(view.Query)
	if err != nil {
		return QueryResult{}, fmt.Errorf("failed to execute view query: %w", err)
	}

	queryResult, ok := result.(QueryResult)
	if !ok {
		return QueryResult{}, fmt.Errorf("view query must be a SELECT statement")
	}

	// Update timing
	queryResult.ExecutionTimeMs = float64(time.Since(startTime).Milliseconds())
	return queryResult, nil
}

// executeMaterializedViewQuery reads from cached materialized view data
func (engine *Engine) executeMaterializedViewQuery(view *core.View, originalStatement sql.SelectStatement, startTime time.Time) (QueryResult, error) {
	// Read cached data
	rows, err := engine.Persistence.ReadMaterializedViewData(view.Database, view.Name)
	if err != nil {
		return QueryResult{}, fmt.Errorf("failed to read materialized view data: %w", err)
	}

	// Determine columns (from first row or view definition)
	var columns []string
	if len(rows) > 0 {
		for col := range rows[0] {
			columns = append(columns, col)
		}
	}

	// Convert rows to Data format
	var data [][]string
	for _, row := range rows {
		rowData := make([]string, len(columns))
		for i, col := range columns {
			rowData[i] = row[col]
		}
		data = append(data, rowData)
	}

	return QueryResult{
		Columns:         columns,
		Data:            data,
		RecordsRead:     len(data),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

// executeTimeTravelSelect executes a SELECT query against data as it existed at a specific transaction.
func (engine *Engine) executeTimeTravelSelect(statement sql.SelectStatement, p *persistence.Persistence, startTime time.Time) (QueryResult, error) {
	transactionID := statement.AsOf

	// Check if this is a view instead of a table
	view, err := p.GetView(statement.Database, statement.Table)
	if err == nil {
		// This is a view - handle time-travel for views
		if view.Materialized {
			// For materialized views, get cached data at that transaction
			return engine.executeTimeTravelMaterializedView(view, transactionID, startTime)
		}
		// For regular views, execute view query with AS OF propagated
		return engine.executeTimeTravelRegularView(view, transactionID)
	}

	// Get table schema at that transaction
	table, err := p.GetTableAtTransaction(statement.Database, statement.Table, transactionID)
	if err != nil {
		return QueryResult{}, fmt.Errorf("failed to get table at transaction %s: %w", transactionID, err)
	}

	// Determine columns to select
	columns := []string{}
	if len(statement.Columns) == 0 {
		for _, column := range table.Columns {
			columns = append(columns, column.Name)
		}
	} else {
		columns = append(columns, statement.Columns...)
	}

	// Get all record keys at that transaction
	keys, err := p.ListRecordsAtTransaction(statement.Database, statement.Table, transactionID)
	if err != nil {
		return QueryResult{}, fmt.Errorf("failed to list records at transaction %s: %w", transactionID, err)
	}

	// Read each record at that transaction
	var results []map[string]string
	for _, key := range keys {
		rawData, exists, err := p.GetRecordAtTransaction(statement.Database, statement.Table, key, transactionID)
		if err != nil {
			return QueryResult{}, err
		}
		if !exists {
			continue
		}

		var jsonData map[string]string
		if err := json.Unmarshal(rawData, &jsonData); err != nil {
			continue
		}
		results = append(results, jsonData)
	}

	// Apply WHERE filter
	if len(statement.Where.Conditions) > 0 {
		var filtered []map[string]string
		for _, row := range results {
			if matchesWhereClause(row, statement.Where) {
				filtered = append(filtered, row)
			}
		}
		results = filtered
	}

	// Apply ORDER BY
	if len(statement.OrderBy) > 0 {
		sortResults(results, statement.OrderBy)
	}

	// Apply LIMIT and OFFSET
	if statement.Offset > 0 && statement.Offset < len(results) {
		results = results[statement.Offset:]
	} else if statement.Offset >= len(results) {
		results = nil
	}

	if statement.Limit > 0 && statement.Limit < len(results) {
		results = results[:statement.Limit]
	}

	// Build result data
	var data [][]string
	for _, row := range results {
		rowData := make([]string, len(columns))
		for i, col := range columns {
			rowData[i] = row[col]
		}
		data = append(data, rowData)
	}

	return QueryResult{
		Columns:         columns,
		Data:            data,
		RecordsRead:     len(results),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		Transaction:     persistence.Transaction{Id: transactionID},
	}, nil
}

// executeTimeTravelRegularView handles time-travel queries on regular (non-materialized) views.
// It parses the view's underlying query and injects the AS OF clause.
func (engine *Engine) executeTimeTravelRegularView(view *core.View, transactionID string) (QueryResult, error) {
	// Parse the view's underlying query
	parser := sql.NewParser(view.Query)
	stmt, err := parser.Parse()
	if err != nil {
		return QueryResult{}, fmt.Errorf("failed to parse view query: %w", err)
	}

	selectStmt, ok := stmt.(sql.SelectStatement)
	if !ok {
		return QueryResult{}, fmt.Errorf("view query must be a SELECT statement")
	}

	// Inject the AS OF clause into the underlying query
	selectStmt.AsOf = transactionID

	// Execute the modified query
	return engine.executeSelectStatement(selectStmt)
}

// executeTimeTravelMaterializedView handles time-travel queries on materialized views.
// It reads the cached view data as it existed at the specified transaction.
func (engine *Engine) executeTimeTravelMaterializedView(view *core.View, transactionID string, startTime time.Time) (QueryResult, error) {
	// Read materialized view data at that transaction
	rows, err := engine.Persistence.GetMaterializedViewDataAtTransaction(view.Database, view.Name, transactionID)
	if err != nil {
		return QueryResult{}, fmt.Errorf("failed to get materialized view data at transaction %s: %w", transactionID, err)
	}

	// Determine columns from the data
	var columns []string
	if len(rows) > 0 {
		for col := range rows[0] {
			columns = append(columns, col)
		}
		// Sort for consistent ordering
		sort.Strings(columns)
	}

	// Convert rows to Data format
	var data [][]string
	for _, row := range rows {
		rowData := make([]string, len(columns))
		for i, col := range columns {
			rowData[i] = row[col]
		}
		data = append(data, rowData)
	}

	return QueryResult{
		Columns:         columns,
		Data:            data,
		RecordsRead:     len(data),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		Transaction:     persistence.Transaction{Id: transactionID},
	}, nil
}
