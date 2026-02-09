// DDL (Data Definition Language) operations: CREATE/DROP TABLE/DATABASE, ALTER TABLE, indexes.
package engine

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nickyhof/CommitDB/v2/core"
	"github.com/nickyhof/CommitDB/v2/internal/ops"
	"github.com/nickyhof/CommitDB/v2/internal/sql"
	"github.com/nickyhof/CommitDB/v2/persistence"
)

func (engine *Engine) executeCreateTableStatement(statement sql.CreateTableStatement) (CommitResult, error) {
	startTime := time.Now()
	opCount := 1

	txn, _, err := ops.CreateTable(core.Table{
		Database: statement.Database,
		Name:     statement.Table,
		Columns:  statement.Columns,
	}, engine.Persistence, engine.Identity)
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		Transaction:      *txn,
		DatabasesCreated: 0,
		DatabasesDeleted: 0,
		TablesCreated:    1,
		TablesDeleted:    0,
		RecordsWritten:   0,
		RecordsDeleted:   0,
		ExecutionTimeMs:  float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:     opCount,
	}, nil
}

func (engine *Engine) executeDropTableStatement(statement sql.DropTableStatement) (CommitResult, error) {
	startTime := time.Now()
	opCount := 1

	tableOp, err := ops.GetTable(statement.Database, statement.Table, engine.Persistence)
	if err != nil {
		// If IF EXISTS was specified, don't error on missing table
		if statement.IfExists {
			return CommitResult{
				ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
				ExecutionOps:    opCount,
			}, nil
		}
		return CommitResult{}, err
	}

	opCount++
	txn, err := tableOp.DropTable(engine.Identity)
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		Transaction:      txn,
		DatabasesCreated: 0,
		DatabasesDeleted: 0,
		TablesCreated:    0,
		TablesDeleted:    1,
		RecordsWritten:   0,
		RecordsDeleted:   0,
		ExecutionTimeMs:  float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:     opCount,
	}, nil
}

func (engine *Engine) executeCreateDatabaseStatement(statement sql.CreateDatabaseStatement) (CommitResult, error) {
	startTime := time.Now()
	opCount := 1

	txn, _, err := ops.CreateDatabase(core.Database{Name: statement.Database}, engine.Persistence, engine.Identity)
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		Transaction:      *txn,
		DatabasesCreated: 1,
		DatabasesDeleted: 0,
		TablesCreated:    0,
		TablesDeleted:    0,
		RecordsWritten:   0,
		RecordsDeleted:   0,
		ExecutionTimeMs:  float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:     opCount,
	}, nil
}

func (engine *Engine) executeDropDatabaseStatement(statement sql.DropDatabaseStatement) (CommitResult, error) {
	startTime := time.Now()
	opCount := 1

	databaseOp, err := ops.GetDatabase(statement.Database, engine.Persistence)
	if err != nil {
		// If IF EXISTS was specified, don't error on missing database
		if statement.IfExists {
			return CommitResult{
				ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
				ExecutionOps:    opCount,
			}, nil
		}
		return CommitResult{}, err
	}

	opCount++
	txn, err := databaseOp.DropDatabase(engine.Identity)
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		Transaction:      txn,
		DatabasesCreated: 0,
		DatabasesDeleted: 1,
		TablesCreated:    0,
		TablesDeleted:    0,
		RecordsWritten:   0,
		RecordsDeleted:   0,
		ExecutionTimeMs:  float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:     opCount,
	}, nil
}

func (engine *Engine) executeShowDatabasesStatement(statement sql.ShowDatabasesStatement) (QueryResult, error) {
	startTime := time.Now()

	databases := engine.Persistence.ListDatabases()

	// Convert to row-per-database format
	data := make([][]string, len(databases))
	for i, db := range databases {
		data[i] = []string{db}
	}

	return QueryResult{
		Transaction:     engine.Persistence.LatestTransaction(),
		Columns:         []string{"name"},
		Data:            data,
		RecordsRead:     len(databases),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    len(databases),
	}, nil
}

func (engine *Engine) executeShowTablesStatement(statement sql.ShowTablesStatement) (QueryResult, error) {
	startTime := time.Now()

	tables := engine.Persistence.ListTables(statement.Database)

	// Convert to row-per-table format
	data := make([][]string, len(tables))
	for i, table := range tables {
		data[i] = []string{table}
	}

	return QueryResult{
		Transaction:     engine.Persistence.LatestTransaction(),
		Columns:         []string{"name"},
		Data:            data,
		RecordsRead:     len(tables),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    len(tables),
	}, nil
}

func (engine *Engine) executeCreateIndexStatement(statement sql.CreateIndexStatement) (CommitResult, error) {
	startTime := time.Now()
	opCount := 0

	// Get table to scan existing data
	tableOp, err := ops.GetTable(statement.Database, statement.Table, engine.Persistence)
	if err != nil {
		return CommitResult{}, fmt.Errorf("table not found: %v", err)
	}

	// Create index manager
	indexManager := persistence.NewIndexManager(engine.Persistence, engine.Identity)

	// Create the index (not yet persisted)
	idx, err := indexManager.CreateIndex(statement.Name, statement.Database, statement.Table, statement.Column, statement.Unique)
	if err != nil {
		return CommitResult{}, err
	}

	// Scan all existing rows and populate the index
	for pk, rawData := range tableOp.Scan() {
		opCount++
		var row map[string]string
		if err := json.Unmarshal(rawData, &row); err != nil {
			continue
		}

		columnValue, exists := row[statement.Column]
		if !exists {
			continue
		}

		if err := idx.Insert(columnValue, pk); err != nil {
			return CommitResult{}, fmt.Errorf("failed to build index: %v", err)
		}
	}

	// Save the populated index
	if err := indexManager.SaveIndex(idx); err != nil {
		return CommitResult{}, fmt.Errorf("failed to save index: %v", err)
	}

	return CommitResult{
		Transaction:     engine.Persistence.LatestTransaction(),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    opCount,
	}, nil
}

func (engine *Engine) executeDropIndexStatement(statement sql.DropIndexStatement) (CommitResult, error) {
	startTime := time.Now()
	opCount := 1

	// Create index manager
	indexManager := persistence.NewIndexManager(engine.Persistence, engine.Identity)

	// Find and drop the index by looking it up
	err := indexManager.DropIndex(statement.Database, statement.Table, statement.Name)
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		Transaction:     engine.Persistence.LatestTransaction(),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    opCount,
	}, nil
}

func (engine *Engine) executeAlterTableStatement(statement sql.AlterTableStatement) (CommitResult, error) {
	startTime := time.Now()
	opCount := 1

	// Get existing table
	table, err := engine.Persistence.GetTable(statement.Database, statement.Table)
	if err != nil {
		return CommitResult{}, fmt.Errorf("table %s.%s does not exist", statement.Database, statement.Table)
	}

	switch statement.Action {
	case "ADD":
		// Check if column already exists
		for _, col := range table.Columns {
			if col.Name == statement.ColumnName {
				return CommitResult{}, fmt.Errorf("column %s already exists", statement.ColumnName)
			}
		}
		// Parse column type
		colType := parseColumnType(statement.ColumnType)
		table.Columns = append(table.Columns, core.Column{
			Name: statement.ColumnName,
			Type: colType,
		})

	case "DROP":
		// Find and remove column
		found := false
		newColumns := make([]core.Column, 0, len(table.Columns))
		for _, col := range table.Columns {
			if col.Name == statement.ColumnName {
				if col.PrimaryKey {
					return CommitResult{}, fmt.Errorf("cannot drop primary key column %s", statement.ColumnName)
				}
				found = true
				continue
			}
			newColumns = append(newColumns, col)
		}
		if !found {
			return CommitResult{}, fmt.Errorf("column %s does not exist", statement.ColumnName)
		}
		table.Columns = newColumns

	case "MODIFY":
		// Find and update column type
		found := false
		for i, col := range table.Columns {
			if col.Name == statement.ColumnName {
				colType := parseColumnType(statement.ColumnType)
				table.Columns[i].Type = colType
				found = true
				break
			}
		}
		if !found {
			return CommitResult{}, fmt.Errorf("column %s does not exist", statement.ColumnName)
		}

	case "RENAME":
		// Check new name doesn't already exist
		for _, col := range table.Columns {
			if col.Name == statement.NewColumnName {
				return CommitResult{}, fmt.Errorf("column %s already exists", statement.NewColumnName)
			}
		}
		// Find and rename column
		found := false
		for i, col := range table.Columns {
			if col.Name == statement.ColumnName {
				table.Columns[i].Name = statement.NewColumnName
				found = true
				break
			}
		}
		if !found {
			return CommitResult{}, fmt.Errorf("column %s does not exist", statement.ColumnName)
		}

	default:
		return CommitResult{}, fmt.Errorf("unknown ALTER action: %s", statement.Action)
	}

	// Update table schema
	message := fmt.Sprintf("ALTER TABLE %s.%s %s COLUMN %s", statement.Database, statement.Table, statement.Action, statement.ColumnName)
	txn, err := engine.Persistence.UpdateTable(*table, engine.Identity, message)
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		Transaction:     txn,
		TablesAltered:   1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    opCount,
	}, nil
}

// parseColumnType converts string type to core.ColumnType
func parseColumnType(typeName string) core.ColumnType {
	switch strings.ToUpper(typeName) {
	case "INT", "INTEGER":
		return core.IntType
	case "STRING", "VARCHAR":
		return core.StringType
	case "FLOAT", "DOUBLE", "REAL":
		return core.FloatType
	case "BOOL", "BOOLEAN":
		return core.BoolType
	case "TEXT":
		return core.TextType
	case "DATE":
		return core.DateType
	case "TIMESTAMP", "DATETIME":
		return core.TimestampType
	case "JSON":
		return core.JsonType
	default:
		return core.StringType
	}
}

func (engine *Engine) executeDescribeStatement(statement sql.DescribeStatement) (QueryResult, error) {
	startTime := time.Now()
	opCount := 1

	tableOp, err := ops.GetTable(statement.Database, statement.Table, engine.Persistence)
	if err != nil {
		return QueryResult{}, err
	}

	// Build column info
	var data [][]string
	for _, col := range tableOp.Table.Columns {
		typeStr := ""
		switch col.Type {
		case core.StringType:
			typeStr = "STRING"
		case core.IntType:
			typeStr = "INT"
		case core.FloatType:
			typeStr = "FLOAT"
		case core.BoolType:
			typeStr = "BOOL"
		case core.TextType:
			typeStr = "TEXT"
		case core.DateType:
			typeStr = "DATE"
		case core.TimestampType:
			typeStr = "TIMESTAMP"
		case core.JsonType:
			typeStr = "JSON"
		}

		pkStr := "NO"
		if col.PrimaryKey {
			pkStr = "YES"
		}

		data = append(data, []string{col.Name, typeStr, pkStr})
	}

	return QueryResult{
		Transaction:     engine.Persistence.LatestTransaction(),
		Columns:         []string{"Column", "Type", "PrimaryKey"},
		Data:            data,
		RecordsRead:     len(data),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    opCount,
	}, nil
}

func (engine *Engine) executeShowIndexesStatement(statement sql.ShowIndexesStatement) (QueryResult, error) {
	startTime := time.Now()

	// Get table to find columns
	tableOp, err := ops.GetTable(statement.Database, statement.Table, engine.Persistence)
	if err != nil {
		return QueryResult{}, err
	}

	// Load indexes
	indexManager := persistence.NewIndexManager(engine.Persistence, engine.Identity)
	_ = indexManager.LoadIndexes(statement.Database, statement.Table, tableOp.Table.Columns)

	// Build index info
	var data [][]string
	for _, col := range tableOp.Table.Columns {
		idx, exists := indexManager.GetIndex(statement.Database, statement.Table, col.Name)
		if exists {
			uniqueStr := "NO"
			if idx.Unique {
				uniqueStr = "YES"
			}
			data = append(data, []string{idx.Name, col.Name, uniqueStr})
		}
	}

	return QueryResult{
		Transaction:     engine.Persistence.LatestTransaction(),
		Columns:         []string{"Name", "Column", "Unique"},
		Data:            data,
		RecordsRead:     len(data),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    len(data),
	}, nil
}
