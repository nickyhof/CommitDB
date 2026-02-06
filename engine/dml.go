// DML (Data Manipulation Language) operations: INSERT, UPDATE, and DELETE statements.
package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nickyhof/CommitDB/v2/core"
	"github.com/nickyhof/CommitDB/v2/internal/ops"
	"github.com/nickyhof/CommitDB/v2/persistence"
	"github.com/nickyhof/CommitDB/v2/internal/sql"
)

func (engine *Engine) executeInsertStatement(statement sql.InsertStatement) (CommitResult, error) {
	startTime := time.Now()

	tableOp, err := ops.GetTable(statement.Database, statement.Table, engine.Persistence)
	if err != nil {
		return CommitResult{}, err
	}

	if len(statement.Columns) != len(tableOp.Table.Columns) {
		return CommitResult{}, fmt.Errorf("statement column length does not match table column count")
	}

	pk, err := tableOp.PrimaryKey()
	if err != nil {
		return CommitResult{}, err
	}

	// Build column type map for validation
	columnTypes := make(map[string]core.ColumnType)
	for _, col := range tableOp.Table.Columns {
		columnTypes[col.Name] = col.Type
	}

	var txn persistence.Transaction
	recordsWritten := 0

	// Process each row in the bulk insert
	for _, valueRow := range statement.ValueRows {
		if len(statement.Columns) != len(valueRow) {
			return CommitResult{}, fmt.Errorf("value count does not match column count")
		}

		data := make(map[string]interface{})

		for index, column := range statement.Columns {
			value := valueRow[index]

			// Handle NOW() function - expand to current timestamp
			if strings.ToUpper(value) == "NOW()" {
				colType := columnTypes[column]
				if colType == core.DateType {
					value = time.Now().Format("2006-01-02")
				} else {
					value = time.Now().Format("2006-01-02 15:04:05")
				}
			}

			// Validate DATE/TIMESTAMP format
			colType := columnTypes[column]
			if colType == core.DateType {
				if _, err := parseDateTime(value); err != nil {
					// Try common date formats
					if !isValidDateFormat(value) {
						return CommitResult{}, fmt.Errorf("invalid DATE format for column %s: %s (expected YYYY-MM-DD)", column, value)
					}
				}
			} else if colType == core.TimestampType {
				if _, err := parseDateTime(value); err != nil {
					return CommitResult{}, fmt.Errorf("invalid TIMESTAMP format for column %s: %s (expected YYYY-MM-DD HH:MM:SS)", column, value)
				}
			} else if colType == core.JsonType {
				// Validate JSON format
				var js interface{}
				if err := json.Unmarshal([]byte(value), &js); err != nil {
					return CommitResult{}, fmt.Errorf("invalid JSON format for column %s: %s", column, err.Error())
				}
			}

			data[column] = value
		}

		pkValue := data[*pk].(string)
		jsonData, err := json.Marshal(data)
		if err != nil {
			return CommitResult{}, err
		}

		txn, err = tableOp.Put(pkValue, jsonData, engine.Identity)
		if err != nil {
			return CommitResult{}, err
		}
		recordsWritten++
	}

	return CommitResult{
		Transaction:      txn,
		DatabasesCreated: 0,
		DatabasesDeleted: 0,
		TablesCreated:    0,
		TablesDeleted:    0,
		RecordsWritten:   recordsWritten,
		RecordsDeleted:   0,
		ExecutionTimeMs:  float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:     recordsWritten,
	}, nil
}

// isValidDateFormat checks if the string is a valid date format
func isValidDateFormat(s string) bool {
	dateFormats := []string{
		"2006-01-02",
		"2006/01/02",
		"01/02/2006",
		"01-02-2006",
	}
	for _, format := range dateFormats {
		if _, err := time.Parse(format, s); err == nil {
			return true
		}
	}
	return false
}

func (engine *Engine) executeUpdateStatement(statement sql.UpdateStatement) (CommitResult, error) {
	startTime := time.Now()

	tableOp, err := ops.GetTable(statement.Database, statement.Table, engine.Persistence)
	if err != nil {
		return CommitResult{}, err
	}

	pk, err := tableOp.PrimaryKey()
	if err != nil {
		return CommitResult{}, err
	}

	// TODO: Add support for multiple conditions in the WHERE clause including non-PK columns

	if len(statement.Where.Conditions) > 0 {
		where := statement.Where.Conditions[0]

		if where.Left != *pk {
			return CommitResult{}, fmt.Errorf("currently only support primary key updates")
		}

		rawData, exists := tableOp.GetString(where.Right)
		if !exists {
			return CommitResult{}, errors.New("record not found")
		}

		var jsonData map[string]string
		err = json.Unmarshal([]byte(rawData), &jsonData)
		if err != nil {
			return CommitResult{}, err
		}

		for _, update := range statement.Updates {
			jsonData[update.Column] = update.Value
		}

		newData, err := json.Marshal(jsonData)
		if err != nil {
			return CommitResult{}, err
		}

		txn, err := tableOp.Put(where.Right, newData, engine.Identity)
		if err != nil {
			return CommitResult{}, err
		}

		return CommitResult{
			Transaction:      txn,
			DatabasesCreated: 0,
			DatabasesDeleted: 0,
			TablesCreated:    0,
			TablesDeleted:    0,
			RecordsWritten:   1,
			RecordsDeleted:   0,
			ExecutionTimeMs:  float64(time.Since(startTime).Milliseconds()),
			ExecutionOps:     1,
		}, nil
	} else {
		return CommitResult{}, fmt.Errorf("no WHERE clause provided in the UPDATE statement")
	}
}

func (engine *Engine) executeDeleteStatement(statement sql.DeleteStatement) (CommitResult, error) {
	startTime := time.Now()
	opCount := 1

	tableOp, err := ops.GetTable(statement.Database, statement.Table, engine.Persistence)
	if err != nil {
		return CommitResult{}, err
	}

	pk, err := tableOp.PrimaryKey()
	if err != nil {
		return CommitResult{}, err
	}

	// TODO: Add support for multiple conditions in the WHERE clause including non-PK columns

	if len(statement.Where.Conditions) > 0 {
		where := statement.Where.Conditions[0]

		if where.Left != *pk {
			return CommitResult{}, fmt.Errorf("currently only support primary key deletes")
		}

		opCount++
		txn, err := tableOp.Delete(where.Right, engine.Identity)
		if err != nil {
			return CommitResult{}, err
		}

		return CommitResult{
			Transaction:      txn,
			DatabasesCreated: 0,
			DatabasesDeleted: 0,
			TablesCreated:    0,
			TablesDeleted:    0,
			RecordsWritten:   0,
			RecordsDeleted:   1,
			ExecutionTimeMs:  float64(time.Since(startTime).Milliseconds()),
			ExecutionOps:     1,
		}, nil
	} else {
		return CommitResult{}, fmt.Errorf("no WHERE clause provided in the DELETE statement")
	}
}
