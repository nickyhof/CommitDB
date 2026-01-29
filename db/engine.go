package db

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nickyhof/CommitDB/core"
	"github.com/nickyhof/CommitDB/op"
	"github.com/nickyhof/CommitDB/ps"
	"github.com/nickyhof/CommitDB/sql"
)

type Engine struct {
	*ps.Persistence
	QueryContext
}

func NewEngine(persistence *ps.Persistence, identity core.Identity) *Engine {
	return &Engine{
		Persistence:  persistence,
		QueryContext: QueryContext{Identity: identity},
	}
}

func (engine *Engine) Execute(query string) (Result, error) {
	parser := sql.NewParser(query)
	statement, err := parser.Parse()
	if err != nil {
		return nil, err
	}

	switch statement.Type() {
	case sql.SelectStatementType:
		return engine.executeSelectStatement(statement.(sql.SelectStatement))
	case sql.InsertStatementType:
		return engine.executeInsertStatement(statement.(sql.InsertStatement))
	case sql.UpdateStatementType:
		return engine.executeUpdateStatement(statement.(sql.UpdateStatement))
	case sql.DeleteStatementType:
		return engine.executeDeleteStatement(statement.(sql.DeleteStatement))
	case sql.CreateTableStatementType:
		return engine.executeCreateTableStatement(statement.(sql.CreateTableStatement))
	case sql.DropTableStatementType:
		return engine.executeDropTableStatement(statement.(sql.DropTableStatement))
	case sql.CreateDatabaseStatementType:
		return engine.executeCreateDatabaseStatement(statement.(sql.CreateDatabaseStatement))
	case sql.DropDatabaseStatementType:
		return engine.executeDropDatabaseStatement(statement.(sql.DropDatabaseStatement))
	case sql.CreateIndexStatementType:
		return engine.executeCreateIndexStatement(statement.(sql.CreateIndexStatement))
	case sql.DropIndexStatementType:
		return engine.executeDropIndexStatement(statement.(sql.DropIndexStatement))
	case sql.AlterTableStatementType:
		return engine.executeAlterTableStatement(statement.(sql.AlterTableStatement))
	case sql.BeginStatementType:
		return engine.executeBeginStatement()
	case sql.CommitStatementType:
		return engine.executeCommitStatement()
	case sql.RollbackStatementType:
		return engine.executeRollbackStatement()
	case sql.DescribeStatementType:
		return engine.executeDescribeStatement(statement.(sql.DescribeStatement))
	case sql.ShowDatabasesStatementType:
		return engine.executeShowDatabasesStatement(statement.(sql.ShowDatabasesStatement))
	case sql.ShowTablesStatementType:
		return engine.executeShowTablesStatement(statement.(sql.ShowTablesStatement))
	case sql.ShowIndexesStatementType:
		return engine.executeShowIndexesStatement(statement.(sql.ShowIndexesStatement))
	case sql.CreateBranchStatementType:
		return engine.executeCreateBranchStatement(statement.(sql.CreateBranchStatement))
	case sql.CheckoutStatementType:
		return engine.executeCheckoutStatement(statement.(sql.CheckoutStatement))
	case sql.MergeStatementType:
		return engine.executeMergeStatement(statement.(sql.MergeStatement))
	case sql.ShowBranchesStatementType:
		return engine.executeShowBranchesStatement(statement.(sql.ShowBranchesStatement))
	case sql.ShowMergeConflictsStatementType:
		return engine.executeShowMergeConflictsStatement()
	case sql.ResolveConflictStatementType:
		return engine.executeResolveConflictStatement(statement.(sql.ResolveConflictStatement))
	case sql.CommitMergeStatementType:
		return engine.executeCommitMergeStatement()
	case sql.AbortMergeStatementType:
		return engine.executeAbortMergeStatement()
	case sql.AddRemoteStatementType:
		return engine.executeAddRemoteStatement(statement.(sql.AddRemoteStatement))
	case sql.ShowRemotesStatementType:
		return engine.executeShowRemotesStatement()
	case sql.DropRemoteStatementType:
		return engine.executeDropRemoteStatement(statement.(sql.DropRemoteStatement))
	case sql.PushStatementType:
		return engine.executePushStatement(statement.(sql.PushStatement))
	case sql.PullStatementType:
		return engine.executePullStatement(statement.(sql.PullStatement))
	case sql.FetchStatementType:
		return engine.executeFetchStatement(statement.(sql.FetchStatement))
	case sql.CopyStatementType:
		return engine.executeCopyStatement(statement.(sql.CopyStatement))
	case sql.CreateShareStatementType:
		return engine.executeCreateShareStatement(statement.(sql.CreateShareStatement))
	case sql.SyncShareStatementType:
		return engine.executeSyncShareStatement(statement.(sql.SyncShareStatement))
	case sql.DropShareStatementType:
		return engine.executeDropShareStatement(statement.(sql.DropShareStatement))
	case sql.ShowSharesStatementType:
		return engine.executeShowSharesStatement()
	default:
		return nil, fmt.Errorf("unsupported statement type: %v", statement.Type())
	}
}

func (engine *Engine) executeSelectStatement(statement sql.SelectStatement) (QueryResult, error) {
	startTime := time.Now()
	rowsScanned := 0

	// Determine which persistence to use - share or local
	persistence := engine.Persistence
	if statement.Share != "" {
		sharePersistence, err := engine.Persistence.OpenSharePersistence(statement.Share)
		if err != nil {
			return QueryResult{}, fmt.Errorf("failed to access share '%s': %w", statement.Share, err)
		}
		persistence = sharePersistence
	}

	tableOp, err := op.GetTable(statement.Database, statement.Table, persistence)
	if err != nil {
		return QueryResult{}, err
	}

	// Determine columns to select
	columns := []string{}
	if len(statement.Columns) == 0 {
		for _, column := range tableOp.Table.Columns {
			columns = append(columns, column.Name)
		}
	} else {
		columns = append(columns, statement.Columns...)
	}

	// Try to use an index for WHERE clause optimization
	var results []map[string]string
	indexUsed := false

	if len(statement.Where.Conditions) > 0 && len(statement.Joins) == 0 {
		// Load indexes for this table
		indexManager := ps.NewIndexManager(persistence, engine.Identity)
		indexManager.LoadIndexes(statement.Database, statement.Table, tableOp.Table.Columns)

		// Check if any WHERE condition can use an index (simple equality for now)
		for _, cond := range statement.Where.Conditions {
			if cond.Operator == sql.EqualsOperator {
				if idx, found := indexManager.GetIndex(statement.Database, statement.Table, cond.Left); found {
					// Use index lookup!
					primaryKeys := idx.Lookup(cond.Right)
					for _, pk := range primaryKeys {
						rowsScanned++
						rawData, exists := tableOp.Get(pk)
						if !exists {
							continue
						}
						var jsonData map[string]string
						if err := json.Unmarshal(rawData, &jsonData); err != nil {
							continue
						}
						results = append(results, jsonData)
					}
					indexUsed = true
					break // Only use first matching index
				}
			}
		}
	}

	// Fall back to full scan if no index was used
	if !indexUsed {
		for _, rawData := range tableOp.Scan() {
			rowsScanned++

			var jsonData map[string]string
			err := json.Unmarshal(rawData, &jsonData)
			if err != nil {
				return QueryResult{}, err
			}

			results = append(results, jsonData)
		}
	}

	// Execute JOINs
	for _, join := range statement.Joins {
		var joinTableOp *op.TableOp
		var err error

		// Check if this is a share table (3-level naming)
		if join.Share != "" {
			// Open share persistence
			sharePersistence, shareErr := engine.Persistence.OpenSharePersistence(join.Share)
			if shareErr != nil {
				return QueryResult{}, fmt.Errorf("failed to open share '%s' for join: %w", join.Share, shareErr)
			}
			joinTableOp, err = op.GetTable(join.Database, join.Table, sharePersistence)
		} else {
			joinTableOp, err = op.GetTable(join.Database, join.Table, engine.Persistence)
		}

		if err != nil {
			if join.Share != "" {
				return QueryResult{}, fmt.Errorf("join table not found: %s.%s.%s", join.Share, join.Database, join.Table)
			}
			return QueryResult{}, fmt.Errorf("join table not found: %s.%s", join.Database, join.Table)
		}

		// Scan join table
		var joinRows []map[string]string
		for _, rawData := range joinTableOp.Scan() {
			rowsScanned++
			var jsonData map[string]string
			if err := json.Unmarshal(rawData, &jsonData); err != nil {
				continue
			}
			joinRows = append(joinRows, jsonData)
		}

		// Perform the join
		results = executeJoin(results, joinRows, join)

		// Add join table columns to output columns if selecting *
		if len(statement.Columns) == 0 {
			for _, col := range joinTableOp.Table.Columns {
				columns = append(columns, col.Name)
			}
		}
	}

	// Apply WHERE clause filtering (after joins)
	if len(statement.Where.Conditions) > 0 {
		var filtered []map[string]string
		for _, row := range results {
			if matchesWhereClause(row, statement.Where) {
				filtered = append(filtered, row)
			}
		}
		results = filtered
	}

	// Apply DISTINCT if requested
	if statement.Distinct {
		results = applyDistinct(results, columns)
	}

	// Apply ORDER BY if present
	if len(statement.OrderBy) > 0 {
		sortResults(results, statement.OrderBy)
	}

	// Handle COUNT(*) - return count before LIMIT/OFFSET
	if statement.CountAll {
		countResult := [][]string{{strconv.Itoa(len(results))}}
		return QueryResult{
			Transaction:     engine.Persistence.LatestTransaction(),
			Columns:         []string{"COUNT(*)"},
			Data:            countResult,
			RecordsRead:     len(results),
			ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
			ExecutionOps:    rowsScanned,
		}, nil
	}

	// Handle aggregate functions (SUM, AVG, MIN, MAX)
	if len(statement.Aggregates) > 0 {
		return executeAggregates(results, statement, engine.Persistence.LatestTransaction(), startTime, rowsScanned)
	}

	// Handle string functions
	if len(statement.Functions) > 0 {
		return executeStringFunctions(results, statement, engine.Persistence.LatestTransaction(), startTime, rowsScanned)
	}

	// Apply OFFSET
	if statement.Offset > 0 {
		if statement.Offset >= len(results) {
			results = []map[string]string{}
		} else {
			results = results[statement.Offset:]
		}
	}

	// Apply LIMIT
	if statement.Limit > 0 && len(results) > statement.Limit {
		results = results[:statement.Limit]
	}

	// Convert results to column-based output
	outputData := make([][]string, len(results))
	for i, row := range results {
		outputData[i] = make([]string, len(columns))
		for j, col := range columns {
			outputData[i][j] = row[col]
		}
	}

	return QueryResult{
		Transaction:     engine.Persistence.LatestTransaction(),
		Columns:         columns,
		Data:            outputData,
		RecordsRead:     len(outputData),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    rowsScanned,
	}, nil
}

// executeAggregates handles SUM, AVG, MIN, MAX aggregate functions
func executeAggregates(results []map[string]string, statement sql.SelectStatement, txn ps.Transaction, startTime time.Time, opCount int) (QueryResult, error) {
	// Group results if GROUP BY is present
	groups := make(map[string][]map[string]string)

	if len(statement.GroupBy) > 0 {
		for _, row := range results {
			// Build group key
			keyParts := make([]string, len(statement.GroupBy))
			for i, col := range statement.GroupBy {
				keyParts[i] = row[col]
			}
			key := strings.Join(keyParts, "|")
			groups[key] = append(groups[key], row)
		}
	} else {
		// Single group for all rows
		groups[""] = results
	}

	// Calculate aggregates for each group
	var outputColumns []string
	var outputData [][]string

	// Add GROUP BY columns first
	outputColumns = append(outputColumns, statement.GroupBy...)

	// Add aggregate columns
	for _, agg := range statement.Aggregates {
		colName := agg.Function + "(" + agg.Column + ")"
		if agg.Alias != "" {
			colName = agg.Alias
		}
		outputColumns = append(outputColumns, colName)
	}

	// Process each group
	for groupKey, groupRows := range groups {
		row := make([]string, 0)

		// Add GROUP BY values
		if len(statement.GroupBy) > 0 {
			keyParts := strings.Split(groupKey, "|")
			row = append(row, keyParts...)
		}

		// Calculate each aggregate
		for _, agg := range statement.Aggregates {
			value := calculateAggregate(groupRows, agg.Function, agg.Column)
			row = append(row, value)
		}

		outputData = append(outputData, row)
	}

	return QueryResult{
		Transaction:     txn,
		Columns:         outputColumns,
		Data:            outputData,
		RecordsRead:     len(results),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    opCount,
	}, nil
}

// calculateAggregate calculates a single aggregate function over a set of rows
func calculateAggregate(rows []map[string]string, function, column string) string {
	if len(rows) == 0 {
		return "0"
	}

	switch function {
	case "COUNT":
		return strconv.Itoa(len(rows))

	case "SUM":
		sum := 0.0
		for _, row := range rows {
			val, err := strconv.ParseFloat(row[column], 64)
			if err == nil {
				sum += val
			}
		}
		if sum == float64(int(sum)) {
			return strconv.Itoa(int(sum))
		}
		return strconv.FormatFloat(sum, 'f', 2, 64)

	case "AVG":
		sum := 0.0
		count := 0
		for _, row := range rows {
			val, err := strconv.ParseFloat(row[column], 64)
			if err == nil {
				sum += val
				count++
			}
		}
		if count == 0 {
			return "0"
		}
		avg := sum / float64(count)
		return strconv.FormatFloat(avg, 'f', 2, 64)

	case "MIN":
		var minVal *float64
		for _, row := range rows {
			val, err := strconv.ParseFloat(row[column], 64)
			if err == nil {
				if minVal == nil || val < *minVal {
					minVal = &val
				}
			}
		}
		if minVal == nil {
			return ""
		}
		if *minVal == float64(int(*minVal)) {
			return strconv.Itoa(int(*minVal))
		}
		return strconv.FormatFloat(*minVal, 'f', 2, 64)

	case "MAX":
		var maxVal *float64
		for _, row := range rows {
			val, err := strconv.ParseFloat(row[column], 64)
			if err == nil {
				if maxVal == nil || val > *maxVal {
					maxVal = &val
				}
			}
		}
		if maxVal == nil {
			return ""
		}
		if *maxVal == float64(int(*maxVal)) {
			return strconv.Itoa(int(*maxVal))
		}
		return strconv.FormatFloat(*maxVal, 'f', 2, 64)

	default:
		return ""
	}
}

// executeStringFunctions handles string functions like UPPER, LOWER, CONCAT, SUBSTRING, TRIM, LENGTH, REPLACE
func executeStringFunctions(results []map[string]string, statement sql.SelectStatement, txn ps.Transaction, startTime time.Time, opCount int) (QueryResult, error) {
	// Apply OFFSET
	if statement.Offset > 0 {
		if statement.Offset >= len(results) {
			results = []map[string]string{}
		} else {
			results = results[statement.Offset:]
		}
	}

	// Apply LIMIT
	if statement.Limit > 0 && len(results) > statement.Limit {
		results = results[:statement.Limit]
	}

	// Build output columns (function results + additional columns)
	var outputColumns []string
	for _, fn := range statement.Functions {
		if fn.Alias != "" {
			outputColumns = append(outputColumns, fn.Alias)
		} else {
			outputColumns = append(outputColumns, fn.Function+"("+strings.Join(fn.Args, ", ")+")")
		}
	}
	// Add any regular columns
	for _, col := range statement.Columns {
		outputColumns = append(outputColumns, col)
	}

	// Evaluate functions for each row
	outputData := make([][]string, len(results))
	for i, row := range results {
		rowData := make([]string, len(outputColumns))
		colIdx := 0

		// Evaluate each function
		for _, fn := range statement.Functions {
			rowData[colIdx] = evalStringFunction(fn, row)
			colIdx++
		}

		// Add regular column values
		for _, col := range statement.Columns {
			rowData[colIdx] = row[col]
			colIdx++
		}

		outputData[i] = rowData
	}

	return QueryResult{
		Transaction:     txn,
		Columns:         outputColumns,
		Data:            outputData,
		RecordsRead:     len(outputData),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    opCount,
	}, nil
}

// evalStringFunction evaluates a string function on a row
func evalStringFunction(fn sql.FunctionExpr, row map[string]string) string {
	// Resolve arguments (column names get value from row, literals stay as-is)
	args := make([]string, len(fn.Args))
	for i, arg := range fn.Args {
		if val, ok := row[arg]; ok {
			args[i] = val
		} else {
			args[i] = arg // literal value
		}
	}

	switch fn.Function {
	case "UPPER":
		if len(args) >= 1 {
			return strings.ToUpper(args[0])
		}
	case "LOWER":
		if len(args) >= 1 {
			return strings.ToLower(args[0])
		}
	case "CONCAT":
		return strings.Join(args, "")
	case "SUBSTRING":
		if len(args) >= 2 {
			start, _ := strconv.Atoi(args[1])
			if start < 1 {
				start = 1
			}
			str := args[0]
			if start > len(str) {
				return ""
			}
			if len(args) >= 3 {
				length, _ := strconv.Atoi(args[2])
				end := start - 1 + length
				if end > len(str) {
					end = len(str)
				}
				return str[start-1 : end]
			}
			return str[start-1:]
		}
	case "TRIM":
		if len(args) >= 1 {
			return strings.TrimSpace(args[0])
		}
	case "LENGTH":
		if len(args) >= 1 {
			return strconv.Itoa(len(args[0]))
		}
	case "REPLACE":
		if len(args) >= 3 {
			return strings.ReplaceAll(args[0], args[1], args[2])
		}
	// Date functions
	case "NOW":
		return time.Now().Format("2006-01-02 15:04:05")
	case "DATE":
		if len(args) >= 1 {
			// Parse and return just the date part
			t, err := parseDateTime(args[0])
			if err == nil {
				return t.Format("2006-01-02")
			}
			return args[0]
		}
		return time.Now().Format("2006-01-02")
	case "YEAR":
		if len(args) >= 1 {
			t, err := parseDateTime(args[0])
			if err == nil {
				return strconv.Itoa(t.Year())
			}
		}
		return strconv.Itoa(time.Now().Year())
	case "MONTH":
		if len(args) >= 1 {
			t, err := parseDateTime(args[0])
			if err == nil {
				return strconv.Itoa(int(t.Month()))
			}
		}
		return strconv.Itoa(int(time.Now().Month()))
	case "DAY":
		if len(args) >= 1 {
			t, err := parseDateTime(args[0])
			if err == nil {
				return strconv.Itoa(t.Day())
			}
		}
		return strconv.Itoa(time.Now().Day())
	case "HOUR":
		if len(args) >= 1 {
			t, err := parseDateTime(args[0])
			if err == nil {
				return strconv.Itoa(t.Hour())
			}
		}
		return strconv.Itoa(time.Now().Hour())
	case "MINUTE":
		if len(args) >= 1 {
			t, err := parseDateTime(args[0])
			if err == nil {
				return strconv.Itoa(t.Minute())
			}
		}
		return strconv.Itoa(time.Now().Minute())
	case "SECOND":
		if len(args) >= 1 {
			t, err := parseDateTime(args[0])
			if err == nil {
				return strconv.Itoa(t.Second())
			}
		}
		return strconv.Itoa(time.Now().Second())
	case "DATE_ADD":
		// DATE_ADD(date, interval, unit) - e.g., DATE_ADD(date, 7, 'DAY')
		if len(args) >= 3 {
			t, err := parseDateTime(args[0])
			if err == nil {
				interval, _ := strconv.Atoi(args[1])
				unit := strings.ToUpper(args[2])
				return addToDate(t, interval, unit).Format("2006-01-02 15:04:05")
			}
		}
	case "DATE_SUB":
		// DATE_SUB(date, interval, unit)
		if len(args) >= 3 {
			t, err := parseDateTime(args[0])
			if err == nil {
				interval, _ := strconv.Atoi(args[1])
				unit := strings.ToUpper(args[2])
				return addToDate(t, -interval, unit).Format("2006-01-02 15:04:05")
			}
		}
	case "DATEDIFF":
		// DATEDIFF(date1, date2) - returns days between dates
		if len(args) >= 2 {
			t1, err1 := parseDateTime(args[0])
			t2, err2 := parseDateTime(args[1])
			if err1 == nil && err2 == nil {
				diff := t1.Sub(t2)
				return strconv.Itoa(int(diff.Hours() / 24))
			}
		}
	case "DATE_FORMAT":
		// DATE_FORMAT(date, format)
		if len(args) >= 2 {
			t, err := parseDateTime(args[0])
			if err == nil {
				return formatDate(t, args[1])
			}
		}
	// JSON functions
	case "JSON_EXTRACT":
		// JSON_EXTRACT(json, path) - e.g., JSON_EXTRACT(data, '$.name')
		if len(args) >= 2 {
			return jsonExtract(args[0], args[1])
		}
	case "JSON_KEYS":
		// JSON_KEYS(json) - returns comma-separated list of keys
		if len(args) >= 1 {
			return jsonKeys(args[0])
		}
	case "JSON_LENGTH":
		// JSON_LENGTH(json) - returns number of elements
		if len(args) >= 1 {
			return jsonLength(args[0])
		}
	case "JSON_TYPE":
		// JSON_TYPE(json) - returns type (object, array, string, number, boolean, null)
		if len(args) >= 1 {
			return jsonType(args[0])
		}
	case "JSON_CONTAINS":
		// JSON_CONTAINS(json, value) - returns 1 if value exists, 0 otherwise
		if len(args) >= 2 {
			return jsonContains(args[0], args[1])
		}
	}
	return ""
}

// jsonExtract extracts a value from JSON using a path like $.key.nested
func jsonExtract(jsonStr, path string) string {
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return ""
	}
	// Parse path (supports $.key.nested format)
	path = strings.TrimPrefix(path, "$")
	if path == "" || path == "." {
		// Return whole JSON
		return jsonStr
	}
	parts := strings.Split(strings.TrimPrefix(path, "."), ".")
	current := data
	for _, part := range parts {
		if part == "" {
			continue
		}
		// Check for array index like [0]
		if strings.HasPrefix(part, "[") && strings.HasSuffix(part, "]") {
			idx, err := strconv.Atoi(part[1 : len(part)-1])
			if err != nil {
				return ""
			}
			if arr, ok := current.([]interface{}); ok && idx < len(arr) {
				current = arr[idx]
			} else {
				return ""
			}
		} else if obj, ok := current.(map[string]interface{}); ok {
			if val, exists := obj[part]; exists {
				current = val
			} else {
				return ""
			}
		} else {
			return ""
		}
	}
	// Return result as string
	switch v := current.(type) {
	case string:
		return v
	case float64:
		if v == float64(int(v)) {
			return strconv.Itoa(int(v))
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case nil:
		return "null"
	default:
		// Return as JSON
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// jsonKeys returns comma-separated list of object keys
func jsonKeys(jsonStr string) string {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return ""
	}
	var keys []string
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// jsonLength returns length of array or object
func jsonLength(jsonStr string) string {
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return "0"
	}
	switch v := data.(type) {
	case []interface{}:
		return strconv.Itoa(len(v))
	case map[string]interface{}:
		return strconv.Itoa(len(v))
	default:
		return "1"
	}
}

// jsonType returns the type of JSON value
func jsonType(jsonStr string) string {
	jsonStr = strings.TrimSpace(jsonStr)
	if jsonStr == "" {
		return "null"
	}
	var data interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		return "string"
	}
	switch data.(type) {
	case map[string]interface{}:
		return "object"
	case []interface{}:
		return "array"
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "boolean"
	case nil:
		return "null"
	}
	return "unknown"
}

// jsonContains checks if JSON contains a value
func jsonContains(jsonStr, value string) string {
	if strings.Contains(jsonStr, value) {
		return "1"
	}
	return "0"
}

// parseDateTime parses various date/time formats
func parseDateTime(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02",
		"2006/01/02",
		"01/02/2006",
		"Jan 2, 2006",
		time.RFC3339,
	}
	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unable to parse date: %s", s)
}

// addToDate adds an interval to a date
func addToDate(t time.Time, interval int, unit string) time.Time {
	switch unit {
	case "YEAR", "YEARS":
		return t.AddDate(interval, 0, 0)
	case "MONTH", "MONTHS":
		return t.AddDate(0, interval, 0)
	case "DAY", "DAYS":
		return t.AddDate(0, 0, interval)
	case "HOUR", "HOURS":
		return t.Add(time.Duration(interval) * time.Hour)
	case "MINUTE", "MINUTES":
		return t.Add(time.Duration(interval) * time.Minute)
	case "SECOND", "SECONDS":
		return t.Add(time.Duration(interval) * time.Second)
	}
	return t
}

// formatDate formats a date using SQL-style format codes
func formatDate(t time.Time, format string) string {
	// Convert SQL format codes to Go format
	result := format
	result = strings.ReplaceAll(result, "%Y", "2006")
	result = strings.ReplaceAll(result, "%m", "01")
	result = strings.ReplaceAll(result, "%d", "02")
	result = strings.ReplaceAll(result, "%H", "15")
	result = strings.ReplaceAll(result, "%i", "04")
	result = strings.ReplaceAll(result, "%s", "05")
	result = strings.ReplaceAll(result, "%M", "January")
	result = strings.ReplaceAll(result, "%D", "2")
	result = strings.ReplaceAll(result, "%W", "Monday")
	return t.Format(result)
}

// executeJoin performs a join between two result sets
func executeJoin(leftRows, rightRows []map[string]string, join sql.JoinClause) []map[string]string {
	var results []map[string]string

	switch join.Type {
	case "INNER":
		for _, leftRow := range leftRows {
			for _, rightRow := range rightRows {
				if matchJoinCondition(leftRow, rightRow, join) {
					merged := mergeRows(leftRow, rightRow)
					results = append(results, merged)
				}
			}
		}

	case "LEFT":
		for _, leftRow := range leftRows {
			matched := false
			for _, rightRow := range rightRows {
				if matchJoinCondition(leftRow, rightRow, join) {
					merged := mergeRows(leftRow, rightRow)
					results = append(results, merged)
					matched = true
				}
			}
			if !matched {
				// Include left row with nulls for right columns
				results = append(results, leftRow)
			}
		}

	case "RIGHT":
		for _, rightRow := range rightRows {
			matched := false
			for _, leftRow := range leftRows {
				if matchJoinCondition(leftRow, rightRow, join) {
					merged := mergeRows(leftRow, rightRow)
					results = append(results, merged)
					matched = true
				}
			}
			if !matched {
				// Include right row with nulls for left columns
				results = append(results, rightRow)
			}
		}
	}

	return results
}

// matchJoinCondition checks if two rows satisfy the join ON condition
func matchJoinCondition(leftRow, rightRow map[string]string, join sql.JoinClause) bool {
	leftVal := getColumnValue(leftRow, join.LeftCol)
	rightVal := getColumnValue(rightRow, join.RightCol)
	return leftVal == rightVal
}

// getColumnValue extracts a column value, handling table.column format
func getColumnValue(row map[string]string, colName string) string {
	// Try exact match first
	if val, ok := row[colName]; ok {
		return val
	}
	// Try without table prefix
	parts := strings.Split(colName, ".")
	if len(parts) == 2 {
		if val, ok := row[parts[1]]; ok {
			return val
		}
	}
	return ""
}

// mergeRows combines two row maps into one
func mergeRows(left, right map[string]string) map[string]string {
	merged := make(map[string]string)
	for k, v := range left {
		merged[k] = v
	}
	for k, v := range right {
		merged[k] = v
	}
	return merged
}

// matchesWhereClause evaluates all conditions in the WHERE clause
func matchesWhereClause(row map[string]string, where sql.WhereClause) bool {
	if len(where.Conditions) == 0 {
		return true
	}

	// Evaluate first condition
	result := evaluateCondition(row, where.Conditions[0])

	// Apply logical operators for remaining conditions
	for i := 1; i < len(where.Conditions); i++ {
		condResult := evaluateCondition(row, where.Conditions[i])

		if i-1 < len(where.LogicalOps) {
			switch where.LogicalOps[i-1] {
			case sql.LogicalAnd:
				result = result && condResult
			case sql.LogicalOr:
				result = result || condResult
			}
		} else {
			// Default to AND if no operator specified
			result = result && condResult
		}
	}

	return result
}

// evaluateCondition evaluates a single WHERE condition
func evaluateCondition(row map[string]string, cond sql.WhereCondition) bool {
	value, exists := row[cond.Left]

	var result bool

	switch cond.Operator {
	case sql.IsNullOperator:
		result = !exists || value == ""
	case sql.IsNotNullOperator:
		result = exists && value != ""
	case sql.EqualsOperator:
		result = value == cond.Right
	case sql.NotEqualsOperator:
		result = value != cond.Right
	case sql.LessThanOperator:
		result = compareValues(value, cond.Right) < 0
	case sql.GreaterThanOperator:
		result = compareValues(value, cond.Right) > 0
	case sql.LessThanOrEqualOperator:
		result = compareValues(value, cond.Right) <= 0
	case sql.GreaterThanOrEqualOperator:
		result = compareValues(value, cond.Right) >= 0
	case sql.LikeOperator:
		result = matchLike(value, cond.Right)
	case sql.InOperator:
		result = false
		for _, v := range cond.InValues {
			if value == v {
				result = true
				break
			}
		}
	default:
		result = false
	}

	// Apply NOT negation if present
	if cond.Negated {
		result = !result
	}

	return result
}

// compareValues compares two values, trying numeric comparison first, then string
func compareValues(a, b string) int {
	// Try numeric comparison first
	aNum, aErr := strconv.ParseFloat(a, 64)
	bNum, bErr := strconv.ParseFloat(b, 64)

	if aErr == nil && bErr == nil {
		if aNum < bNum {
			return -1
		} else if aNum > bNum {
			return 1
		}
		return 0
	}

	// Fall back to string comparison
	return strings.Compare(a, b)
}

// matchLike performs simple LIKE pattern matching with % wildcards
func matchLike(value, pattern string) bool {
	// Handle simple cases
	if pattern == "%" {
		return true
	}

	// Check for patterns like %text%, %text, text%
	if strings.HasPrefix(pattern, "%") && strings.HasSuffix(pattern, "%") {
		// Contains match
		search := pattern[1 : len(pattern)-1]
		return strings.Contains(strings.ToLower(value), strings.ToLower(search))
	} else if strings.HasPrefix(pattern, "%") {
		// Ends with match
		search := pattern[1:]
		return strings.HasSuffix(strings.ToLower(value), strings.ToLower(search))
	} else if strings.HasSuffix(pattern, "%") {
		// Starts with match
		search := pattern[:len(pattern)-1]
		return strings.HasPrefix(strings.ToLower(value), strings.ToLower(search))
	}

	// Exact match (case-insensitive)
	return strings.EqualFold(value, pattern)
}

// applyDistinct removes duplicate rows based on selected columns
func applyDistinct(results []map[string]string, columns []string) []map[string]string {
	seen := make(map[string]bool)
	var distinct []map[string]string

	for _, row := range results {
		// Create a key from the selected column values
		var keyParts []string
		for _, col := range columns {
			keyParts = append(keyParts, row[col])
		}
		key := strings.Join(keyParts, "\x00")

		if !seen[key] {
			seen[key] = true
			distinct = append(distinct, row)
		}
	}

	return distinct
}

// sortResults sorts the results by ORDER BY clauses
func sortResults(results []map[string]string, orderBy []sql.OrderByClause) {
	sort.SliceStable(results, func(i, j int) bool {
		for _, clause := range orderBy {
			valI := results[i][clause.Column]
			valJ := results[j][clause.Column]

			cmp := compareValues(valI, valJ)
			if cmp != 0 {
				if clause.Descending {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
}

func (engine *Engine) executeInsertStatement(statement sql.InsertStatement) (CommitResult, error) {
	startTime := time.Now()

	tableOp, err := op.GetTable(statement.Database, statement.Table, engine.Persistence)
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

	var txn ps.Transaction
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

	tableOp, err := op.GetTable(statement.Database, statement.Table, engine.Persistence)
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
			ExecutionOps:     1, // 1 record updated
		}, nil
	} else {
		return CommitResult{}, fmt.Errorf("no WHERE clause provided in the UPDATE statement")
	}
}

func (engine *Engine) executeDeleteStatement(statement sql.DeleteStatement) (CommitResult, error) {
	startTime := time.Now()
	opCount := 1

	tableOp, err := op.GetTable(statement.Database, statement.Table, engine.Persistence)
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
			ExecutionOps:     1, // 1 record updated
		}, nil
	} else {
		return CommitResult{}, fmt.Errorf("no WHERE clause provided in the DELETE statement")
	}
}

func (engine *Engine) executeCreateTableStatement(statement sql.CreateTableStatement) (CommitResult, error) {
	startTime := time.Now()
	opCount := 1

	txn, _, err := op.CreateTable(core.Table{
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

	tableOp, err := op.GetTable(statement.Database, statement.Table, engine.Persistence)
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

	txn, _, err := op.CreateDatabase(core.Database{Name: statement.Database}, engine.Persistence, engine.Identity)
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

	databaseOp, err := op.GetDatabase(statement.Database, engine.Persistence)
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
	tableOp, err := op.GetTable(statement.Database, statement.Table, engine.Persistence)
	if err != nil {
		return CommitResult{}, fmt.Errorf("table not found: %v", err)
	}

	// Create index manager
	indexManager := ps.NewIndexManager(engine.Persistence, engine.Identity)

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
	indexManager := ps.NewIndexManager(engine.Persistence, engine.Identity)

	// Find and drop the index by looking it up
	// For now, we need to know the column from the index file
	// This is a simplified version - a full implementation would track index name -> column mapping
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

func (engine *Engine) executeBeginStatement() (CommitResult, error) {
	startTime := time.Now()

	// Create a new transaction builder
	_, err := engine.Persistence.BeginTransaction()
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    1,
	}, nil
}

func (engine *Engine) executeCommitStatement() (CommitResult, error) {
	startTime := time.Now()

	// Note: In a full implementation, we'd track the current transaction and commit it
	// For now, this is a no-op since each statement auto-commits

	return CommitResult{
		Transaction:     engine.Persistence.LatestTransaction(),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    1,
	}, nil
}

func (engine *Engine) executeRollbackStatement() (CommitResult, error) {
	startTime := time.Now()

	// Note: In a full implementation, we'd track the current transaction and rollback
	// For now, this is a no-op

	return CommitResult{
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    1,
	}, nil
}

func (engine *Engine) executeDescribeStatement(statement sql.DescribeStatement) (QueryResult, error) {
	startTime := time.Now()
	opCount := 1

	tableOp, err := op.GetTable(statement.Database, statement.Table, engine.Persistence)
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
	tableOp, err := op.GetTable(statement.Database, statement.Table, engine.Persistence)
	if err != nil {
		return QueryResult{}, err
	}

	// Load indexes
	indexManager := ps.NewIndexManager(engine.Persistence, engine.Identity)
	indexManager.LoadIndexes(statement.Database, statement.Table, tableOp.Table.Columns)

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

// Branching execution methods

func (engine *Engine) executeCreateBranchStatement(statement sql.CreateBranchStatement) (CommitResult, error) {
	startTime := time.Now()

	var from *ps.Transaction
	if statement.FromTxnId != "" {
		from = &ps.Transaction{Id: statement.FromTxnId}
	}

	err := engine.Persistence.Branch(statement.Name, from)
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		Transaction:     engine.Persistence.LatestTransaction(),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    1,
	}, nil
}

func (engine *Engine) executeCheckoutStatement(statement sql.CheckoutStatement) (CommitResult, error) {
	startTime := time.Now()

	err := engine.Persistence.Checkout(statement.Branch)
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		Transaction:     engine.Persistence.LatestTransaction(),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    1,
	}, nil
}

func (engine *Engine) executeMergeStatement(statement sql.MergeStatement) (Result, error) {
	startTime := time.Now()

	opts := ps.DefaultMergeOptions()
	if statement.ManualResolution {
		opts.Strategy = ps.MergeStrategyManual
	}

	result, err := engine.Persistence.MergeWithOptions(statement.SourceBranch, engine.Identity, opts)
	if err != nil {
		return CommitResult{}, err
	}

	// If pending (manual mode with conflicts)
	if result.Pending {
		// Return query result showing conflicts
		data := make([][]string, len(result.Unresolved))
		for i, conflict := range result.Unresolved {
			data[i] = []string{
				conflict.Database,
				conflict.Table,
				conflict.Key,
				string(conflict.HeadVal),
				string(conflict.SourceVal),
			}
		}
		return QueryResult{
			Columns:         []string{"Database", "Table", "Key", "HEAD", "SOURCE"},
			Data:            data,
			RecordsRead:     len(data),
			ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		}, nil
	}

	return CommitResult{
		Transaction:     result.Transaction,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    1,
	}, nil
}

func (engine *Engine) executeShowBranchesStatement(statement sql.ShowBranchesStatement) (QueryResult, error) {
	startTime := time.Now()

	branches, err := engine.Persistence.ListBranches()
	if err != nil {
		return QueryResult{}, err
	}

	currentBranch, _ := engine.Persistence.CurrentBranch()

	data := make([][]string, len(branches))
	for i, branch := range branches {
		isCurrent := ""
		if branch == currentBranch {
			isCurrent = "*"
		}
		data[i] = []string{branch, isCurrent}
	}

	return QueryResult{
		Transaction:     engine.Persistence.LatestTransaction(),
		Columns:         []string{"Branch", "Current"},
		Data:            data,
		RecordsRead:     len(data),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    len(data),
	}, nil
}

func (engine *Engine) executeShowMergeConflictsStatement() (QueryResult, error) {
	startTime := time.Now()

	pending := engine.Persistence.GetPendingMerge()
	if pending == nil {
		return QueryResult{
			Columns:         []string{"Database", "Table", "Key", "HEAD", "SOURCE"},
			Data:            [][]string{},
			RecordsRead:     0,
			ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		}, nil
	}

	data := make([][]string, len(pending.Unresolved))
	for i, conflict := range pending.Unresolved {
		data[i] = []string{
			conflict.Database,
			conflict.Table,
			conflict.Key,
			string(conflict.HeadVal),
			string(conflict.SourceVal),
		}
	}

	return QueryResult{
		Columns:         []string{"Database", "Table", "Key", "HEAD", "SOURCE"},
		Data:            data,
		RecordsRead:     len(data),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeResolveConflictStatement(statement sql.ResolveConflictStatement) (QueryResult, error) {
	startTime := time.Now()

	pending := engine.Persistence.GetPendingMerge()
	if pending == nil {
		return QueryResult{}, fmt.Errorf("no pending merge")
	}

	var resolution []byte
	switch statement.Resolution {
	case "HEAD":
		// Find HEAD value from pending conflicts
		for _, c := range pending.Unresolved {
			if c.Database == statement.Database && c.Table == statement.Table && c.Key == statement.Key {
				resolution = c.HeadVal
				break
			}
		}
	case "SOURCE":
		// Find SOURCE value from pending conflicts
		for _, c := range pending.Unresolved {
			if c.Database == statement.Database && c.Table == statement.Table && c.Key == statement.Key {
				resolution = c.SourceVal
				break
			}
		}
	default:
		resolution = []byte(statement.Resolution)
	}

	err := engine.Persistence.ResolveConflict(statement.Database, statement.Table, statement.Key, resolution)
	if err != nil {
		return QueryResult{}, err
	}

	// Return remaining conflicts count
	remaining := len(engine.Persistence.GetPendingMerge().Unresolved)
	return QueryResult{
		Columns:         []string{"Resolved", "Remaining"},
		Data:            [][]string{{fmt.Sprintf("%s.%s.%s", statement.Database, statement.Table, statement.Key), fmt.Sprintf("%d", remaining)}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeCommitMergeStatement() (CommitResult, error) {
	startTime := time.Now()

	txn, err := engine.Persistence.CompleteMerge(engine.Identity)
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		Transaction:     txn,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    1,
	}, nil
}

func (engine *Engine) executeAbortMergeStatement() (QueryResult, error) {
	startTime := time.Now()

	err := engine.Persistence.AbortMerge()
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:         []string{"Status"},
		Data:            [][]string{{"Merge aborted"}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeAddRemoteStatement(statement sql.AddRemoteStatement) (QueryResult, error) {
	startTime := time.Now()

	err := engine.Persistence.AddRemote(statement.Name, statement.URL)
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:         []string{"Status"},
		Data:            [][]string{{fmt.Sprintf("Remote '%s' added", statement.Name)}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeShowRemotesStatement() (QueryResult, error) {
	startTime := time.Now()

	remotes, err := engine.Persistence.ListRemotes()
	if err != nil {
		return QueryResult{}, err
	}

	data := make([][]string, len(remotes))
	for i, remote := range remotes {
		urls := strings.Join(remote.URLs, ", ")
		data[i] = []string{remote.Name, urls}
	}

	return QueryResult{
		Columns:         []string{"Name", "URLs"},
		Data:            data,
		RecordsRead:     len(remotes),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeDropRemoteStatement(statement sql.DropRemoteStatement) (QueryResult, error) {
	startTime := time.Now()

	err := engine.Persistence.RemoveRemote(statement.Name)
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:         []string{"Status"},
		Data:            [][]string{{fmt.Sprintf("Remote '%s' removed", statement.Name)}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executePushStatement(statement sql.PushStatement) (QueryResult, error) {
	startTime := time.Now()

	auth := convertAuthConfig(statement.Auth)
	err := engine.Persistence.Push(statement.Remote, statement.Branch, auth)
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:         []string{"Status"},
		Data:            [][]string{{fmt.Sprintf("Pushed to '%s'", statement.Remote)}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executePullStatement(statement sql.PullStatement) (QueryResult, error) {
	startTime := time.Now()

	auth := convertAuthConfig(statement.Auth)
	err := engine.Persistence.Pull(statement.Remote, statement.Branch, auth)
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:         []string{"Status"},
		Data:            [][]string{{fmt.Sprintf("Pulled from '%s'", statement.Remote)}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeFetchStatement(statement sql.FetchStatement) (QueryResult, error) {
	startTime := time.Now()

	auth := convertAuthConfig(statement.Auth)
	err := engine.Persistence.Fetch(statement.Remote, auth)
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:         []string{"Status"},
		Data:            [][]string{{fmt.Sprintf("Fetched from '%s'", statement.Remote)}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

// convertAuthConfig converts sql.AuthConfig to ps.RemoteAuth
func convertAuthConfig(auth *sql.AuthConfig) *ps.RemoteAuth {
	if auth == nil {
		return nil
	}

	if auth.Token != "" {
		return &ps.RemoteAuth{
			Type:  ps.AuthTypeToken,
			Token: auth.Token,
		}
	}

	if auth.SSHKeyPath != "" {
		return &ps.RemoteAuth{
			Type:       ps.AuthTypeSSH,
			KeyPath:    auth.SSHKeyPath,
			Passphrase: auth.Passphrase,
		}
	}

	if auth.Username != "" {
		return &ps.RemoteAuth{
			Type:     ps.AuthTypeBasic,
			Username: auth.Username,
			Password: auth.Password,
		}
	}

	return nil
}

// Share statement handlers

func (engine *Engine) executeCreateShareStatement(statement sql.CreateShareStatement) (QueryResult, error) {
	startTime := time.Now()

	auth := convertAuthConfig(statement.Auth)
	err := engine.Persistence.CreateShare(statement.Name, statement.URL, auth, engine.Identity)
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:         []string{"Status"},
		Data:            [][]string{{fmt.Sprintf("Share '%s' created from '%s'", statement.Name, statement.URL)}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeSyncShareStatement(statement sql.SyncShareStatement) (QueryResult, error) {
	startTime := time.Now()

	auth := convertAuthConfig(statement.Auth)
	err := engine.Persistence.SyncShare(statement.Name, auth)
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:         []string{"Status"},
		Data:            [][]string{{fmt.Sprintf("Share '%s' synced", statement.Name)}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeDropShareStatement(statement sql.DropShareStatement) (QueryResult, error) {
	startTime := time.Now()

	err := engine.Persistence.DropShare(statement.Name, engine.Identity)
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:         []string{"Status"},
		Data:            [][]string{{fmt.Sprintf("Share '%s' dropped", statement.Name)}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeShowSharesStatement() (QueryResult, error) {
	startTime := time.Now()

	shares, err := engine.Persistence.ListShares()
	if err != nil {
		return QueryResult{}, err
	}

	data := make([][]string, len(shares))
	for i, share := range shares {
		data[i] = []string{share.Name, share.URL}
	}

	return QueryResult{
		Columns:         []string{"Name", "URL"},
		Data:            data,
		RecordsRead:     len(shares),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

// executeCopyStatement handles COPY INTO for bulk CSV import/export
func (engine *Engine) executeCopyStatement(statement sql.CopyStatement) (Result, error) {
	startTime := time.Now()

	if statement.Direction == "INTO_TABLE" {
		// Import: Read CSV file into table
		return engine.executeCopyIntoTable(statement, startTime)
	} else if statement.Direction == "INTO_FILE" {
		// Export: Write table to CSV file
		return engine.executeCopyIntoFile(statement, startTime)
	}

	return nil, errors.New("invalid COPY direction")
}

// executeCopyIntoTable imports CSV data into a table
func (engine *Engine) executeCopyIntoTable(statement sql.CopyStatement, startTime time.Time) (Result, error) {
	// Build S3 config if credentials provided
	var cfg *s3Config
	if statement.S3AccessKey != "" || statement.S3SecretKey != "" || statement.S3Region != "" {
		cfg = &s3Config{
			accessKey: statement.S3AccessKey,
			secretKey: statement.S3SecretKey,
			region:    statement.S3Region,
		}
	}

	// Open file/URL using remote I/O
	reader, err := openRemoteReader(statement.FilePath, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to open source: %v", err)
	}
	defer reader.Close()

	// Create CSV reader
	csvReader := csv.NewReader(reader)
	if len(statement.Delimiter) == 1 {
		csvReader.Comma = rune(statement.Delimiter[0])
	}

	// Get table info
	tableOp, err := op.GetTable(statement.Database, statement.Table, engine.Persistence)
	if err != nil {
		return nil, err
	}

	pk, err := tableOp.PrimaryKey()
	if err != nil {
		return nil, err
	}

	// Get column names from table
	tableColumns := make([]string, len(tableOp.Table.Columns))
	for i, col := range tableOp.Table.Columns {
		tableColumns[i] = col.Name
	}

	// Determine columns from header or use table columns
	var columnNames []string
	if statement.Header {
		// Read header row first
		headerRow, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				return CommitResult{
					RecordsWritten:  0,
					ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
				}, nil
			}
			return nil, fmt.Errorf("failed to read CSV header: %v", err)
		}
		columnNames = headerRow
	} else {
		columnNames = tableColumns
	}

	// Validate column count
	if len(columnNames) != len(tableColumns) {
		return nil, fmt.Errorf("CSV columns (%d) don't match table columns (%d)", len(columnNames), len(tableColumns))
	}

	// Batch all records for a single commit
	records := make(map[string][]byte)
	rowNum := 1
	if statement.Header {
		rowNum = 2 // Account for header row in error messages
	}

	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read row %d: %v", rowNum, err)
		}

		if len(row) != len(columnNames) {
			return nil, fmt.Errorf("row %d has %d values, expected %d", rowNum, len(row), len(columnNames))
		}

		data := make(map[string]interface{})
		// Use table column names (not CSV header names) so primary key lookup works
		for j, colName := range tableColumns {
			data[colName] = row[j]
		}

		pkValue := data[*pk].(string)
		jsonData, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal row %d: %v", rowNum, err)
		}

		records[pkValue] = jsonData
		rowNum++
	}

	if len(records) == 0 {
		return CommitResult{
			RecordsWritten:  0,
			ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		}, nil
	}

	// Insert all records in a single atomic transaction
	txn, err := tableOp.PutAll(records, engine.Identity)
	if err != nil {
		return nil, fmt.Errorf("failed to insert records: %v", err)
	}

	return CommitResult{
		Transaction:     txn,
		RecordsWritten:  len(records),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

// executeCopyIntoFile exports table data to a CSV file
func (engine *Engine) executeCopyIntoFile(statement sql.CopyStatement, startTime time.Time) (Result, error) {
	// Build S3 config if credentials provided
	var cfg *s3Config
	if statement.S3AccessKey != "" || statement.S3SecretKey != "" || statement.S3Region != "" {
		cfg = &s3Config{
			accessKey: statement.S3AccessKey,
			secretKey: statement.S3SecretKey,
			region:    statement.S3Region,
		}
	}

	// Open file/URL for writing using remote I/O
	writer, err := openRemoteWriter(statement.FilePath, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to open destination: %v", err)
	}
	defer writer.Close()

	// Create CSV writer
	csvWriter := csv.NewWriter(writer)
	if len(statement.Delimiter) == 1 {
		csvWriter.Comma = rune(statement.Delimiter[0])
	}
	defer csvWriter.Flush()

	// Get table data
	tableOp, err := op.GetTable(statement.Database, statement.Table, engine.Persistence)
	if err != nil {
		return nil, err
	}

	// Get column names
	columnNames := make([]string, len(tableOp.Table.Columns))
	for i, col := range tableOp.Table.Columns {
		columnNames[i] = col.Name
	}

	// Write header if requested
	if statement.Header {
		if err := csvWriter.Write(columnNames); err != nil {
			return nil, fmt.Errorf("failed to write header: %v", err)
		}
	}

	// Scan all rows
	recordsWritten := 0
	for _, payload := range tableOp.Scan() {
		var data map[string]interface{}
		if err := json.Unmarshal(payload, &data); err != nil {
			return nil, fmt.Errorf("failed to parse row: %v", err)
		}

		// Build row in column order
		csvRow := make([]string, len(columnNames))
		for i, colName := range columnNames {
			if val, ok := data[colName]; ok {
				csvRow[i] = fmt.Sprintf("%v", val)
			}
		}

		if err := csvWriter.Write(csvRow); err != nil {
			return nil, fmt.Errorf("failed to write row: %v", err)
		}
		recordsWritten++
	}

	return CommitResult{
		RecordsWritten:  recordsWritten,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}
