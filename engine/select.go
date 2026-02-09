package engine

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nickyhof/CommitDB/v2/internal/ops"
	"github.com/nickyhof/CommitDB/v2/internal/sql"
	"github.com/nickyhof/CommitDB/v2/persistence"
)

func (engine *Engine) executeSelectStatement(statement sql.SelectStatement) (QueryResult, error) {
	startTime := time.Now()
	rowsScanned := 0

	// Determine which persistence to use - share or local
	p := engine.Persistence
	if statement.Share != "" {
		sharePersistence, err := engine.Persistence.OpenSharePersistence(statement.Share)
		if err != nil {
			return QueryResult{}, fmt.Errorf("failed to access share '%s': %w", statement.Share, err)
		}
		p = sharePersistence
	}

	// Handle time-travel queries (AS OF 'transaction')
	if statement.AsOf != "" {
		return engine.executeTimeTravelSelect(statement, p, startTime)
	}

	// Check if this is a view instead of a table
	view, err := p.GetView(statement.Database, statement.Table)
	if err == nil {
		// This is a view - redirect the query
		if view.Materialized {
			// For materialized views, read cached data
			return engine.executeMaterializedViewQuery(view, statement, startTime)
		}
		// For regular views, execute the underlying query
		// Note: This creates a new query based on the view definition
		return engine.executeViewQuery(view, statement, startTime)
	}

	tableOp, err := ops.GetTable(statement.Database, statement.Table, p)
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
		// Fast path: direct PK lookup for simple WHERE pk = 'value' queries
		pk, pkErr := tableOp.PrimaryKey()
		if pkErr == nil {
			for _, cond := range statement.Where.Conditions {
				if cond.Operator == sql.EqualsOperator && cond.Left == *pk && !cond.Negated {
					rowsScanned++
					rawData, exists := tableOp.Get(cond.Right)
					if exists {
						var jsonData map[string]string
						if err := json.Unmarshal(rawData, &jsonData); err == nil {
							// Verify all other WHERE conditions match
							if matchesWhereClause(jsonData, statement.Where) {
								results = append(results, jsonData)
							}
						}
					}
					indexUsed = true
					break
				}
			}
		}

		// Try index lookup if PK fast path wasn't used
		if !indexUsed {
			indexManager := persistence.NewIndexManager(p, engine.Identity)
			_ = indexManager.LoadIndexes(statement.Database, statement.Table, tableOp.Table.Columns)

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
	}

	// Fall back to full scan if no index was used
	if !indexUsed {
		hasWhere := len(statement.Where.Conditions) > 0
		hasJoins := len(statement.Joins) > 0

		for _, rawData := range tableOp.Scan() {
			rowsScanned++

			var jsonData map[string]string
			err := json.Unmarshal(rawData, &jsonData)
			if err != nil {
				return QueryResult{}, err
			}

			// WHERE pushdown: filter during scan for non-JOIN queries
			// (JOIN queries need post-join filtering since WHERE may reference joined columns)
			if hasWhere && !hasJoins && !matchesWhereClause(jsonData, statement.Where) {
				continue
			}

			results = append(results, jsonData)
		}
	}

	// Execute JOINs
	for _, join := range statement.Joins {
		var joinTableOp *ops.TableOp
		var err error

		// Check if this is a share table (3-level naming)
		if join.Share != "" {
			// Open share persistence
			sharePersistence, shareErr := engine.Persistence.OpenSharePersistence(join.Share)
			if shareErr != nil {
				return QueryResult{}, fmt.Errorf("failed to open share '%s' for join: %w", join.Share, shareErr)
			}
			joinTableOp, err = ops.GetTable(join.Database, join.Table, sharePersistence)
		} else {
			joinTableOp, err = ops.GetTable(join.Database, join.Table, engine.Persistence)
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

	// Apply WHERE clause filtering (only needed after joins, since non-join queries filter during scan)
	if len(statement.Joins) > 0 && len(statement.Where.Conditions) > 0 {
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
func executeAggregates(results []map[string]string, statement sql.SelectStatement, txn persistence.Transaction, startTime time.Time, opCount int) (QueryResult, error) {
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
func executeStringFunctions(results []map[string]string, statement sql.SelectStatement, txn persistence.Transaction, startTime time.Time, opCount int) (QueryResult, error) {
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
	outputColumns = append(outputColumns, statement.Columns...)

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
