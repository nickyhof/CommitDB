package db

import (
	"encoding/json"
	"errors"
	"fmt"
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
	default:
		return nil, fmt.Errorf("unsupported statement type: %v", statement.Type())
	}
}

func (engine *Engine) executeSelectStatement(statement sql.SelectStatement) (QueryResult, error) {
	startTime := time.Now()
	rowsScanned := 0

	tableOp, err := op.GetTable(statement.Database, statement.Table, engine.Persistence)
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

	// Scan all rows from the main table
	var results []map[string]string
	for _, rawData := range tableOp.Scan() {
		rowsScanned++

		var jsonData map[string]string
		err := json.Unmarshal(rawData, &jsonData)
		if err != nil {
			return QueryResult{}, err
		}

		results = append(results, jsonData)
	}

	// Execute JOINs
	for _, join := range statement.Joins {
		joinTableOp, err := op.GetTable(join.Database, join.Table, engine.Persistence)
		if err != nil {
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
			Transaction:      engine.Persistence.LatestTransaction(),
			Columns:          []string{"COUNT(*)"},
			Data:             countResult,
			RecordsRead:      len(results),
			ExecutionTimeSec: time.Since(startTime).Seconds(),
			ExecutionOps:     rowsScanned,
		}, nil
	}

	// Handle aggregate functions (SUM, AVG, MIN, MAX)
	if len(statement.Aggregates) > 0 {
		return executeAggregates(results, statement, engine.Persistence.LatestTransaction(), startTime, rowsScanned)
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
		Transaction:      engine.Persistence.LatestTransaction(),
		Columns:          columns,
		Data:             outputData,
		RecordsRead:      len(outputData),
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     rowsScanned,
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
		Transaction:      txn,
		Columns:          outputColumns,
		Data:             outputData,
		RecordsRead:      len(results),
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     opCount,
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

	if len(statement.Columns) != len(statement.Values) {
		return CommitResult{}, fmt.Errorf("statement value length does not match statement column length")
	}

	pk, err := tableOp.PrimaryKey()
	if err != nil {
		return CommitResult{}, err
	}

	data := make(map[string]interface{})

	for index, column := range statement.Columns {
		data[column] = statement.Values[index]
	}

	pkValue := data[*pk].(string)

	jsonData, err := json.Marshal(data)

	txn, err := tableOp.Put(pkValue, jsonData, engine.Identity)
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
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     1, // 1 record inserted
	}, nil
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
			ExecutionTimeSec: time.Since(startTime).Seconds(),
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
			ExecutionTimeSec: time.Since(startTime).Seconds(),
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
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     opCount,
	}, nil
}

func (engine *Engine) executeDropTableStatement(statement sql.DropTableStatement) (CommitResult, error) {
	startTime := time.Now()
	opCount := 1

	tableOp, err := op.GetTable(statement.Database, statement.Table, engine.Persistence)
	if err != nil {
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
		ExecutionTimeSec: time.Since(startTime).Seconds(),
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
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     opCount,
	}, nil
}

func (engine *Engine) executeDropDatabaseStatement(statement sql.DropDatabaseStatement) (CommitResult, error) {
	startTime := time.Now()
	opCount := 1

	databaseOp, err := op.GetDatabase(statement.Database, engine.Persistence)
	if err != nil {
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
		ExecutionTimeSec: time.Since(startTime).Seconds(),
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
		Transaction:      engine.Persistence.LatestTransaction(),
		Columns:          []string{"name"},
		Data:             data,
		RecordsRead:      len(databases),
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     len(databases),
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
		Transaction:      engine.Persistence.LatestTransaction(),
		Columns:          []string{"name"},
		Data:             data,
		RecordsRead:      len(tables),
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     len(tables),
	}, nil
}

func (engine *Engine) executeCreateIndexStatement(statement sql.CreateIndexStatement) (CommitResult, error) {
	startTime := time.Now()
	opCount := 1

	// Create index manager
	indexManager := ps.NewIndexManager(engine.Persistence)

	// Create the index
	_, err := indexManager.CreateIndex(statement.Name, statement.Database, statement.Table, statement.Column, statement.Unique)
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		Transaction:      engine.Persistence.LatestTransaction(),
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     opCount,
	}, nil
}

func (engine *Engine) executeDropIndexStatement(statement sql.DropIndexStatement) (CommitResult, error) {
	startTime := time.Now()
	opCount := 1

	// Create index manager
	indexManager := ps.NewIndexManager(engine.Persistence)

	// Find and drop the index by looking it up
	// For now, we need to know the column from the index file
	// This is a simplified version - a full implementation would track index name -> column mapping
	err := indexManager.DropIndex(statement.Database, statement.Table, statement.Name)
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		Transaction:      engine.Persistence.LatestTransaction(),
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     opCount,
	}, nil
}

func (engine *Engine) executeAlterTableStatement(statement sql.AlterTableStatement) (CommitResult, error) {
	startTime := time.Now()
	opCount := 1

	// TODO: Implement actual ALTER TABLE functionality
	// This would require modifying the table schema in storage

	return CommitResult{
		Transaction:      engine.Persistence.LatestTransaction(),
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     opCount,
	}, nil
}

func (engine *Engine) executeBeginStatement() (CommitResult, error) {
	startTime := time.Now()

	// Create a new transaction builder
	_, err := engine.Persistence.BeginTransaction()
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     1,
	}, nil
}

func (engine *Engine) executeCommitStatement() (CommitResult, error) {
	startTime := time.Now()

	// Note: In a full implementation, we'd track the current transaction and commit it
	// For now, this is a no-op since each statement auto-commits

	return CommitResult{
		Transaction:      engine.Persistence.LatestTransaction(),
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     1,
	}, nil
}

func (engine *Engine) executeRollbackStatement() (CommitResult, error) {
	startTime := time.Now()

	// Note: In a full implementation, we'd track the current transaction and rollback
	// For now, this is a no-op

	return CommitResult{
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     1,
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
		case core.TimestampType:
			typeStr = "TIMESTAMP"
		}

		pkStr := "NO"
		if col.PrimaryKey {
			pkStr = "YES"
		}

		data = append(data, []string{col.Name, typeStr, pkStr})
	}

	return QueryResult{
		Transaction:      engine.Persistence.LatestTransaction(),
		Columns:          []string{"Column", "Type", "PrimaryKey"},
		Data:             data,
		RecordsRead:      len(data),
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     opCount,
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
	indexManager := ps.NewIndexManager(engine.Persistence)
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
		Transaction:      engine.Persistence.LatestTransaction(),
		Columns:          []string{"Name", "Column", "Unique"},
		Data:             data,
		RecordsRead:      len(data),
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     len(data),
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
		Transaction:      engine.Persistence.LatestTransaction(),
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     1,
	}, nil
}

func (engine *Engine) executeCheckoutStatement(statement sql.CheckoutStatement) (CommitResult, error) {
	startTime := time.Now()

	err := engine.Persistence.Checkout(statement.Branch)
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		Transaction:      engine.Persistence.LatestTransaction(),
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     1,
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
			Columns:          []string{"Database", "Table", "Key", "HEAD", "SOURCE"},
			Data:             data,
			RecordsRead:      len(data),
			ExecutionTimeSec: time.Since(startTime).Seconds(),
		}, nil
	}

	return CommitResult{
		Transaction:      result.Transaction,
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     1,
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
		Transaction:      engine.Persistence.LatestTransaction(),
		Columns:          []string{"Branch", "Current"},
		Data:             data,
		RecordsRead:      len(data),
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     len(data),
	}, nil
}

func (engine *Engine) executeShowMergeConflictsStatement() (QueryResult, error) {
	startTime := time.Now()

	pending := engine.Persistence.GetPendingMerge()
	if pending == nil {
		return QueryResult{
			Columns:          []string{"Database", "Table", "Key", "HEAD", "SOURCE"},
			Data:             [][]string{},
			RecordsRead:      0,
			ExecutionTimeSec: time.Since(startTime).Seconds(),
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
		Columns:          []string{"Database", "Table", "Key", "HEAD", "SOURCE"},
		Data:             data,
		RecordsRead:      len(data),
		ExecutionTimeSec: time.Since(startTime).Seconds(),
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
		Columns:          []string{"Resolved", "Remaining"},
		Data:             [][]string{{fmt.Sprintf("%s.%s.%s", statement.Database, statement.Table, statement.Key), fmt.Sprintf("%d", remaining)}},
		RecordsRead:      1,
		ExecutionTimeSec: time.Since(startTime).Seconds(),
	}, nil
}

func (engine *Engine) executeCommitMergeStatement() (CommitResult, error) {
	startTime := time.Now()

	txn, err := engine.Persistence.CompleteMerge(engine.Identity)
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		Transaction:      txn,
		ExecutionTimeSec: time.Since(startTime).Seconds(),
		ExecutionOps:     1,
	}, nil
}

func (engine *Engine) executeAbortMergeStatement() (QueryResult, error) {
	startTime := time.Now()

	err := engine.Persistence.AbortMerge()
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:          []string{"Status"},
		Data:             [][]string{{"Merge aborted"}},
		RecordsRead:      1,
		ExecutionTimeSec: time.Since(startTime).Seconds(),
	}, nil
}
