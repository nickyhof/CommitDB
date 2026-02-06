// WHERE clause evaluation, value comparison, LIKE matching, DISTINCT, and ORDER BY sorting.
package engine

import (
	"sort"
	"strings"

	"github.com/nickyhof/CommitDB/v2/internal/compare"
	"github.com/nickyhof/CommitDB/v2/internal/sql"
)

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
		result = compare.Values(value, cond.Right) < 0
	case sql.GreaterThanOperator:
		result = compare.Values(value, cond.Right) > 0
	case sql.LessThanOrEqualOperator:
		result = compare.Values(value, cond.Right) <= 0
	case sql.GreaterThanOrEqualOperator:
		result = compare.Values(value, cond.Right) >= 0
	case sql.LikeOperator:
		result = compare.MatchLike(value, cond.Right)
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

// compareValues delegates to the shared compare package
func compareValues(a, b string) int {
	return compare.Values(a, b)
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

			cmp := compare.Values(valI, valJ)
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
