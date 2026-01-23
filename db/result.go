package db

import (
	"fmt"
	"os"
	"strings"

	"github.com/nickyhof/CommitDB/ps"
)

type ResultType int

const (
	QueryResultType ResultType = iota
	CommitResultType
)

type Result interface {
	Type() ResultType
	Display()
}

type QueryResult struct {
	Transaction      ps.Transaction
	Columns          []string
	Data             [][]string
	RecordsRead      int
	ExecutionTimeSec float64
	ExecutionOps     int
}

type CommitResult struct {
	Transaction      ps.Transaction
	DatabasesCreated int
	DatabasesDeleted int
	TablesCreated    int
	TablesDeleted    int
	RecordsWritten   int
	RecordsDeleted   int
	ExecutionTimeSec float64
	ExecutionOps     int
}

func (result QueryResult) Type() ResultType {
	return QueryResultType
}

func (result CommitResult) Type() ResultType {
	return CommitResultType
}

// formatDuration formats a duration in human-readable form
func formatDuration(secs float64) string {
	if secs < 0.001 {
		return "<1ms"
	} else if secs < 0.01 {
		return fmt.Sprintf("%dms", int(secs*1000))
	} else if secs < 1 {
		ms := secs * 1000
		if ms < 10 {
			return fmt.Sprintf("%.1fms", ms)
		}
		return fmt.Sprintf("%dms", int(ms))
	} else if secs < 60 {
		if secs < 10 {
			return fmt.Sprintf("%.1fs", secs)
		}
		return fmt.Sprintf("%ds", int(secs))
	} else {
		mins := int(secs / 60)
		remainSecs := int(secs) % 60
		if remainSecs == 0 {
			return fmt.Sprintf("%dm", mins)
		}
		return fmt.Sprintf("%dm%ds", mins, remainSecs)
	}
}

func (result QueryResult) ExecutionTime() string {
	return formatDuration(result.ExecutionTimeSec)
}

func (result CommitResult) ExecutionTime() string {
	return formatDuration(result.ExecutionTimeSec)
}

func (result QueryResult) Display() {
	// Show data table first if there is data
	if len(result.Data) > 0 {
		data := NewTable(os.Stdout)
		data.Header(result.Columns)
		data.Bulk(result.Data)
		data.Render()
	}

	// Calculate throughput
	var throughputStr string
	if result.ExecutionTimeSec > 0 && result.ExecutionOps > 0 {
		ops := float64(result.ExecutionOps) / result.ExecutionTimeSec
		if ops >= 1000000 {
			throughputStr = fmt.Sprintf(", %.1fM ops/s", ops/1000000)
		} else if ops >= 1000 {
			throughputStr = fmt.Sprintf(", %.1fK ops/s", ops/1000)
		} else {
			throughputStr = fmt.Sprintf(", %.0f ops/s", ops)
		}
	}

	// Show compact stats line after data
	fmt.Printf("%d rows (%s%s)\n", result.RecordsRead, result.ExecutionTime(), throughputStr)
}

func (result CommitResult) Display() {
	var parts []string

	if result.DatabasesCreated > 0 {
		parts = append(parts, fmt.Sprintf("%d database(s) created", result.DatabasesCreated))
	}
	if result.DatabasesDeleted > 0 {
		parts = append(parts, fmt.Sprintf("%d database(s) deleted", result.DatabasesDeleted))
	}
	if result.TablesCreated > 0 {
		parts = append(parts, fmt.Sprintf("%d table(s) created", result.TablesCreated))
	}
	if result.TablesDeleted > 0 {
		parts = append(parts, fmt.Sprintf("%d table(s) deleted", result.TablesDeleted))
	}
	if result.RecordsWritten > 0 {
		parts = append(parts, fmt.Sprintf("%d record(s) written", result.RecordsWritten))
	}
	if result.RecordsDeleted > 0 {
		parts = append(parts, fmt.Sprintf("%d record(s) deleted", result.RecordsDeleted))
	}

	// Calculate throughput
	var throughputStr string
	if result.ExecutionTimeSec > 0 && result.ExecutionOps > 0 {
		ops := float64(result.ExecutionOps) / result.ExecutionTimeSec
		if ops >= 1000000 {
			throughputStr = fmt.Sprintf(", %.1fM ops/s", ops/1000000)
		} else if ops >= 1000 {
			throughputStr = fmt.Sprintf(", %.1fK ops/s", ops/1000)
		} else {
			throughputStr = fmt.Sprintf(", %.0f ops/s", ops)
		}
	}

	if len(parts) == 0 {
		fmt.Printf("OK (%s%s)\n", result.ExecutionTime(), throughputStr)
	} else {
		fmt.Printf("%s (%s%s)\n", strings.Join(parts, ", "), result.ExecutionTime(), throughputStr)
	}
}
