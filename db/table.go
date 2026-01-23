package db

import (
	"fmt"
	"io"
	"strings"
)

// SimpleTable provides basic table formatting without external dependencies
type SimpleTable struct {
	writer  io.Writer
	headers []string
	rows    [][]string
}

// NewTable creates a new table writer
func NewTable(w io.Writer) *SimpleTable {
	return &SimpleTable{
		writer: w,
		rows:   make([][]string, 0),
	}
}

// Header sets the table headers
func (t *SimpleTable) Header(headers []string) {
	t.headers = headers
}

// Row adds a single row
func (t *SimpleTable) Row(row []string) {
	t.rows = append(t.rows, row)
}

// Bulk adds multiple rows
func (t *SimpleTable) Bulk(rows [][]string) {
	t.rows = append(t.rows, rows...)
}

// Render outputs the formatted table
func (t *SimpleTable) Render() {
	if len(t.headers) == 0 && len(t.rows) == 0 {
		return
	}

	// Calculate column widths
	colWidths := t.calculateWidths()

	// Build separator line
	separator := t.buildSeparator(colWidths)

	// Print table
	fmt.Fprintln(t.writer, separator)

	// Print headers
	if len(t.headers) > 0 {
		fmt.Fprintln(t.writer, t.formatRow(t.headers, colWidths))
		fmt.Fprintln(t.writer, separator)
	}

	// Print rows
	for _, row := range t.rows {
		fmt.Fprintln(t.writer, t.formatRow(row, colWidths))
	}

	fmt.Fprintln(t.writer, separator)
}

// calculateWidths determines the width needed for each column
func (t *SimpleTable) calculateWidths() []int {
	// Determine number of columns
	numCols := len(t.headers)
	for _, row := range t.rows {
		if len(row) > numCols {
			numCols = len(row)
		}
	}

	widths := make([]int, numCols)

	// Check header widths
	for i, h := range t.headers {
		if len(h) > widths[i] {
			widths[i] = len(h)
		}
	}

	// Check row widths
	for _, row := range t.rows {
		for i, cell := range row {
			if i < numCols && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	// Minimum width of 1
	for i := range widths {
		if widths[i] < 1 {
			widths[i] = 1
		}
	}

	return widths
}

// buildSeparator creates the horizontal line
func (t *SimpleTable) buildSeparator(widths []int) string {
	parts := make([]string, len(widths))
	for i, w := range widths {
		parts[i] = strings.Repeat("-", w+2)
	}
	return "+" + strings.Join(parts, "+") + "+"
}

// formatRow formats a single row with proper padding
func (t *SimpleTable) formatRow(row []string, widths []int) string {
	parts := make([]string, len(widths))
	for i, w := range widths {
		cell := ""
		if i < len(row) {
			cell = row[i]
		}
		// Left-align with padding
		parts[i] = " " + cell + strings.Repeat(" ", w-len(cell)+1)
	}
	return "|" + strings.Join(parts, "|") + "|"
}
