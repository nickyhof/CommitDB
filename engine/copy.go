package engine

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/nickyhof/CommitDB/v2/internal/ops"
	"github.com/nickyhof/CommitDB/v2/internal/sql"
)

// executeCopyStatement handles COPY INTO for bulk CSV import/export
func (engine *Engine) executeCopyStatement(statement sql.CopyStatement) (Result, error) {
	startTime := time.Now()

	switch statement.Direction {
	case "INTO_TABLE":
		return engine.executeCopyIntoTable(statement, startTime)
	case "INTO_FILE":
		return engine.executeCopyIntoFile(statement, startTime)
	default:
		return nil, errors.New("invalid COPY direction")
	}
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
	defer func() { _ = reader.Close() }()

	// Create CSV reader
	csvReader := csv.NewReader(reader)
	if len(statement.Delimiter) == 1 {
		csvReader.Comma = rune(statement.Delimiter[0])
	}

	// Get table info
	tableOp, err := ops.GetTable(statement.Database, statement.Table, engine.Persistence)
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
	defer func() { _ = writer.Close() }()

	// Create CSV writer
	csvWriter := csv.NewWriter(writer)
	if len(statement.Delimiter) == 1 {
		csvWriter.Comma = rune(statement.Delimiter[0])
	}
	defer csvWriter.Flush()

	// Get table data
	tableOp, err := ops.GetTable(statement.Database, statement.Table, engine.Persistence)
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
