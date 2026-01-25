package tests

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/nickyhof/CommitDB"
	"github.com/nickyhof/CommitDB/core"
	"github.com/nickyhof/CommitDB/db"
	"github.com/nickyhof/CommitDB/ps"
	"github.com/nickyhof/CommitDB/sql"
)

// setupBenchmarkDB creates a database with test data for benchmarks
func setupBenchmarkDB(b *testing.B) *db.Engine {
	persistence, err := ps.NewMemoryPersistence()
	if err != nil {
		b.Fatalf("Failed to initialize persistence: %v", err)
	}
	instance := CommitDB.Open(&persistence)
	engine := instance.Engine(core.Identity{Name: "benchmark", Email: "bench@test.com"})

	// Create database and table
	engine.Execute("CREATE DATABASE bench")
	engine.Execute("CREATE TABLE bench.users (id INT PRIMARY KEY, name STRING, age INT, city STRING)")

	// Insert 1000 records
	for i := 1; i <= 1000; i++ {
		engine.Execute("INSERT INTO bench.users (id, name, age, city) VALUES (" +
			strconv.Itoa(i) + ", 'User" + strconv.Itoa(i) + "', " + strconv.Itoa(20+i%50) + ", 'City" + strconv.Itoa(i%10) + "')")
	}

	return engine
}

// BenchmarkSQLParsing benchmarks SQL parsing performance
func BenchmarkSQLParsing(b *testing.B) {
	queries := []struct {
		name  string
		query string
	}{
		{"SimpleSelect", "SELECT * FROM bench.users"},
		{"SelectWithWhere", "SELECT * FROM bench.users WHERE age > 30"},
		{"SelectWithOrderBy", "SELECT * FROM bench.users ORDER BY age DESC"},
		{"SelectWithIn", "SELECT * FROM bench.users WHERE city IN ('City1', 'City2', 'City3')"},
		{"SelectComplex", "SELECT * FROM bench.users WHERE age > 25 AND city = 'City5' ORDER BY name ASC LIMIT 10"},
		{"Insert", "INSERT INTO bench.users (id, name, age, city) VALUES (1, 'Test', 25, 'NYC')"},
		{"Update", "UPDATE bench.users SET age = 30 WHERE id = 1"},
		{"Delete", "DELETE FROM bench.users WHERE id = 1"},
	}

	for _, q := range queries {
		b.Run(q.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				parser := sql.NewParser(q.query)
				_, err := parser.Parse()
				if err != nil {
					b.Fatalf("Parse error: %v", err)
				}
			}
		})
	}
}

// BenchmarkSelectAll benchmarks SELECT * FROM table
func BenchmarkSelectAll(b *testing.B) {
	engine := setupBenchmarkDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT * FROM bench.users")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

// BenchmarkSelectWithWhere benchmarks SELECT with WHERE clause
func BenchmarkSelectWithWhere(b *testing.B) {
	engine := setupBenchmarkDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT * FROM bench.users WHERE age > 40")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

// BenchmarkSelectWithIn benchmarks SELECT with IN clause
func BenchmarkSelectWithIn(b *testing.B) {
	engine := setupBenchmarkDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT * FROM bench.users WHERE city IN ('City1', 'City2', 'City3')")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

// BenchmarkSelectWithOrderBy benchmarks SELECT with ORDER BY
func BenchmarkSelectWithOrderBy(b *testing.B) {
	engine := setupBenchmarkDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT * FROM bench.users ORDER BY age DESC")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

// BenchmarkSelectWithLimit benchmarks SELECT with LIMIT
func BenchmarkSelectWithLimit(b *testing.B) {
	engine := setupBenchmarkDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT * FROM bench.users LIMIT 10")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

// BenchmarkCount benchmarks COUNT(*)
func BenchmarkCount(b *testing.B) {
	engine := setupBenchmarkDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT COUNT(*) FROM bench.users")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

// BenchmarkAggregates benchmarks aggregate functions
func BenchmarkAggregates(b *testing.B) {
	engine := setupBenchmarkDB(b)

	aggregates := []struct {
		name  string
		query string
	}{
		{"SUM", "SELECT SUM(age) FROM bench.users"},
		{"AVG", "SELECT AVG(age) FROM bench.users"},
		{"MIN", "SELECT MIN(age) FROM bench.users"},
		{"MAX", "SELECT MAX(age) FROM bench.users"},
	}

	for _, agg := range aggregates {
		b.Run(agg.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := engine.Execute(agg.query)
				if err != nil {
					b.Fatalf("Execute error: %v", err)
				}
			}
		})
	}
}

// BenchmarkInsert benchmarks INSERT performance
func BenchmarkInsert(b *testing.B) {
	persistence, _ := ps.NewMemoryPersistence()
	instance := CommitDB.Open(&persistence)
	engine := instance.Engine(core.Identity{Name: "benchmark", Email: "bench@test.com"})

	engine.Execute("CREATE DATABASE bench")
	engine.Execute("CREATE TABLE bench.items (id INT PRIMARY KEY, value STRING)")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("INSERT INTO bench.items (id, value) VALUES (" +
			strconv.Itoa(i) + ", 'value" + strconv.Itoa(i) + "')")
		if err != nil {
			b.Fatalf("Insert error: %v", err)
		}
	}
}

// BenchmarkUpdate benchmarks UPDATE performance
func BenchmarkUpdate(b *testing.B) {
	engine := setupBenchmarkDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := (i % 1000) + 1
		_, err := engine.Execute("UPDATE bench.users SET age = 99 WHERE id = " + strconv.Itoa(id))
		if err != nil {
			b.Fatalf("Update error: %v", err)
		}
	}
}

// BenchmarkDistinct benchmarks DISTINCT queries
func BenchmarkDistinct(b *testing.B) {
	engine := setupBenchmarkDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT DISTINCT city FROM bench.users")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

// BenchmarkComplexQuery benchmarks a complex query
func BenchmarkComplexQuery(b *testing.B) {
	engine := setupBenchmarkDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT * FROM bench.users WHERE age > 30 AND city = 'City5' ORDER BY age DESC LIMIT 20")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

// BenchmarkLexer benchmarks lexer performance
func BenchmarkLexer(b *testing.B) {
	query := "SELECT id, name, age FROM bench.users WHERE age > 25 AND city = 'NYC' ORDER BY name ASC LIMIT 100 OFFSET 10"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lexer := sql.NewLexer(query)
		for {
			token := lexer.NextToken()
			if token.Type == sql.EOF {
				break
			}
		}
	}
}

// BenchmarkBulkInsert benchmarks bulk INSERT with VALUES list
func BenchmarkBulkInsert(b *testing.B) {
	engine := setupBenchmarkDB(b)
	engine.Execute("CREATE TABLE bench.bulk_test (id INT PRIMARY KEY, name STRING, value INT)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Build a bulk insert with 100 values
		values := ""
		for j := 0; j < 100; j++ {
			if j > 0 {
				values += ", "
			}
			id := i*100 + j
			values += fmt.Sprintf("(%d, 'Name%d', %d)", id, id, id*10)
		}
		_, err := engine.Execute("INSERT INTO bench.bulk_test (id, name, value) VALUES " + values)
		if err != nil {
			b.Fatalf("Bulk insert error: %v", err)
		}
	}
}

// BenchmarkCopyIntoExport benchmarks COPY INTO for CSV export
func BenchmarkCopyIntoExport(b *testing.B) {
	engine := setupBenchmarkDB(b)
	exportPath := b.TempDir() + "/export_bench.csv"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("COPY INTO '" + exportPath + "' FROM bench.users")
		if err != nil {
			b.Fatalf("Copy export error: %v", err)
		}
	}
}

// BenchmarkCopyIntoImport benchmarks COPY INTO for CSV import
func BenchmarkCopyIntoImport(b *testing.B) {
	engine := setupBenchmarkDB(b)

	// Create export file first
	exportPath := b.TempDir() + "/import_bench.csv"
	_, err := engine.Execute("COPY INTO '" + exportPath + "' FROM bench.users")
	if err != nil {
		b.Fatalf("Setup export error: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create fresh table for each import
		tableName := fmt.Sprintf("import_test_%d", i)
		engine.Execute("CREATE TABLE bench." + tableName + " (id INT PRIMARY KEY, name STRING, age INT, city STRING)")

		_, err := engine.Execute("COPY INTO bench." + tableName + " FROM '" + exportPath + "'")
		if err != nil {
			b.Fatalf("Copy import error: %v", err)
		}
	}
}

// BenchmarkGroupBy benchmarks GROUP BY with aggregates
func BenchmarkGroupBy(b *testing.B) {
	engine := setupBenchmarkDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT COUNT(*) FROM bench.users GROUP BY city")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

// BenchmarkJoin benchmarks JOIN operations
func BenchmarkJoin(b *testing.B) {
	engine := setupBenchmarkDB(b)
	// Create second table for join
	engine.Execute("CREATE TABLE bench.orders (id INT PRIMARY KEY, user_id INT, amount INT)")
	for i := 0; i < 100; i++ {
		engine.Execute(fmt.Sprintf("INSERT INTO bench.orders (id, user_id, amount) VALUES (%d, %d, %d)", i, i%50, (i+1)*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT u.name, o.amount FROM bench.users u INNER JOIN bench.orders o ON u.id = o.user_id")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

// BenchmarkStringFunctions benchmarks string function execution
func BenchmarkStringFunctions(b *testing.B) {
	engine := setupBenchmarkDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT UPPER(name), LOWER(city), LENGTH(name) FROM bench.users")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}
