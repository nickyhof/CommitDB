//go:build comparative

package tests

import (
	"database/sql"
	"strconv"
	"testing"

	"github.com/nickyhof/CommitDB"
	"github.com/nickyhof/CommitDB/core"
	"github.com/nickyhof/CommitDB/db"
	"github.com/nickyhof/CommitDB/ps"

	_ "github.com/duckdb/duckdb-go/v2"
)

// ============================================================================
// SETUP FUNCTIONS
// ============================================================================

// setupCommitDB creates a CommitDB instance with test data
func setupCommitDB(b *testing.B) *db.Engine {
	persistence, err := ps.NewMemoryPersistence()
	if err != nil {
		b.Fatalf("Failed to initialize persistence: %v", err)
	}
	instance := CommitDB.Open(&persistence)
	engine := instance.Engine(core.Identity{Name: "benchmark", Email: "bench@test.com"})

	engine.Execute("CREATE DATABASE bench")
	engine.Execute("CREATE TABLE bench.users (id INT PRIMARY KEY, name STRING, age INT, city STRING)")

	for i := 1; i <= 1000; i++ {
		engine.Execute("INSERT INTO bench.users (id, name, age, city) VALUES (" +
			strconv.Itoa(i) + ", 'User" + strconv.Itoa(i) + "', " + strconv.Itoa(20+i%50) + ", 'City" + strconv.Itoa(i%10) + "')")
	}

	return engine
}

// setupDuckDB creates a DuckDB instance with identical test data
func setupDuckDB(b *testing.B) *sql.DB {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		b.Fatalf("Failed to open DuckDB: %v", err)
	}

	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR, age INTEGER, city VARCHAR)")
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Insert 1000 records
	for i := 1; i <= 1000; i++ {
		_, err = db.Exec("INSERT INTO users VALUES (?, ?, ?, ?)",
			i, "User"+strconv.Itoa(i), 20+i%50, "City"+strconv.Itoa(i%10))
		if err != nil {
			b.Fatalf("Failed to insert: %v", err)
		}
	}

	return db
}

// ============================================================================
// SELECT ALL BENCHMARKS
// ============================================================================

func BenchmarkCommitDB_SelectAll(b *testing.B) {
	engine := setupCommitDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT * FROM bench.users")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

func BenchmarkDuckDB_SelectAll(b *testing.B) {
	db := setupDuckDB(b)
	defer db.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT * FROM users")
		if err != nil {
			b.Fatalf("Query error: %v", err)
		}
		// Consume all rows to match CommitDB behavior
		for rows.Next() {
			var id, age int
			var name, city string
			rows.Scan(&id, &name, &age, &city)
		}
		rows.Close()
	}
}

// ============================================================================
// SELECT WITH WHERE BENCHMARKS
// ============================================================================

func BenchmarkCommitDB_SelectWhere(b *testing.B) {
	engine := setupCommitDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT * FROM bench.users WHERE age > 40")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

func BenchmarkDuckDB_SelectWhere(b *testing.B) {
	db := setupDuckDB(b)
	defer db.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT * FROM users WHERE age > 40")
		if err != nil {
			b.Fatalf("Query error: %v", err)
		}
		for rows.Next() {
			var id, age int
			var name, city string
			rows.Scan(&id, &name, &age, &city)
		}
		rows.Close()
	}
}

// ============================================================================
// ORDER BY BENCHMARKS
// ============================================================================

func BenchmarkCommitDB_OrderBy(b *testing.B) {
	engine := setupCommitDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT * FROM bench.users ORDER BY age DESC")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

func BenchmarkDuckDB_OrderBy(b *testing.B) {
	db := setupDuckDB(b)
	defer db.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT * FROM users ORDER BY age DESC")
		if err != nil {
			b.Fatalf("Query error: %v", err)
		}
		for rows.Next() {
			var id, age int
			var name, city string
			rows.Scan(&id, &name, &age, &city)
		}
		rows.Close()
	}
}

// ============================================================================
// AGGREGATE BENCHMARKS
// ============================================================================

func BenchmarkCommitDB_Count(b *testing.B) {
	engine := setupCommitDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT COUNT(*) FROM bench.users")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

func BenchmarkDuckDB_Count(b *testing.B) {
	db := setupDuckDB(b)
	defer db.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
		if err != nil {
			b.Fatalf("Query error: %v", err)
		}
	}
}

func BenchmarkCommitDB_Sum(b *testing.B) {
	engine := setupCommitDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT SUM(age) FROM bench.users")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

func BenchmarkDuckDB_Sum(b *testing.B) {
	db := setupDuckDB(b)
	defer db.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var sum int
		err := db.QueryRow("SELECT SUM(age) FROM users").Scan(&sum)
		if err != nil {
			b.Fatalf("Query error: %v", err)
		}
	}
}

func BenchmarkCommitDB_Avg(b *testing.B) {
	engine := setupCommitDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT AVG(age) FROM bench.users")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

func BenchmarkDuckDB_Avg(b *testing.B) {
	db := setupDuckDB(b)
	defer db.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var avg float64
		err := db.QueryRow("SELECT AVG(age) FROM users").Scan(&avg)
		if err != nil {
			b.Fatalf("Query error: %v", err)
		}
	}
}

// ============================================================================
// GROUP BY BENCHMARKS
// ============================================================================

func BenchmarkCommitDB_GroupBy(b *testing.B) {
	engine := setupCommitDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT city, COUNT(*), AVG(age) FROM bench.users GROUP BY city")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

func BenchmarkDuckDB_GroupBy(b *testing.B) {
	db := setupDuckDB(b)
	defer db.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT city, COUNT(*), AVG(age) FROM users GROUP BY city")
		if err != nil {
			b.Fatalf("Query error: %v", err)
		}
		for rows.Next() {
			var city string
			var count int
			var avg float64
			rows.Scan(&city, &count, &avg)
		}
		rows.Close()
	}
}

// ============================================================================
// INSERT BENCHMARKS
// ============================================================================

func BenchmarkCommitDB_Insert(b *testing.B) {
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

func BenchmarkDuckDB_Insert(b *testing.B) {
	db, _ := sql.Open("duckdb", "")
	defer db.Close()
	db.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY, value VARCHAR)")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := db.Exec("INSERT INTO items VALUES (?, ?)", i, "value"+strconv.Itoa(i))
		if err != nil {
			b.Fatalf("Insert error: %v", err)
		}
	}
}

// ============================================================================
// LIMIT BENCHMARKS
// ============================================================================

func BenchmarkCommitDB_Limit(b *testing.B) {
	engine := setupCommitDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT * FROM bench.users LIMIT 10")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

func BenchmarkDuckDB_Limit(b *testing.B) {
	db := setupDuckDB(b)
	defer db.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT * FROM users LIMIT 10")
		if err != nil {
			b.Fatalf("Query error: %v", err)
		}
		for rows.Next() {
			var id, age int
			var name, city string
			rows.Scan(&id, &name, &age, &city)
		}
		rows.Close()
	}
}

// ============================================================================
// COMPLEX QUERY BENCHMARKS
// ============================================================================

func BenchmarkCommitDB_Complex(b *testing.B) {
	engine := setupCommitDB(b)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("SELECT * FROM bench.users WHERE age > 30 AND city = 'City5' ORDER BY age DESC LIMIT 20")
		if err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}
}

func BenchmarkDuckDB_Complex(b *testing.B) {
	db := setupDuckDB(b)
	defer db.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT * FROM users WHERE age > 30 AND city = 'City5' ORDER BY age DESC LIMIT 20")
		if err != nil {
			b.Fatalf("Query error: %v", err)
		}
		for rows.Next() {
			var id, age int
			var name, city string
			rows.Scan(&id, &name, &age, &city)
		}
		rows.Close()
	}
}
