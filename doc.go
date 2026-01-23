// Package CommitDB provides a Git-backed SQL database engine.
//
// CommitDB stores data using Git as the underlying storage mechanism,
// making every transaction a Git commit. This provides built-in version
// control, history tracking, and the ability to restore to any point in time.
//
// # Quick Start
//
// Create an in-memory database:
//
//	persistence, _ := ps.NewMemoryPersistence()
//	db := CommitDB.Open(&persistence)
//	engine := db.Engine(core.Identity{Name: "App", Email: "app@example.com"})
//
//	engine.Execute("CREATE DATABASE mydb")
//	engine.Execute("CREATE TABLE mydb.users (id INT PRIMARY KEY, name STRING)")
//	engine.Execute("INSERT INTO mydb.users (id, name) VALUES (1, 'Alice')")
//
//	result, _ := engine.Execute("SELECT * FROM mydb.users")
//	result.Display()
//
// # Supported SQL
//
// CommitDB supports a subset of SQL including:
//   - CREATE/DROP DATABASE
//   - CREATE/DROP TABLE
//   - CREATE/DROP INDEX
//   - INSERT, SELECT, UPDATE, DELETE
//   - WHERE with comparison operators
//   - ORDER BY, LIMIT, OFFSET
//   - Aggregate functions: SUM, AVG, MIN, MAX, COUNT
//   - GROUP BY, HAVING
//   - JOINs: INNER, LEFT, RIGHT
//   - DISTINCT
//   - Transactions: BEGIN, COMMIT, ROLLBACK
package CommitDB
