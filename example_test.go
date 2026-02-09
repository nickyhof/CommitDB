package commitdb_test

import (
	"fmt"

	commitdb "github.com/nickyhof/CommitDB/v2"
	"github.com/nickyhof/CommitDB/v2/core"
	"github.com/nickyhof/CommitDB/v2/engine"
	"github.com/nickyhof/CommitDB/v2/persistence"
)

func Example() {
	// Create an in-memory database
	p, _ := persistence.NewMemoryPersistence()
	instance := commitdb.Open(&p)

	// Create an engine with identity (used for Git commit author)
	e := instance.Engine(core.Identity{
		Name:  "Example",
		Email: "example@commitdb.local",
	})

	// Create a database and table
	e.Execute("CREATE DATABASE myapp")
	e.Execute("CREATE TABLE myapp.users (id INT PRIMARY KEY, name STRING)")

	// Insert data
	e.Execute("INSERT INTO myapp.users (id, name) VALUES (1, 'Alice')")
	e.Execute("INSERT INTO myapp.users (id, name) VALUES (2, 'Bob')")

	// Query data
	result, _ := e.Execute("SELECT name FROM myapp.users ORDER BY id")

	// Access result data
	qr := result.(engine.QueryResult)
	for _, row := range qr.Data {
		fmt.Println(row[0])
	}
	// Output:
	// Alice
	// Bob
}

func ExampleInstance_Engine() {
	p, _ := persistence.NewMemoryPersistence()
	instance := commitdb.Open(&p)

	// Each engine has its own identity for Git commits
	alice := instance.Engine(core.Identity{Name: "Alice", Email: "alice@example.com"})
	bob := instance.Engine(core.Identity{Name: "Bob", Email: "bob@example.com"})

	alice.Execute("CREATE DATABASE demo")
	alice.Execute("CREATE TABLE demo.messages (id INT PRIMARY KEY, text STRING)")
	alice.Execute("INSERT INTO demo.messages (id, text) VALUES (1, 'Hello from Alice')")
	bob.Execute("INSERT INTO demo.messages (id, text) VALUES (2, 'Hello from Bob')")

	result, _ := alice.Execute("SELECT text FROM demo.messages ORDER BY id")
	qr := result.(engine.QueryResult)
	for _, row := range qr.Data {
		fmt.Println(row[0])
	}
	// Output:
	// Hello from Alice
	// Hello from Bob
}
