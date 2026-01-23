// Package core provides core types used throughout CommitDB.
//
// The package defines fundamental types like Identity, Database, Table,
// Column, and associated type constants.
//
// # Identity
//
// Identity identifies the author of transactions (Git commit author):
//
//	identity := core.Identity{
//	    Name:  "John Doe",
//	    Email: "john@example.com",
//	}
//
// # Column Types
//
// Supported column types:
//   - StringType: Short strings (VARCHAR equivalent)
//   - TextType: Long text (TEXT equivalent)
//   - IntType: Integers
//   - FloatType: Floating point numbers
//   - BoolType: Boolean values
//   - TimestampType: Date/time values
//
// # Table Definition
//
//	table := core.Table{
//	    Database: "mydb",
//	    Name:     "users",
//	    Columns: []core.Column{
//	        {Name: "id", Type: core.IntType, PrimaryKey: true},
//	        {Name: "name", Type: core.StringType},
//	        {Name: "active", Type: core.BoolType},
//	    },
//	}
package core
