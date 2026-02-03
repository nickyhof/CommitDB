// Package sql provides SQL lexing and parsing for CommitDB.
//
// The package includes a lexer that tokenizes SQL strings and a parser
// that produces abstract syntax trees for SQL statements.
//
// # Lexer Usage
//
//	lexer := sql.NewLexer("SELECT * FROM users")
//	for {
//	    token := lexer.NextToken()
//	    if token.Type == sql.EOF {
//	        break
//	    }
//	    fmt.Printf("Token: %s = %s\n", token.Type, token.Value)
//	}
//
// # Parser Usage
//
//	parser := sql.NewParser("SELECT * FROM mydb.users WHERE id = 1")
//	statement, err := parser.Parse()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// # Supported Statements
//
// The parser supports the following statement types:
//   - SelectStatement
//   - InsertStatement
//   - UpdateStatement
//   - DeleteStatement
//   - CreateTableStatement
//   - DropTableStatement
//   - CreateDatabaseStatement
//   - DropDatabaseStatement
//   - CreateIndexStatement
//   - DropIndexStatement
//   - AlterTableStatement
//   - BeginStatement, CommitStatement, RollbackStatement
//   - DescribeStatement
//   - ShowDatabasesStatement, ShowTablesStatement, ShowIndexesStatement
package sql
