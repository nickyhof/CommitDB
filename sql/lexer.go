package sql

type Token struct {
	Type  TokenType
	Value string
}

type TokenType int

const (
	Identifier TokenType = iota
	DatabaseIdentifier
	DatabasesIdentifier
	TableIdentifier
	TablesIdentifier
	IndexIdentifier
	Show
	In
	On
	Wildcard
	String
	Int
	Float
	Bool
	PrimaryKey
	Unique
	Comma
	Quote
	ParenOpen
	ParenClose
	Equals
	NotEquals
	LessThan
	GreaterThan
	LessThanOrEqual
	GreaterThanOrEqual
	And
	Or
	Not
	Is
	Null
	Like
	True
	False
	Select
	From
	Where
	Limit
	Offset
	Order
	By
	Asc
	Desc
	Count
	Sum
	Avg
	Min
	Max
	Distinct
	Group
	Having
	Create
	Drop
	Alter
	Add
	Modify
	Column
	Insert
	Update
	Delete
	Set
	Into
	Values
	Begin
	Commit
	Rollback
	Join
	Inner
	Left
	Right
	Outer
	Describe
	As
	Branch
	Branches
	Checkout
	Merge
	Manual
	Resolution
	With
	Resolve
	Conflict
	Conflicts
	Using
	Abort
	Head
	Source
	Remote
	Remotes
	Push
	Pull
	Fetch
	TokenKeyword
	Key
	Passphrase
	Ssh
	To
	Rename
	Upper
	Lower
	Concat
	Substring
	Trim
	Length
	Replace
	LeftFunc
	RightFunc
	Now
	DateAdd
	DateSub
	DateDiff
	DateFunc
	Year
	Month
	Day
	Hour
	Minute
	Second
	DateFormat
	JsonExtract
	JsonSet
	JsonRemove
	JsonContains
	JsonKeys
	JsonLength
	JsonType
	EOF
	Unknown
)

func (token Token) String() string {
	switch token.Type {
	case Identifier:
		return "Identifier(" + token.Value + ")"
	case DatabaseIdentifier:
		return "DatabaseIdentifier"
	case DatabasesIdentifier:
		return "DatabasesIdentifier"
	case TableIdentifier:
		return "TableIdentifier"
	case TablesIdentifier:
		return "TablesIdentifier"
	case IndexIdentifier:
		return "IndexIdentifier"
	case Show:
		return "Show"
	case In:
		return "In"
	case On:
		return "On"
	case Wildcard:
		return "Wildcard"
	case String:
		return "String(" + token.Value + ")"
	case Int:
		return "Int(" + token.Value + ")"
	case Float:
		return "Float(" + token.Value + ")"
	case Bool:
		return "Bool(" + token.Value + ")"
	case PrimaryKey:
		return "PrimaryKey"
	case Quote:
		return "Quote"
	case Comma:
		return "Comma"
	case ParenOpen:
		return "ParenOpen"
	case ParenClose:
		return "ParenClose"
	case Equals:
		return "Equals"
	case NotEquals:
		return "NotEquals"
	case LessThan:
		return "LessThan"
	case GreaterThan:
		return "GreaterThan"
	case LessThanOrEqual:
		return "LessThanOrEqual"
	case GreaterThanOrEqual:
		return "GreaterThanOrEqual"
	case And:
		return "And"
	case Or:
		return "Or"
	case Not:
		return "Not"
	case Is:
		return "Is"
	case Null:
		return "Null"
	case Like:
		return "Like"
	case True:
		return "True"
	case False:
		return "False"
	case Select:
		return "Select"
	case From:
		return "From"
	case Where:
		return "Where"
	case Limit:
		return "Limit"
	case Offset:
		return "Offset"
	case Order:
		return "Order"
	case By:
		return "By"
	case Asc:
		return "Asc"
	case Desc:
		return "Desc"
	case Count:
		return "Count"
	case Distinct:
		return "Distinct"
	case Create:
		return "Create"
	case Drop:
		return "Drop"
	case Insert:
		return "Insert"
	case Update:
		return "Update"
	case Delete:
		return "Delete"
	case Set:
		return "Set"
	case Into:
		return "Into"
	case Values:
		return "Values"
	case EOF:
		return "EOF"
	default:
		return "Unknown(" + token.Value + ")"
	}
}

type Lexer struct {
	sql          string
	position     int
	readPosition int
	ch           byte
}

func NewLexer(sql string) *Lexer {
	lexer := &Lexer{sql: sql}
	lexer.readChar()
	return lexer
}

func (lexer *Lexer) readChar() {
	if lexer.readPosition >= len(lexer.sql) {
		lexer.ch = 0
	} else {
		lexer.ch = lexer.sql[lexer.readPosition]
	}
	lexer.position = lexer.readPosition
	lexer.readPosition++
}

func (lexer *Lexer) NextToken() Token {
	var token Token

	lexer.skipWhitespace()

	switch lexer.ch {
	case ',':
		token = Token{Type: Comma, Value: string(lexer.ch)}
	case '(':
		token = Token{Type: ParenOpen, Value: string(lexer.ch)}
	case ')':
		token = Token{Type: ParenClose, Value: string(lexer.ch)}
	case 0:
		token = Token{Type: EOF, Value: ""}
	case '\'':
		token = Token{Type: String, Value: lexer.readString()}
	case '*':
		token = Token{Type: Wildcard, Value: string(lexer.ch)}
	default:
		if isOperator(lexer.ch) {
			operator := lexer.readOperator()
			switch operator {
			case "=":
				return Token{Type: Equals, Value: operator}
			case "!=", "<>":
				return Token{Type: NotEquals, Value: operator}
			case "<":
				return Token{Type: LessThan, Value: operator}
			case ">":
				return Token{Type: GreaterThan, Value: operator}
			case "<=":
				return Token{Type: LessThanOrEqual, Value: operator}
			case ">=":
				return Token{Type: GreaterThanOrEqual, Value: operator}
			default:
				return Token{Type: Unknown, Value: operator}
			}
		} else if isDigit(lexer.ch) {
			num := lexer.readNumber()
			// Check if it's a float
			if lexer.ch == '.' {
				lexer.readChar() // consume '.'
				decimal := lexer.readNumber()
				return Token{Type: Float, Value: num + "." + decimal}
			}
			return Token{Type: Int, Value: num}
		} else if isAlphaNumeric(lexer.ch) {
			literal := lexer.readIdentifier()
			if literal == "PRIMARY" {
				// Check for KEY
				lexer.skipWhitespace()
				nextLiteral := lexer.readIdentifier()
				if nextLiteral == "KEY" {
					return Token{Type: PrimaryKey, Value: "PRIMARY KEY"}
				} else {
					return Token{Type: Unknown, Value: literal + " " + nextLiteral}
				}
			} else {
				tokenType := lookupIdentifier(literal)
				return Token{Type: tokenType, Value: literal}
			}
		} else {
			token = Token{Type: Unknown, Value: string(lexer.ch)}
		}
	}

	lexer.readChar()
	return token
}

func (lexer *Lexer) PeekToken() Token {
	// Save current state
	savedPosition := lexer.position
	savedReadPosition := lexer.readPosition
	savedCh := lexer.ch

	// Get next token
	token := lexer.NextToken()

	// Restore state
	lexer.position = savedPosition
	lexer.readPosition = savedReadPosition
	lexer.ch = savedCh

	return token
}

func (lexer *Lexer) skipWhitespace() {
	for lexer.ch == ' ' || lexer.ch == '\t' || lexer.ch == '\n' || lexer.ch == '\r' {
		lexer.readChar()
	}
}

func (lexer *Lexer) readIdentifier() string {
	position := lexer.position
	for isAlphaNumeric(lexer.ch) {
		lexer.readChar()
	}
	return lexer.sql[position:lexer.position]
}

func (lexer *Lexer) readString() string {
	lexer.readChar() // skip opening quote
	position := lexer.position
	for lexer.ch != '\'' && lexer.ch != 0 {
		lexer.readChar()
	}
	str := lexer.sql[position:lexer.position]
	return str
}

func (lexer *Lexer) readNumber() string {
	position := lexer.position
	for isDigit(lexer.ch) {
		lexer.readChar()
	}
	return lexer.sql[position:lexer.position]
}

func (lexer *Lexer) readOperator() string {
	position := lexer.position
	for isOperator(lexer.ch) {
		lexer.readChar()
	}
	return lexer.sql[position:lexer.position]
}

func isAlphaNumeric(ch byte) bool {
	return ('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z') || ch == '_' || ch == '.' || isDigit(ch)
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func isOperator(ch byte) bool {
	return ch == '=' || ch == '!' || ch == '<' || ch == '>'
}

func lookupIdentifier(id string) TokenType {
	// Convert to uppercase for case-insensitive matching
	upperID := toUpper(id)
	switch upperID {
	case "DATABASE":
		return DatabaseIdentifier
	case "DATABASES":
		return DatabasesIdentifier
	case "TABLE":
		return TableIdentifier
	case "TABLES":
		return TablesIdentifier
	case "INDEX":
		return IndexIdentifier
	case "SHOW":
		return Show
	case "IN":
		return In
	case "ON":
		return On
	case "UNIQUE":
		return Unique
	case "AND":
		return And
	case "OR":
		return Or
	case "NOT":
		return Not
	case "IS":
		return Is
	case "NULL":
		return Null
	case "LIKE":
		return Like
	case "TRUE":
		return True
	case "FALSE":
		return False
	case "SELECT":
		return Select
	case "FROM":
		return From
	case "WHERE":
		return Where
	case "LIMIT":
		return Limit
	case "OFFSET":
		return Offset
	case "ORDER":
		return Order
	case "BY":
		return By
	case "ASC":
		return Asc
	case "DESC":
		return Desc
	case "COUNT":
		return Count
	case "SUM":
		return Sum
	case "AVG":
		return Avg
	case "MIN":
		return Min
	case "MAX":
		return Max
	case "DISTINCT":
		return Distinct
	case "GROUP":
		return Group
	case "HAVING":
		return Having
	case "CREATE":
		return Create
	case "DROP":
		return Drop
	case "ALTER":
		return Alter
	case "ADD":
		return Add
	case "MODIFY":
		return Modify
	case "COLUMN":
		return Column
	case "INSERT":
		return Insert
	case "UPDATE":
		return Update
	case "DELETE":
		return Delete
	case "SET":
		return Set
	case "INTO":
		return Into
	case "VALUES":
		return Values
	case "BEGIN":
		return Begin
	case "COMMIT":
		return Commit
	case "ROLLBACK":
		return Rollback
	case "JOIN":
		return Join
	case "INNER":
		return Inner
	case "LEFT":
		return Left
	case "RIGHT":
		return Right
	case "OUTER":
		return Outer
	case "DESCRIBE":
		return Describe
	case "AS":
		return As
	case "BRANCH":
		return Branch
	case "BRANCHES":
		return Branches
	case "CHECKOUT":
		return Checkout
	case "MERGE":
		return Merge
	case "MANUAL":
		return Manual
	case "RESOLUTION":
		return Resolution
	case "WITH":
		return With
	case "RESOLVE":
		return Resolve
	case "CONFLICT":
		return Conflict
	case "CONFLICTS":
		return Conflicts
	case "USING":
		return Using
	case "ABORT":
		return Abort
	case "HEAD":
		return Head
	case "SOURCE":
		return Source
	case "REMOTE":
		return Remote
	case "REMOTES":
		return Remotes
	case "PUSH":
		return Push
	case "PULL":
		return Pull
	case "FETCH":
		return Fetch
	case "TOKEN":
		return TokenKeyword
	case "KEY":
		return Key
	case "PASSPHRASE":
		return Passphrase
	case "SSH":
		return Ssh
	case "TO":
		return To
	case "RENAME":
		return Rename
	case "UPPER":
		return Upper
	case "LOWER":
		return Lower
	case "CONCAT":
		return Concat
	case "SUBSTRING", "SUBSTR":
		return Substring
	case "TRIM":
		return Trim
	case "LENGTH", "LEN":
		return Length
	case "REPLACE":
		return Replace
	case "NOW":
		return Now
	case "DATE_ADD", "DATEADD":
		return DateAdd
	case "DATE_SUB", "DATESUB":
		return DateSub
	case "DATEDIFF", "DATE_DIFF":
		return DateDiff
	case "DATE":
		return DateFunc
	case "YEAR":
		return Year
	case "MONTH":
		return Month
	case "DAY":
		return Day
	case "HOUR":
		return Hour
	case "MINUTE":
		return Minute
	case "SECOND":
		return Second
	case "DATE_FORMAT", "DATEFORMAT":
		return DateFormat
	case "JSON_EXTRACT":
		return JsonExtract
	case "JSON_SET":
		return JsonSet
	case "JSON_REMOVE":
		return JsonRemove
	case "JSON_CONTAINS":
		return JsonContains
	case "JSON_KEYS":
		return JsonKeys
	case "JSON_LENGTH":
		return JsonLength
	case "JSON_TYPE":
		return JsonType
	default:
		return Identifier
	}
}

// toUpper converts a string to uppercase without allocating for ASCII strings
func toUpper(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= 'a' && s[i] <= 'z' {
			// Need to convert, allocate a new string
			b := make([]byte, len(s))
			for j := 0; j < len(s); j++ {
				if s[j] >= 'a' && s[j] <= 'z' {
					b[j] = s[j] - 32
				} else {
					b[j] = s[j]
				}
			}
			return string(b)
		}
	}
	return s
}

func tokenize(sql string) []Token {
	lexer := NewLexer(sql)

	var tokens []Token

	for {
		token := lexer.NextToken()
		if token.Type == EOF {
			return append(tokens, token)
		}
		tokens = append(tokens, token)
	}
}
