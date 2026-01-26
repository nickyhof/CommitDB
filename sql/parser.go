package sql

import (
	"errors"
	"strconv"
	"strings"

	"github.com/nickyhof/CommitDB/core"
)

type StatementType int

const (
	SelectStatementType StatementType = iota
	InsertStatementType
	UpdateStatementType
	DeleteStatementType
	CreateTableStatementType
	DropTableStatementType
	CreateDatabaseStatementType
	DropDatabaseStatementType
	CreateIndexStatementType
	DropIndexStatementType
	AlterTableStatementType
	BeginStatementType
	CommitStatementType
	RollbackStatementType
	DescribeStatementType
	ShowDatabasesStatementType
	ShowTablesStatementType
	ShowIndexesStatementType
	CreateBranchStatementType
	CheckoutStatementType
	MergeStatementType
	ShowBranchesStatementType
	ShowMergeConflictsStatementType
	ResolveConflictStatementType
	CommitMergeStatementType
	AbortMergeStatementType
	AddRemoteStatementType
	ShowRemotesStatementType
	DropRemoteStatementType
	PushStatementType
	PullStatementType
	FetchStatementType
	CopyStatementType
)

type Statement interface {
	Type() StatementType
}

type SelectStatement struct {
	Database   string
	Table      string
	TableAlias string
	Columns    []string
	Aggregates []AggregateExpr
	Functions  []FunctionExpr // String functions like UPPER, LOWER, etc.
	Joins      []JoinClause
	Distinct   bool
	CountAll   bool
	Where      WhereClause
	GroupBy    []string
	Having     WhereClause
	OrderBy    []OrderByClause
	Limit      int
	Offset     int
}

type JoinClause struct {
	Type       string // INNER, LEFT, RIGHT
	Database   string
	Table      string
	TableAlias string
	LeftCol    string // left table column
	RightCol   string // right table column
}

type AggregateExpr struct {
	Function string // COUNT, SUM, AVG, MIN, MAX
	Column   string
	Alias    string
}

// FunctionExpr represents a function call like UPPER(column), CONCAT(a, b)
type FunctionExpr struct {
	Function string   // UPPER, LOWER, CONCAT, SUBSTRING, TRIM, LENGTH, REPLACE
	Args     []string // Arguments (column names or literals)
	Alias    string   // Optional AS alias
}

type InsertStatement struct {
	Database  string
	Table     string
	Columns   []string
	ValueRows [][]string // Multiple rows for bulk insert: VALUES (v1), (v2), ...
}

type UpdateStatement struct {
	Database string
	Table    string
	Updates  []SetClause
	Where    WhereClause
}

type SetClause struct {
	Column string
	Value  string
}

type DeleteStatement struct {
	Database string
	Table    string
	Where    WhereClause
}

type CreateTableStatement struct {
	Database string
	Table    string
	Columns  []core.Column
}

type DropTableStatement struct {
	Database string
	Table    string
}

type ShowDatabasesStatement struct {
}

type ShowTablesStatement struct {
	Database string
}

type WhereClause struct {
	Conditions []WhereCondition
	LogicalOps []LogicalOperator // AND/OR between conditions
}

type LogicalOperator int

const (
	LogicalAnd LogicalOperator = iota
	LogicalOr
)

type WhereCondition struct {
	Left     string
	Operator WhereOperator
	Right    string
	InValues []string // for IN operator
	Negated  bool     // for NOT
}

type WhereOperator int

const (
	EqualsOperator WhereOperator = iota
	NotEqualsOperator
	LessThanOperator
	GreaterThanOperator
	LessThanOrEqualOperator
	GreaterThanOrEqualOperator
	LikeOperator
	IsNullOperator
	IsNotNullOperator
	InOperator
)

type OrderByClause struct {
	Column     string
	Descending bool
}

type CreateDatabaseStatement struct {
	Database string
}

type DropDatabaseStatement struct {
	Database string
}

type CreateIndexStatement struct {
	Name     string
	Database string
	Table    string
	Column   string
	Unique   bool
}

type DropIndexStatement struct {
	Name     string
	Database string
	Table    string
}

type AlterTableStatement struct {
	Database      string
	Table         string
	Action        string // ADD, DROP, MODIFY, RENAME
	ColumnName    string
	NewColumnName string // for RENAME
	ColumnType    string
}

type BeginStatement struct{}
type CommitStatement struct{}
type RollbackStatement struct{}

type DescribeStatement struct {
	Database string
	Table    string
}

type ShowIndexesStatement struct {
	Database string
	Table    string
}

// CopyStatement for bulk data import/export
type CopyStatement struct {
	Direction string // "INTO_TABLE" or "INTO_FILE"
	Database  string
	Table     string
	FilePath  string
	Header    bool   // Include/expect header row
	Delimiter string // Column delimiter (default ",")
	// S3 configuration (optional)
	S3AccessKey string
	S3SecretKey string
	S3Region    string
}

// Branch statements
type CreateBranchStatement struct {
	Name      string
	FromTxnId string // Optional: create from specific transaction
}

type CheckoutStatement struct {
	Branch string
}

type MergeStatement struct {
	SourceBranch     string
	ManualResolution bool
}

type ShowBranchesStatement struct{}

type ShowMergeConflictsStatement struct{}

type ResolveConflictStatement struct {
	Database   string
	Table      string
	Key        string
	Resolution string // "HEAD", "SOURCE", or a literal value
}

type CommitMergeStatement struct{}

type AbortMergeStatement struct{}

// AuthConfig represents authentication configuration for remote operations
type AuthConfig struct {
	Token      string // Token-based authentication
	SSHKeyPath string // Path to SSH private key
	Passphrase string // Passphrase for SSH key
	Username   string // Username for basic auth
	Password   string // Password for basic auth
}

type AddRemoteStatement struct {
	Name string
	URL  string
}

type ShowRemotesStatement struct{}

type DropRemoteStatement struct {
	Name string
}

type PushStatement struct {
	Remote string
	Branch string
	Auth   *AuthConfig
}

type PullStatement struct {
	Remote string
	Branch string
	Auth   *AuthConfig
}

type FetchStatement struct {
	Remote string
	Auth   *AuthConfig
}

func (s SelectStatement) Type() StatementType {
	return SelectStatementType
}

func (s InsertStatement) Type() StatementType {
	return InsertStatementType
}

func (s UpdateStatement) Type() StatementType {
	return UpdateStatementType
}

func (s DeleteStatement) Type() StatementType {
	return DeleteStatementType
}

func (s CreateTableStatement) Type() StatementType {
	return CreateTableStatementType
}

func (s DropTableStatement) Type() StatementType {
	return DropTableStatementType
}

func (s CreateDatabaseStatement) Type() StatementType {
	return CreateDatabaseStatementType
}

func (s DropDatabaseStatement) Type() StatementType {
	return DropDatabaseStatementType
}

func (s CreateIndexStatement) Type() StatementType {
	return CreateIndexStatementType
}

func (s DropIndexStatement) Type() StatementType {
	return DropIndexStatementType
}

func (s AlterTableStatement) Type() StatementType {
	return AlterTableStatementType
}

func (s BeginStatement) Type() StatementType {
	return BeginStatementType
}

func (s CommitStatement) Type() StatementType {
	return CommitStatementType
}

func (s RollbackStatement) Type() StatementType {
	return RollbackStatementType
}

func (s DescribeStatement) Type() StatementType {
	return DescribeStatementType
}

func (s ShowIndexesStatement) Type() StatementType {
	return ShowIndexesStatementType
}

func (s ShowDatabasesStatement) Type() StatementType {
	return ShowDatabasesStatementType
}

func (s ShowTablesStatement) Type() StatementType {
	return ShowTablesStatementType
}

func (s CreateBranchStatement) Type() StatementType {
	return CreateBranchStatementType
}

func (s CheckoutStatement) Type() StatementType {
	return CheckoutStatementType
}

func (s MergeStatement) Type() StatementType {
	return MergeStatementType
}

func (s ShowBranchesStatement) Type() StatementType {
	return ShowBranchesStatementType
}

func (s ShowMergeConflictsStatement) Type() StatementType {
	return ShowMergeConflictsStatementType
}

func (s ResolveConflictStatement) Type() StatementType {
	return ResolveConflictStatementType
}

func (s CommitMergeStatement) Type() StatementType {
	return CommitMergeStatementType
}

func (s AbortMergeStatement) Type() StatementType {
	return AbortMergeStatementType
}

func (s AddRemoteStatement) Type() StatementType {
	return AddRemoteStatementType
}

func (s ShowRemotesStatement) Type() StatementType {
	return ShowRemotesStatementType
}

func (s DropRemoteStatement) Type() StatementType {
	return DropRemoteStatementType
}

func (s PushStatement) Type() StatementType {
	return PushStatementType
}

func (s PullStatement) Type() StatementType {
	return PullStatementType
}

func (s FetchStatement) Type() StatementType {
	return FetchStatementType
}

func (s CopyStatement) Type() StatementType {
	return CopyStatementType
}

type Parser struct {
	lexer *Lexer
}

func NewParser(sql string) *Parser {
	lexer := NewLexer(sql)
	return &Parser{lexer: lexer}
}

func (parser *Parser) Parse() (Statement, error) {
	token := parser.lexer.NextToken()
	switch token.Type {
	case Select:
		return ParseSelect(parser)
	case Insert:
		return ParseInsert(parser)
	case Update:
		return ParseUpdate(parser)
	case Delete:
		return ParseDelete(parser)
	case Create:
		return ParseCreate(parser)
	case Drop:
		return ParseDrop(parser)
	case Alter:
		return ParseAlter(parser)
	case Begin:
		return BeginStatement{}, nil
	case Commit:
		// Could be regular COMMIT or COMMIT MERGE
		nextToken := parser.lexer.PeekToken()
		if nextToken.Type == Merge {
			parser.lexer.NextToken() // consume MERGE
			return CommitMergeStatement{}, nil
		}
		return CommitStatement{}, nil
	case Rollback:
		return RollbackStatement{}, nil
	case Describe:
		return ParseDescribe(parser)
	case Show:
		return ParseShow(parser)
	case Checkout:
		return ParseCheckout(parser)
	case Merge:
		return ParseMerge(parser)
	case Resolve:
		return ParseResolveConflict(parser)
	case Abort:
		return ParseAbortMerge(parser)
	case Push:
		return ParsePush(parser)
	case Pull:
		return ParsePull(parser)
	case Fetch:
		return ParseFetch(parser)
	case Copy:
		return ParseCopy(parser)
	default:
		return nil, errors.New("unknown statement type")
	}
}

func ParseSelect(parser *Parser) (Statement, error) {
	var selectStatement SelectStatement

	token := parser.lexer.NextToken()

	// Check for DISTINCT
	if token.Type == Distinct {
		selectStatement.Distinct = true
		token = parser.lexer.NextToken()
	}

	// Check for COUNT(*)
	if token.Type == Count {
		token = parser.lexer.NextToken()
		if token.Type != ParenOpen {
			return nil, errors.New("expected '(' after COUNT")
		}
		token = parser.lexer.NextToken()
		switch token.Type {
		case Wildcard:
			token = parser.lexer.NextToken()
			if token.Type != ParenClose {
				return nil, errors.New("expected ')' after COUNT(*")
			}
			selectStatement.CountAll = true
			selectStatement.Columns = []string{}
			token = parser.lexer.NextToken()
		case Identifier:
			// COUNT(column)
			col := token.Value
			token = parser.lexer.NextToken()
			if token.Type != ParenClose {
				return nil, errors.New("expected ')' after column name")
			}
			selectStatement.Aggregates = append(selectStatement.Aggregates, AggregateExpr{
				Function: "COUNT",
				Column:   col,
			})
			token = parser.lexer.NextToken()
		default:
			return nil, errors.New("expected '*' or column name in COUNT()")
		}
	} else if token.Type == Sum || token.Type == Avg || token.Type == Min || token.Type == Max {
		// Parse aggregate function: SUM(col), AVG(col), MIN(col), MAX(col)
		for {
			funcName := ""
			switch token.Type {
			case Sum:
				funcName = "SUM"
			case Avg:
				funcName = "AVG"
			case Min:
				funcName = "MIN"
			case Max:
				funcName = "MAX"
			case Count:
				funcName = "COUNT"
			}

			if funcName == "" {
				break
			}

			token = parser.lexer.NextToken()
			if token.Type != ParenOpen {
				return nil, errors.New("expected '(' after " + funcName)
			}
			token = parser.lexer.NextToken()
			if token.Type != Identifier {
				return nil, errors.New("expected column name in " + funcName + "()")
			}
			col := token.Value
			token = parser.lexer.NextToken()
			if token.Type != ParenClose {
				return nil, errors.New("expected ')' after column name")
			}

			agg := AggregateExpr{
				Function: funcName,
				Column:   col,
			}

			// Check for AS alias
			token = parser.lexer.NextToken()
			if token.Type == As {
				token = parser.lexer.NextToken()
				if token.Type != Identifier {
					return nil, errors.New("expected alias after AS")
				}
				agg.Alias = token.Value
				token = parser.lexer.NextToken()
			}

			selectStatement.Aggregates = append(selectStatement.Aggregates, agg)

			// Check for comma (more aggregates) or break
			if token.Type == Comma {
				token = parser.lexer.NextToken()
				continue
			}
			break
		}
	} else if token.Type == Upper || token.Type == Lower || token.Type == Concat ||
		token.Type == Substring || token.Type == Trim || token.Type == Length || token.Type == Replace {
		// Parse string functions: UPPER(col), LOWER(col), CONCAT(a, b), SUBSTRING(col, start, len), etc.
		for {
			funcName := ""
			switch token.Type {
			case Upper:
				funcName = "UPPER"
			case Lower:
				funcName = "LOWER"
			case Concat:
				funcName = "CONCAT"
			case Substring:
				funcName = "SUBSTRING"
			case Trim:
				funcName = "TRIM"
			case Length:
				funcName = "LENGTH"
			case Replace:
				funcName = "REPLACE"
			}

			if funcName == "" {
				break
			}

			token = parser.lexer.NextToken()
			if token.Type != ParenOpen {
				return nil, errors.New("expected '(' after " + funcName)
			}

			// Parse function arguments
			var args []string
			for {
				token = parser.lexer.NextToken()
				if token.Type == Identifier || token.Type == String || token.Type == Int {
					args = append(args, token.Value)
				} else {
					return nil, errors.New("expected argument in " + funcName + "()")
				}

				token = parser.lexer.NextToken()
				if token.Type == ParenClose {
					break
				}
				if token.Type != Comma {
					return nil, errors.New("expected ',' or ')' in " + funcName + "()")
				}
			}

			fn := FunctionExpr{
				Function: funcName,
				Args:     args,
			}

			// Check for AS alias
			token = parser.lexer.NextToken()
			if token.Type == As {
				token = parser.lexer.NextToken()
				if token.Type != Identifier {
					return nil, errors.New("expected alias after AS")
				}
				fn.Alias = token.Value
				token = parser.lexer.NextToken()
			}

			selectStatement.Functions = append(selectStatement.Functions, fn)

			// Check for comma (more functions/columns) or break
			if token.Type == Comma {
				token = parser.lexer.NextToken()
				// Check if next is another function
				if token.Type == Upper || token.Type == Lower || token.Type == Concat ||
					token.Type == Substring || token.Type == Trim || token.Type == Length || token.Type == Replace {
					continue
				}
				// Otherwise it might be a column
				if token.Type == Identifier {
					selectStatement.Columns = append(selectStatement.Columns, token.Value)
					for {
						token = parser.lexer.NextToken()
						if token.Type == Comma {
							token = parser.lexer.NextToken()
							if token.Type == Identifier {
								selectStatement.Columns = append(selectStatement.Columns, token.Value)
							} else {
								break // might be an aggregate or function
							}
						} else {
							break
						}
					}
				}
			}
			break
		}
	} else if token.Type == Now || token.Type == DateAdd || token.Type == DateSub ||
		token.Type == DateDiff || token.Type == DateFunc || token.Type == Year ||
		token.Type == Month || token.Type == Day || token.Type == Hour ||
		token.Type == Minute || token.Type == Second || token.Type == DateFormat {
		// Parse date functions
		for {
			funcName := ""
			switch token.Type {
			case Now:
				funcName = "NOW"
			case DateAdd:
				funcName = "DATE_ADD"
			case DateSub:
				funcName = "DATE_SUB"
			case DateDiff:
				funcName = "DATEDIFF"
			case DateFunc:
				funcName = "DATE"
			case Year:
				funcName = "YEAR"
			case Month:
				funcName = "MONTH"
			case Day:
				funcName = "DAY"
			case Hour:
				funcName = "HOUR"
			case Minute:
				funcName = "MINUTE"
			case Second:
				funcName = "SECOND"
			case DateFormat:
				funcName = "DATE_FORMAT"
			}

			if funcName == "" {
				break
			}

			token = parser.lexer.NextToken()
			if token.Type != ParenOpen {
				return nil, errors.New("expected '(' after " + funcName)
			}

			// Parse function arguments (NOW() can have no args)
			var args []string
			token = parser.lexer.NextToken()
			if token.Type != ParenClose {
				for {
					if token.Type == Identifier || token.Type == String || token.Type == Int {
						args = append(args, token.Value)
					} else {
						return nil, errors.New("expected argument in " + funcName + "()")
					}

					token = parser.lexer.NextToken()
					if token.Type == ParenClose {
						break
					}
					if token.Type != Comma {
						return nil, errors.New("expected ',' or ')' in " + funcName + "()")
					}
					token = parser.lexer.NextToken()
				}
			}

			fn := FunctionExpr{
				Function: funcName,
				Args:     args,
			}

			// Check for AS alias
			token = parser.lexer.NextToken()
			if token.Type == As {
				token = parser.lexer.NextToken()
				if token.Type != Identifier {
					return nil, errors.New("expected alias after AS")
				}
				fn.Alias = token.Value
				token = parser.lexer.NextToken()
			}

			selectStatement.Functions = append(selectStatement.Functions, fn)

			// Check for comma (more functions/columns) or break
			if token.Type == Comma {
				token = parser.lexer.NextToken()
				continue
			}
			break
		}
	} else if token.Type == JsonExtract || token.Type == JsonSet || token.Type == JsonRemove ||
		token.Type == JsonContains || token.Type == JsonKeys || token.Type == JsonLength || token.Type == JsonType {
		// Parse JSON functions
		for {
			funcName := ""
			switch token.Type {
			case JsonExtract:
				funcName = "JSON_EXTRACT"
			case JsonSet:
				funcName = "JSON_SET"
			case JsonRemove:
				funcName = "JSON_REMOVE"
			case JsonContains:
				funcName = "JSON_CONTAINS"
			case JsonKeys:
				funcName = "JSON_KEYS"
			case JsonLength:
				funcName = "JSON_LENGTH"
			case JsonType:
				funcName = "JSON_TYPE"
			}
			if funcName == "" {
				break
			}
			token = parser.lexer.NextToken()
			if token.Type != ParenOpen {
				return nil, errors.New("expected '(' after " + funcName)
			}
			var args []string
			token = parser.lexer.NextToken()
			if token.Type != ParenClose {
				for {
					if token.Type == Identifier || token.Type == String || token.Type == Int {
						args = append(args, token.Value)
					} else {
						return nil, errors.New("expected argument in " + funcName + "()")
					}
					token = parser.lexer.NextToken()
					if token.Type == ParenClose {
						break
					}
					if token.Type != Comma {
						return nil, errors.New("expected ',' or ')' in " + funcName + "()")
					}
					token = parser.lexer.NextToken()
				}
			}
			fn := FunctionExpr{Function: funcName, Args: args}
			token = parser.lexer.NextToken()
			if token.Type == As {
				token = parser.lexer.NextToken()
				if token.Type != Identifier {
					return nil, errors.New("expected alias after AS")
				}
				fn.Alias = token.Value
				token = parser.lexer.NextToken()
			}
			selectStatement.Functions = append(selectStatement.Functions, fn)
			if token.Type == Comma {
				token = parser.lexer.NextToken()
				continue
			}
			break
		}
	} else if token.Type == Wildcard {
		// Parse wildcard
		selectStatement.Columns = []string{}
		token = parser.lexer.NextToken()
	} else if token.Type == Identifier {
		// Parse columns (may be mixed with aggregates like: SELECT city, COUNT(*) ...)
		selectStatement.Columns = append(selectStatement.Columns, token.Value)
		for {
			token = parser.lexer.NextToken()
			if token.Type == Comma {
				token = parser.lexer.NextToken()
				if token.Type == Identifier {
					selectStatement.Columns = append(selectStatement.Columns, token.Value)
				} else if token.Type == Count {
					// Parse COUNT(*) or COUNT(col)
					token = parser.lexer.NextToken()
					if token.Type != ParenOpen {
						return nil, errors.New("expected '(' after COUNT")
					}
					token = parser.lexer.NextToken()
					if token.Type == Wildcard {
						selectStatement.Aggregates = append(selectStatement.Aggregates, AggregateExpr{Function: "COUNT", Column: "*"})
						token = parser.lexer.NextToken()
					} else if token.Type == Identifier {
						selectStatement.Aggregates = append(selectStatement.Aggregates, AggregateExpr{Function: "COUNT", Column: token.Value})
						token = parser.lexer.NextToken()
					} else {
						return nil, errors.New("expected '*' or column name in COUNT()")
					}
					if token.Type != ParenClose {
						return nil, errors.New("expected ')' after COUNT argument")
					}
				} else if token.Type == Sum || token.Type == Avg || token.Type == Min || token.Type == Max {
					// Parse SUM/AVG/MIN/MAX(col)
					funcName := ""
					switch token.Type {
					case Sum:
						funcName = "SUM"
					case Avg:
						funcName = "AVG"
					case Min:
						funcName = "MIN"
					case Max:
						funcName = "MAX"
					}
					token = parser.lexer.NextToken()
					if token.Type != ParenOpen {
						return nil, errors.New("expected '(' after " + funcName)
					}
					token = parser.lexer.NextToken()
					if token.Type != Identifier {
						return nil, errors.New("expected column name in " + funcName + "()")
					}
					col := token.Value
					token = parser.lexer.NextToken()
					if token.Type != ParenClose {
						return nil, errors.New("expected ')' after column name")
					}
					selectStatement.Aggregates = append(selectStatement.Aggregates, AggregateExpr{Function: funcName, Column: col})
				} else {
					return nil, errors.New("expected identifier or aggregate function after comma")
				}
			} else if token.Type == From {
				break
			} else {
				return nil, errors.New("expected FROM or comma")
			}
		}
	} else {
		return nil, errors.New("expected column name, *, DISTINCT, COUNT, SUM, AVG, MIN, MAX, or string function")
	}

	if token.Type != From {
		return nil, errors.New("expected FROM")
	}

	token = parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected table name")
	}

	parts := strings.Split(token.Value, ".")
	if len(parts) == 2 {
		selectStatement.Database = parts[0]
		selectStatement.Table = parts[1]
	} else {
		return nil, errors.New("expected database.table format")
	}

	token = parser.lexer.NextToken()

	// Check for table alias
	if token.Type == As {
		token = parser.lexer.NextToken()
		if token.Type != Identifier {
			return nil, errors.New("expected alias after AS")
		}
		selectStatement.TableAlias = token.Value
		token = parser.lexer.NextToken()
	} else if token.Type == Identifier {
		// Alias without AS keyword
		selectStatement.TableAlias = token.Value
		token = parser.lexer.NextToken()
	}

	// Parse JOIN clauses
	for token.Type == Join || token.Type == Inner || token.Type == Left || token.Type == Right {
		joinClause := JoinClause{Type: "INNER"} // Default

		// Determine join type
		if token.Type == Left {
			joinClause.Type = "LEFT"
			token = parser.lexer.NextToken()
			if token.Type == Outer {
				token = parser.lexer.NextToken()
			}
			if token.Type != Join {
				return nil, errors.New("expected JOIN after LEFT")
			}
		} else if token.Type == Right {
			joinClause.Type = "RIGHT"
			token = parser.lexer.NextToken()
			if token.Type == Outer {
				token = parser.lexer.NextToken()
			}
			if token.Type != Join {
				return nil, errors.New("expected JOIN after RIGHT")
			}
		} else if token.Type == Inner {
			joinClause.Type = "INNER"
			token = parser.lexer.NextToken()
			if token.Type != Join {
				return nil, errors.New("expected JOIN after INNER")
			}
		}
		// token.Type == Join at this point

		// Parse joined table
		token = parser.lexer.NextToken()
		if token.Type != Identifier {
			return nil, errors.New("expected table name after JOIN")
		}

		tableParts := strings.Split(token.Value, ".")
		if len(tableParts) == 2 {
			joinClause.Database = tableParts[0]
			joinClause.Table = tableParts[1]
		} else {
			joinClause.Table = token.Value
		}

		token = parser.lexer.NextToken()

		// Check for table alias
		if token.Type == As {
			token = parser.lexer.NextToken()
			if token.Type != Identifier {
				return nil, errors.New("expected alias after AS")
			}
			joinClause.TableAlias = token.Value
			token = parser.lexer.NextToken()
		} else if token.Type == Identifier && token.Value != "ON" {
			joinClause.TableAlias = token.Value
			token = parser.lexer.NextToken()
		}

		// Parse ON condition
		if token.Type != On {
			return nil, errors.New("expected ON after JOIN table")
		}

		token = parser.lexer.NextToken()
		if token.Type != Identifier {
			return nil, errors.New("expected column after ON")
		}
		joinClause.LeftCol = token.Value

		token = parser.lexer.NextToken()
		if token.Type != Equals {
			return nil, errors.New("expected = in JOIN ON condition")
		}

		token = parser.lexer.NextToken()
		if token.Type != Identifier {
			return nil, errors.New("expected column after = in JOIN ON")
		}
		joinClause.RightCol = token.Value

		selectStatement.Joins = append(selectStatement.Joins, joinClause)
		token = parser.lexer.NextToken()
	}

	// Parse WHERE clause
	if token.Type == Where {
		whereClause, err := ParseWhere(parser)
		if err != nil {
			return nil, err
		}
		selectStatement.Where = whereClause
		token = parser.lexer.NextToken()
	}

	// Parse GROUP BY clause
	if token.Type == Group {
		token = parser.lexer.NextToken()
		if token.Type != By {
			return nil, errors.New("expected BY after GROUP")
		}
		for {
			token = parser.lexer.NextToken()
			if token.Type != Identifier {
				return nil, errors.New("expected column name in GROUP BY")
			}
			selectStatement.GroupBy = append(selectStatement.GroupBy, token.Value)

			peek := parser.lexer.PeekToken()
			if peek.Type == Comma {
				parser.lexer.NextToken() // consume comma
				continue
			}
			break
		}
		token = parser.lexer.NextToken()
	}

	// Parse HAVING clause (only valid after GROUP BY)
	if token.Type == Having {
		havingClause, err := ParseWhere(parser)
		if err != nil {
			return nil, err
		}
		selectStatement.Having = havingClause
		token = parser.lexer.NextToken()
	}

	// Parse ORDER BY clause
	if token.Type == Order {
		token = parser.lexer.NextToken()
		if token.Type != By {
			return nil, errors.New("expected BY after ORDER")
		}
		for {
			token = parser.lexer.NextToken()
			if token.Type != Identifier {
				return nil, errors.New("expected column name in ORDER BY")
			}
			orderByClause := OrderByClause{Column: token.Value}

			// Check for ASC/DESC
			peek := parser.lexer.PeekToken()
			if peek.Type == Asc {
				parser.lexer.NextToken()
				orderByClause.Descending = false
			} else if peek.Type == Desc {
				parser.lexer.NextToken()
				orderByClause.Descending = true
			}

			selectStatement.OrderBy = append(selectStatement.OrderBy, orderByClause)

			peek = parser.lexer.PeekToken()
			if peek.Type == Comma {
				parser.lexer.NextToken() // consume comma
				continue
			}
			break
		}
		token = parser.lexer.NextToken()
	}

	// Parse LIMIT clause
	if token.Type == Limit {
		token = parser.lexer.NextToken()
		if token.Type != Int {
			return nil, errors.New("expected integer after LIMIT")
		}
		limit, err := strconv.Atoi(token.Value)
		if err != nil {
			return nil, err
		}
		selectStatement.Limit = limit
		token = parser.lexer.NextToken()
	}

	// Parse OFFSET clause
	if token.Type == Offset {
		token = parser.lexer.NextToken()
		if token.Type != Int {
			return nil, errors.New("expected integer after OFFSET")
		}
		offset, err := strconv.Atoi(token.Value)
		if err != nil {
			return nil, err
		}
		selectStatement.Offset = offset
	}

	return selectStatement, nil
}

func ParseWhere(parser *Parser) (WhereClause, error) {
	var whereClause WhereClause

	for {
		token := parser.lexer.NextToken()

		// Check for NOT
		negated := false
		if token.Type == Not {
			negated = true
			token = parser.lexer.NextToken()
		}

		if token.Type != Identifier {
			return whereClause, errors.New("expected identifier in WHERE clause")
		}
		left := token.Value

		token = parser.lexer.NextToken()

		var operator WhereOperator
		var right string

		// Handle IS NULL / IS NOT NULL
		if token.Type == Is {
			token = parser.lexer.NextToken()
			if token.Type == Not {
				token = parser.lexer.NextToken()
				if token.Type != Null {
					return whereClause, errors.New("expected NULL after IS NOT")
				}
				operator = IsNotNullOperator
			} else if token.Type == Null {
				operator = IsNullOperator
			} else {
				return whereClause, errors.New("expected NULL or NOT after IS")
			}
			right = ""
		} else if token.Type == In {
			// Handle IN (val1, val2, ...)
			operator = InOperator
			token = parser.lexer.NextToken()
			if token.Type != ParenOpen {
				return whereClause, errors.New("expected '(' after IN")
			}

			var inValues []string
			for {
				token = parser.lexer.NextToken()
				if token.Type != String && token.Type != Int {
					return whereClause, errors.New("expected value in IN list")
				}
				inValues = append(inValues, token.Value)

				token = parser.lexer.NextToken()
				if token.Type == ParenClose {
					break
				}
				if token.Type != Comma {
					return whereClause, errors.New("expected ',' or ')' in IN list")
				}
			}

			whereClause.Conditions = append(whereClause.Conditions, WhereCondition{
				Left:     left,
				Operator: operator,
				InValues: inValues,
				Negated:  negated,
			})

			token = parser.lexer.PeekToken()
			if token.Type == And {
				parser.lexer.NextToken() // consume AND
				whereClause.LogicalOps = append(whereClause.LogicalOps, LogicalAnd)
				continue
			} else if token.Type == Or {
				parser.lexer.NextToken() // consume OR
				whereClause.LogicalOps = append(whereClause.LogicalOps, LogicalOr)
				continue
			} else {
				break
			}
		} else {
			// Handle comparison operators
			switch token.Type {
			case Equals:
				operator = EqualsOperator
			case NotEquals:
				operator = NotEqualsOperator
			case LessThan:
				operator = LessThanOperator
			case GreaterThan:
				operator = GreaterThanOperator
			case LessThanOrEqual:
				operator = LessThanOrEqualOperator
			case GreaterThanOrEqual:
				operator = GreaterThanOrEqualOperator
			case Like:
				operator = LikeOperator
			default:
				return whereClause, errors.New("expected operator in WHERE clause")
			}

			token = parser.lexer.NextToken()
			if token.Type != String && token.Type != Int {
				return whereClause, errors.New("expected value in WHERE clause")
			}
			right = token.Value
		}

		whereClause.Conditions = append(whereClause.Conditions, WhereCondition{
			Left:     left,
			Operator: operator,
			Right:    right,
			Negated:  negated,
		})

		token = parser.lexer.PeekToken()
		if token.Type == And {
			parser.lexer.NextToken() // consume AND
			whereClause.LogicalOps = append(whereClause.LogicalOps, LogicalAnd)
			continue
		} else if token.Type == Or {
			parser.lexer.NextToken() // consume OR
			whereClause.LogicalOps = append(whereClause.LogicalOps, LogicalOr)
			continue
		} else {
			break
		}
	}

	return whereClause, nil
}

func ParseInsert(parser *Parser) (Statement, error) {
	var insertStatement InsertStatement

	// Parse INTO
	token := parser.lexer.NextToken()
	if token.Type != Into {
		return nil, errors.New("expected INTO after INSERT")
	}

	// Parse table name
	token = parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected table name after INSERT INTO")
	}

	parts := strings.Split(token.Value, ".")
	if len(parts) == 2 {
		insertStatement.Database = parts[0]
		insertStatement.Table = parts[1]
	} else {
		return nil, errors.New("expected database.table format")
	}

	// Parse columns
	token = parser.lexer.NextToken()
	if token.Type != ParenOpen {
		return nil, errors.New("expected '(' after table name")
	}

	for {
		token = parser.lexer.NextToken()
		if token.Type != Identifier {
			return nil, errors.New("expected column name")
		}
		insertStatement.Columns = append(insertStatement.Columns, token.Value)

		token = parser.lexer.NextToken()
		if token.Type == Comma {
			continue
		} else if token.Type == ParenClose {
			break
		} else {
			return nil, errors.New("expected ',' or ')' in column list")
		}
	}

	// Parse VALUES
	token = parser.lexer.NextToken()
	if token.Type != Values {
		return nil, errors.New("expected VALUES")
	}

	// Parse value rows (one or more): VALUES (v1, v2), (v3, v4), ...
	for {
		token = parser.lexer.NextToken()
		if token.Type != ParenOpen {
			return nil, errors.New("expected '(' after VALUES or ','")
		}

		var currentRow []string
		for {
			token = parser.lexer.NextToken()
			var value string

			switch token.Type {
			case String, Int:
				value = token.Value
			case Now:
				// Handle NOW() function
				nextToken := parser.lexer.NextToken()
				if nextToken.Type != ParenOpen {
					return nil, errors.New("expected '(' after NOW")
				}
				nextToken = parser.lexer.NextToken()
				if nextToken.Type != ParenClose {
					return nil, errors.New("expected ')' after NOW(")
				}
				value = "NOW()"
			case Null:
				value = ""
			default:
				return nil, errors.New("expected value (string, number, NOW(), or NULL)")
			}
			currentRow = append(currentRow, value)

			token = parser.lexer.NextToken()
			if token.Type == Comma {
				continue
			} else if token.Type == ParenClose {
				break
			} else {
				return nil, errors.New("expected ',' or ')' in values list")
			}
		}

		insertStatement.ValueRows = append(insertStatement.ValueRows, currentRow)

		// Check for more value rows
		token = parser.lexer.PeekToken()
		if token.Type == Comma {
			parser.lexer.NextToken() // consume the comma
			continue
		}
		break
	}

	return insertStatement, nil
}

func ParseUpdate(parser *Parser) (Statement, error) {
	var updateStatement UpdateStatement

	// Parse table name
	token := parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected table name after UPDATE")
	}

	parts := strings.Split(token.Value, ".")
	if len(parts) == 2 {
		updateStatement.Database = parts[0]
		updateStatement.Table = parts[1]
	} else {
		return nil, errors.New("expected database.table format")
	}

	// Parse SET clause
	token = parser.lexer.NextToken()
	if token.Type != Set {
		return nil, errors.New("expected SET after table name")
	}

	for {
		token = parser.lexer.NextToken()
		if token.Type != Identifier {
			return nil, errors.New("expected column name in SET clause")
		}
		column := token.Value

		token = parser.lexer.NextToken()
		if token.Type != Equals {
			return nil, errors.New("expected '=' in SET clause")
		}

		token = parser.lexer.NextToken()
		if token.Type != String && token.Type != Int {
			return nil, errors.New("expected value in SET clause")
		}
		value := token.Value

		updateStatement.Updates = append(updateStatement.Updates, SetClause{
			Column: column,
			Value:  value,
		})

		token = parser.lexer.PeekToken()
		if token.Type == Comma {
			parser.lexer.NextToken() // consume comma
			continue
		} else {
			break
		}
	}

	token = parser.lexer.NextToken()
	if token.Type == Where {
		whereClause, err := ParseWhere(parser)
		if err != nil {
			return nil, err
		}
		updateStatement.Where = whereClause
	}

	return updateStatement, nil
}

func ParseDelete(parser *Parser) (Statement, error) {
	var deleteStatement DeleteStatement

	// Parse FROM
	token := parser.lexer.NextToken()
	if token.Type != From {
		return nil, errors.New("expected FROM after DELETE")
	}

	// Parse table name
	token = parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected table name after FROM")
	}

	parts := strings.Split(token.Value, ".")
	if len(parts) == 2 {
		deleteStatement.Database = parts[0]
		deleteStatement.Table = parts[1]
	} else {
		return nil, errors.New("expected database.table format")
	}

	// Parse WHERE clause
	token = parser.lexer.NextToken()
	if token.Type == Where {
		whereClause, err := ParseWhere(parser)
		if err != nil {
			return nil, err
		}
		deleteStatement.Where = whereClause
	}

	return deleteStatement, nil
}

func ParseCreate(parser *Parser) (Statement, error) {
	token := parser.lexer.NextToken()
	switch token.Type {
	case TableIdentifier:
		return ParseCreateTable(parser)
	case DatabaseIdentifier:
		return ParseCreateDatabase(parser)
	case IndexIdentifier:
		return ParseCreateIndex(parser, false)
	case Unique:
		// UNIQUE INDEX
		token = parser.lexer.NextToken()
		if token.Type != IndexIdentifier {
			return nil, errors.New("expected INDEX after UNIQUE")
		}
		return ParseCreateIndex(parser, true)
	case Branch:
		return ParseCreateBranch(parser)
	case Remote:
		return ParseAddRemote(parser)
	default:
		return nil, errors.New("expected TABLE, DATABASE, INDEX, BRANCH, or REMOTE after CREATE")
	}
}

func ParseCreateTable(parser *Parser) (Statement, error) {
	var createTableStatement CreateTableStatement

	// Parse table name
	token := parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected table name after TABLE")
	}

	parts := strings.Split(token.Value, ".")
	if len(parts) == 2 {
		createTableStatement.Database = parts[0]
		createTableStatement.Table = parts[1]
	} else {
		return nil, errors.New("expected database.table format")
	}

	// Parse columns
	token = parser.lexer.NextToken()
	if token.Type != ParenOpen {
		return nil, errors.New("expected '(' after table name")
	}

	for {
		token = parser.lexer.NextToken()
		if token.Type != Identifier {
			return nil, errors.New("expected column name")
		}
		columnName := token.Value

		token = parser.lexer.NextToken()
		var columnType core.ColumnType
		switch toUpper(token.Value) {
		case "STRING":
			columnType = core.StringType
		case "INT", "INTEGER":
			columnType = core.IntType
		case "FLOAT", "DOUBLE", "REAL":
			columnType = core.FloatType
		case "BOOL", "BOOLEAN":
			columnType = core.BoolType
		case "TEXT":
			columnType = core.TextType
		case "DATE":
			columnType = core.DateType
		case "TIMESTAMP", "DATETIME":
			columnType = core.TimestampType
		case "JSON":
			columnType = core.JsonType
		default:
			return nil, errors.New("expected column type (STRING, INT, FLOAT, BOOL, TEXT, DATE, TIMESTAMP, JSON)")
		}

		// Check for PRIMARY KEY
		token = parser.lexer.PeekToken()
		isPrimaryKey := false
		if token.Type == PrimaryKey {
			parser.lexer.NextToken() // consume PRIMARY KEY
			isPrimaryKey = true
		}

		createTableStatement.Columns = append(createTableStatement.Columns, core.Column{
			Name:       columnName,
			Type:       columnType,
			PrimaryKey: isPrimaryKey,
		})

		token = parser.lexer.NextToken()
		if token.Type == Comma {
			continue
		} else if token.Type == ParenClose {
			break
		} else {
			return nil, errors.New("expected ',' or ')' in column list")
		}
	}

	return createTableStatement, nil
}

// ParseCreateIndex parses: CREATE [UNIQUE] INDEX name ON database.table(column)
func ParseCreateIndex(parser *Parser, unique bool) (Statement, error) {
	var statement CreateIndexStatement
	statement.Unique = unique

	// Parse index name
	token := parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected index name after INDEX")
	}
	statement.Name = token.Value

	// Parse ON
	token = parser.lexer.NextToken()
	if token.Type != On {
		return nil, errors.New("expected ON after index name")
	}

	// Parse table name (database.table or just table)
	token = parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected table name after ON")
	}

	tableParts := strings.Split(token.Value, ".")
	if len(tableParts) == 2 {
		statement.Database = tableParts[0]
		statement.Table = tableParts[1]
	} else {
		statement.Table = token.Value
	}

	// Parse (column)
	token = parser.lexer.NextToken()
	if token.Type != ParenOpen {
		return nil, errors.New("expected '(' after table name")
	}

	token = parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected column name inside parentheses")
	}
	statement.Column = token.Value

	token = parser.lexer.NextToken()
	if token.Type != ParenClose {
		return nil, errors.New("expected ')' after column name")
	}

	return statement, nil
}

func ParseDrop(parser *Parser) (Statement, error) {
	token := parser.lexer.NextToken()
	switch token.Type {
	case TableIdentifier:
		return ParseDropTable(parser)
	case DatabaseIdentifier:
		return ParseDropDatabase(parser)
	case IndexIdentifier:
		return ParseDropIndex(parser)
	case Remote:
		return ParseDropRemote(parser)
	default:
		return nil, errors.New("expected TABLE, DATABASE, INDEX, or REMOTE after DROP")
	}
}

// ParseDropIndex parses: DROP INDEX name ON database.table
func ParseDropIndex(parser *Parser) (Statement, error) {
	var statement DropIndexStatement

	// Parse index name
	token := parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected index name after INDEX")
	}
	statement.Name = token.Value

	// Parse ON
	token = parser.lexer.NextToken()
	if token.Type != On {
		return nil, errors.New("expected ON after index name")
	}

	// Parse table name
	token = parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected table name after ON")
	}

	tableParts := strings.Split(token.Value, ".")
	if len(tableParts) == 2 {
		statement.Database = tableParts[0]
		statement.Table = tableParts[1]
	} else {
		statement.Table = token.Value
	}

	return statement, nil
}

func ParseDropTable(parser *Parser) (Statement, error) {
	var dropTableStatement DropTableStatement

	// Parse table name
	token := parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected table name after TABLE")
	}

	parts := strings.Split(token.Value, ".")
	if len(parts) == 2 {
		dropTableStatement.Database = parts[0]
		dropTableStatement.Table = parts[1]
	} else {
		return nil, errors.New("expected database.table format")
	}

	return dropTableStatement, nil
}

func ParseCreateDatabase(parser *Parser) (Statement, error) {
	var createDatabaseStatement CreateDatabaseStatement

	// Parse database name
	token := parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected database name after DATABASE")
	}
	createDatabaseStatement.Database = token.Value

	return createDatabaseStatement, nil
}

func ParseDropDatabase(parser *Parser) (Statement, error) {
	var dropDatabaseStatement DropDatabaseStatement

	// Parse database name
	token := parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected database name after DATABASE")
	}
	dropDatabaseStatement.Database = token.Value

	return dropDatabaseStatement, nil
}

func ParseShow(parser *Parser) (Statement, error) {
	token := parser.lexer.NextToken()
	switch token.Type {
	case DatabasesIdentifier:
		return ShowDatabasesStatement{}, nil
	case TablesIdentifier:
		// Parse IN
		token = parser.lexer.NextToken()
		if token.Type != In {
			return nil, errors.New("expected IN after TABLES")
		}
		token = parser.lexer.NextToken()
		if token.Type != Identifier {
			return nil, errors.New("expected database name after IN")
		}
		return ShowTablesStatement{Database: token.Value}, nil
	case IndexIdentifier:
		// SHOW INDEXES ON database.table
		token = parser.lexer.NextToken()
		if token.Type != On {
			return nil, errors.New("expected ON after INDEXES")
		}
		token = parser.lexer.NextToken()
		if token.Type != Identifier {
			return nil, errors.New("expected table name after ON")
		}
		tableParts := strings.Split(token.Value, ".")
		if len(tableParts) == 2 {
			return ShowIndexesStatement{Database: tableParts[0], Table: tableParts[1]}, nil
		}
		return ShowIndexesStatement{Table: token.Value}, nil
	case Branches:
		return ShowBranchesStatement{}, nil
	case Merge:
		// SHOW MERGE CONFLICTS
		token = parser.lexer.NextToken()
		if token.Type != Conflicts {
			return nil, errors.New("expected CONFLICTS after MERGE")
		}
		return ShowMergeConflictsStatement{}, nil
	case Remotes:
		return ShowRemotesStatement{}, nil
	default:
		return nil, errors.New("expected DATABASES, TABLES, INDEXES, BRANCHES, REMOTES, or MERGE CONFLICTS after SHOW")
	}
}

// ParseAlter parses ALTER TABLE statements
func ParseAlter(parser *Parser) (Statement, error) {
	token := parser.lexer.NextToken()
	if token.Type != TableIdentifier {
		return nil, errors.New("expected TABLE after ALTER")
	}

	var statement AlterTableStatement

	// Parse table name
	token = parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected table name")
	}
	tableParts := strings.Split(token.Value, ".")
	if len(tableParts) == 2 {
		statement.Database = tableParts[0]
		statement.Table = tableParts[1]
	} else {
		statement.Table = token.Value
	}

	// Parse action: ADD, DROP, MODIFY, RENAME
	token = parser.lexer.NextToken()
	switch token.Type {
	case Add:
		statement.Action = "ADD"
	case Drop:
		statement.Action = "DROP"
	case Modify:
		statement.Action = "MODIFY"
	case Rename:
		statement.Action = "RENAME"
	default:
		return nil, errors.New("expected ADD, DROP, MODIFY, or RENAME")
	}

	// Parse COLUMN (optional)
	token = parser.lexer.NextToken()
	if token.Type == Column {
		token = parser.lexer.NextToken()
	}

	// Parse column name
	if token.Type != Identifier {
		return nil, errors.New("expected column name")
	}
	statement.ColumnName = token.Value

	// Parse column type for ADD and MODIFY
	if statement.Action == "ADD" || statement.Action == "MODIFY" {
		token = parser.lexer.NextToken()
		if token.Type != Identifier {
			return nil, errors.New("expected column type")
		}
		statement.ColumnType = token.Value
	}

	// Parse TO newname for RENAME
	if statement.Action == "RENAME" {
		token = parser.lexer.NextToken()
		if token.Type != To {
			return nil, errors.New("expected TO after RENAME COLUMN oldname")
		}
		token = parser.lexer.NextToken()
		if token.Type != Identifier {
			return nil, errors.New("expected new column name after TO")
		}
		statement.NewColumnName = token.Value
	}

	return statement, nil
}

// ParseDescribe parses DESCRIBE table statements
func ParseDescribe(parser *Parser) (Statement, error) {
	token := parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected table name after DESCRIBE")
	}

	tableParts := strings.Split(token.Value, ".")
	if len(tableParts) == 2 {
		return DescribeStatement{Database: tableParts[0], Table: tableParts[1]}, nil
	}
	return DescribeStatement{Table: token.Value}, nil
}

func parse(sql string) (Statement, error) {
	parser := NewParser(sql)

	return parser.Parse()
}

// ParseCreateBranch parses CREATE BRANCH statements
// Syntax: CREATE BRANCH name [FROM 'transaction_id']
func ParseCreateBranch(parser *Parser) (Statement, error) {
	var stmt CreateBranchStatement

	// Parse branch name
	token := parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected branch name after CREATE BRANCH")
	}
	stmt.Name = token.Value

	// Check for optional FROM clause
	token = parser.lexer.NextToken()
	if token.Type == From {
		token = parser.lexer.NextToken()
		if token.Type != String && token.Type != Identifier {
			return nil, errors.New("expected transaction ID after FROM")
		}
		stmt.FromTxnId = token.Value
	}

	return stmt, nil
}

// ParseCheckout parses CHECKOUT statements
// Syntax: CHECKOUT branch_name
func ParseCheckout(parser *Parser) (Statement, error) {
	token := parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected branch name after CHECKOUT")
	}
	return CheckoutStatement{Branch: token.Value}, nil
}

// ParseMerge parses MERGE statements
// Syntax: MERGE branch_name [WITH MANUAL RESOLUTION]
func ParseMerge(parser *Parser) (Statement, error) {
	token := parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected branch name after MERGE")
	}

	stmt := MergeStatement{SourceBranch: token.Value}

	// Check for WITH MANUAL RESOLUTION
	nextToken := parser.lexer.PeekToken()
	if nextToken.Type == With {
		parser.lexer.NextToken() // consume WITH
		token = parser.lexer.NextToken()
		if token.Type != Manual {
			return nil, errors.New("expected MANUAL after WITH")
		}
		token = parser.lexer.NextToken()
		if token.Type != Resolution {
			return nil, errors.New("expected RESOLUTION after MANUAL")
		}
		stmt.ManualResolution = true
	}

	return stmt, nil
}

// ParseResolveConflict parses RESOLVE CONFLICT statements
// Syntax: RESOLVE CONFLICT db.table.key USING HEAD|SOURCE|'value'
func ParseResolveConflict(parser *Parser) (Statement, error) {
	// Expect CONFLICT
	token := parser.lexer.NextToken()
	if token.Type != Conflict {
		return nil, errors.New("expected CONFLICT after RESOLVE")
	}

	// Get the conflict path (db.table.key)
	token = parser.lexer.NextToken()
	if token.Type != Identifier {
		return nil, errors.New("expected conflict path (db.table.key)")
	}
	path := token.Value

	// Parse db.table.key
	parts := splitPath(path)
	if len(parts) != 3 {
		return nil, errors.New("conflict path must be db.table.key format")
	}

	// Expect USING
	token = parser.lexer.NextToken()
	if token.Type != Using {
		return nil, errors.New("expected USING after conflict path")
	}

	// Get resolution: HEAD, SOURCE, or 'value'
	token = parser.lexer.NextToken()
	var resolution string
	switch token.Type {
	case Head:
		resolution = "HEAD"
	case Source:
		resolution = "SOURCE"
	case String:
		resolution = token.Value
	default:
		return nil, errors.New("expected HEAD, SOURCE, or string value after USING")
	}

	return ResolveConflictStatement{
		Database:   parts[0],
		Table:      parts[1],
		Key:        parts[2],
		Resolution: resolution,
	}, nil
}

// ParseAbortMerge parses ABORT MERGE statements
func ParseAbortMerge(parser *Parser) (Statement, error) {
	token := parser.lexer.NextToken()
	if token.Type != Merge {
		return nil, errors.New("expected MERGE after ABORT")
	}
	return AbortMergeStatement{}, nil
}

// splitPath splits "db.table.key" into parts
func splitPath(path string) []string {
	parts := make([]string, 0, 3)
	start := 0
	for i, c := range path {
		if c == '.' {
			parts = append(parts, path[start:i])
			start = i + 1
		}
	}
	parts = append(parts, path[start:])
	return parts
}

// ParseAddRemote parses CREATE REMOTE <name> <url> statements
func ParseAddRemote(parser *Parser) (Statement, error) {
	// Expect remote name
	token := parser.lexer.NextToken()
	if token.Type != Identifier && token.Type != String {
		return nil, errors.New("expected remote name after REMOTE")
	}
	name := token.Value

	// Expect URL
	token = parser.lexer.NextToken()
	if token.Type != String && token.Type != Identifier {
		return nil, errors.New("expected URL after remote name")
	}
	url := token.Value

	return AddRemoteStatement{Name: name, URL: url}, nil
}

// ParseDropRemote parses DROP REMOTE <name> statements
func ParseDropRemote(parser *Parser) (Statement, error) {
	token := parser.lexer.NextToken()
	if token.Type != Identifier && token.Type != String {
		return nil, errors.New("expected remote name after REMOTE")
	}
	return DropRemoteStatement{Name: token.Value}, nil
}

// ParsePush parses PUSH [TO <remote>] [BRANCH <branch>] [WITH TOKEN 'xxx' | WITH SSH KEY 'path' [PASSPHRASE 'xxx']]
func ParsePush(parser *Parser) (Statement, error) {
	stmt := PushStatement{Remote: "origin"} // default remote

	for {
		token := parser.lexer.NextToken()
		if token.Type == EOF || token.Type == Unknown {
			break
		}

		switch token.Type {
		case To:
			// TO <remote>
			token = parser.lexer.NextToken()
			if token.Type != Identifier && token.Type != String {
				return nil, errors.New("expected remote name after TO")
			}
			stmt.Remote = token.Value
		case Branch:
			// BRANCH <branch>
			token = parser.lexer.NextToken()
			if token.Type != Identifier && token.Type != String {
				return nil, errors.New("expected branch name after BRANCH")
			}
			stmt.Branch = token.Value
		case With:
			// WITH TOKEN 'xxx' | WITH SSH KEY 'path' [PASSPHRASE 'xxx']
			auth, err := parseAuth(parser)
			if err != nil {
				return nil, err
			}
			stmt.Auth = auth
		default:
			// Unknown token, might be end of statement
			return stmt, nil
		}
	}

	return stmt, nil
}

// ParsePull parses PULL [FROM <remote>] [BRANCH <branch>] [WITH TOKEN 'xxx' | WITH SSH KEY 'path' [PASSPHRASE 'xxx']]
func ParsePull(parser *Parser) (Statement, error) {
	stmt := PullStatement{Remote: "origin"} // default remote

	for {
		token := parser.lexer.NextToken()
		if token.Type == EOF || token.Type == Unknown {
			break
		}

		switch token.Type {
		case From:
			// FROM <remote>
			token = parser.lexer.NextToken()
			if token.Type != Identifier && token.Type != String {
				return nil, errors.New("expected remote name after FROM")
			}
			stmt.Remote = token.Value
		case Branch:
			// BRANCH <branch>
			token = parser.lexer.NextToken()
			if token.Type != Identifier && token.Type != String {
				return nil, errors.New("expected branch name after BRANCH")
			}
			stmt.Branch = token.Value
		case With:
			// WITH TOKEN 'xxx' | WITH SSH KEY 'path' [PASSPHRASE 'xxx']
			auth, err := parseAuth(parser)
			if err != nil {
				return nil, err
			}
			stmt.Auth = auth
		default:
			// Unknown token, might be end of statement
			return stmt, nil
		}
	}

	return stmt, nil
}

// ParseFetch parses FETCH [FROM <remote>] [WITH TOKEN 'xxx' | WITH SSH KEY 'path' [PASSPHRASE 'xxx']]
func ParseFetch(parser *Parser) (Statement, error) {
	stmt := FetchStatement{Remote: "origin"} // default remote

	for {
		token := parser.lexer.NextToken()
		if token.Type == EOF || token.Type == Unknown {
			break
		}

		switch token.Type {
		case From:
			// FROM <remote>
			token = parser.lexer.NextToken()
			if token.Type != Identifier && token.Type != String {
				return nil, errors.New("expected remote name after FROM")
			}
			stmt.Remote = token.Value
		case With:
			// WITH TOKEN 'xxx' | WITH SSH KEY 'path' [PASSPHRASE 'xxx']
			auth, err := parseAuth(parser)
			if err != nil {
				return nil, err
			}
			stmt.Auth = auth
		default:
			// Unknown token, might be end of statement
			return stmt, nil
		}
	}

	return stmt, nil
}

// parseAuth parses authentication options: TOKEN 'xxx' | SSH KEY 'path' [PASSPHRASE 'xxx'] | USER 'username' PASSWORD 'password'
func parseAuth(parser *Parser) (*AuthConfig, error) {
	token := parser.lexer.NextToken()
	auth := &AuthConfig{}

	switch token.Type {
	case TokenKeyword:
		// TOKEN 'value'
		token = parser.lexer.NextToken()
		if token.Type != String {
			return nil, errors.New("expected string value after TOKEN")
		}
		auth.Token = token.Value
		return auth, nil

	case Ssh:
		// SSH KEY 'path' [PASSPHRASE 'xxx']
		token = parser.lexer.NextToken()
		if token.Type != Key {
			return nil, errors.New("expected KEY after SSH")
		}
		token = parser.lexer.NextToken()
		if token.Type != String {
			return nil, errors.New("expected path string after SSH KEY")
		}
		auth.SSHKeyPath = token.Value

		// Check for optional PASSPHRASE
		token = parser.lexer.PeekToken()
		if token.Type == Passphrase {
			parser.lexer.NextToken() // consume PASSPHRASE
			token = parser.lexer.NextToken()
			if token.Type != String {
				return nil, errors.New("expected string value after PASSPHRASE")
			}
			auth.Passphrase = token.Value
		}
		return auth, nil

	case Identifier:
		// Check for USER 'username' PASSWORD 'password'
		if strings.ToUpper(token.Value) == "USER" {
			token = parser.lexer.NextToken()
			if token.Type != String {
				return nil, errors.New("expected string value after USER")
			}
			auth.Username = token.Value

			token = parser.lexer.NextToken()
			if token.Type != Identifier || strings.ToUpper(token.Value) != "PASSWORD" {
				return nil, errors.New("expected PASSWORD after username")
			}
			token = parser.lexer.NextToken()
			if token.Type != String {
				return nil, errors.New("expected string value after PASSWORD")
			}
			auth.Password = token.Value
			return auth, nil
		}
		return nil, errors.New("expected TOKEN, SSH, or USER after WITH")

	default:
		return nil, errors.New("expected TOKEN, SSH, or USER after WITH")
	}
}

// ParseCopy parses COPY INTO commands for bulk data import/export
// COPY INTO table FROM 'file.csv' [WITH (HEADER = TRUE, DELIMITER = ',')]
// COPY INTO 'file.csv' FROM table [WITH (HEADER = TRUE)]
func ParseCopy(parser *Parser) (Statement, error) {
	stmt := CopyStatement{
		Delimiter: ",",  // default delimiter
		Header:    true, // default to having headers
	}

	// Expect INTO
	token := parser.lexer.NextToken()
	if token.Type != Into {
		return nil, errors.New("expected INTO after COPY")
	}

	// Next token determines direction: identifier = INTO_TABLE, string = INTO_FILE
	token = parser.lexer.NextToken()
	if token.Type == String {
		// COPY INTO 'file.csv' FROM table (export)
		stmt.Direction = "INTO_FILE"
		stmt.FilePath = token.Value

		// Expect FROM
		token = parser.lexer.NextToken()
		if token.Type != From {
			return nil, errors.New("expected FROM after file path")
		}

		// Expect table name (database.table)
		token = parser.lexer.NextToken()
		if token.Type == DatabaseIdentifier {
			parts := strings.Split(token.Value, ".")
			stmt.Database = parts[0]
			stmt.Table = parts[1]
		} else if token.Type == Identifier && strings.Contains(token.Value, ".") {
			parts := strings.Split(token.Value, ".")
			stmt.Database = parts[0]
			stmt.Table = parts[1]
		} else {
			return nil, errors.New("expected database.table after FROM")
		}

	} else if token.Type == Identifier || token.Type == DatabaseIdentifier {
		// COPY INTO table FROM 'file.csv' (import)
		stmt.Direction = "INTO_TABLE"
		if token.Type == DatabaseIdentifier {
			parts := strings.Split(token.Value, ".")
			stmt.Database = parts[0]
			stmt.Table = parts[1]
		} else if token.Type == Identifier && strings.Contains(token.Value, ".") {
			parts := strings.Split(token.Value, ".")
			stmt.Database = parts[0]
			stmt.Table = parts[1]
		} else {
			return nil, errors.New("expected database.table after INTO")
		}

		// Expect FROM
		token = parser.lexer.NextToken()
		if token.Type != From {
			return nil, errors.New("expected FROM after table name")
		}

		// Expect file path
		token = parser.lexer.NextToken()
		if token.Type != String {
			return nil, errors.New("expected file path (string) after FROM")
		}
		stmt.FilePath = token.Value
	} else {
		return nil, errors.New("expected table name or file path after INTO")
	}

	// Optional: WITH (HEADER = TRUE, DELIMITER = ',', AWS_KEY = '...', ...)
	token = parser.lexer.PeekToken()
	if token.Type == With {
		parser.lexer.NextToken() // consume WITH

		// Expect opening paren
		token = parser.lexer.NextToken()
		if token.Type != ParenOpen {
			return nil, errors.New("expected '(' after WITH")
		}

		// Parse options
		for {
			token = parser.lexer.NextToken()
			switch token.Type {
			case Header:
				// HEADER = TRUE/FALSE
				token = parser.lexer.NextToken()
				if token.Type != Equals {
					return nil, errors.New("expected '=' after HEADER")
				}
				token = parser.lexer.NextToken()
				if token.Type == Identifier {
					stmt.Header = (toUpper(token.Value) == "TRUE")
				} else {
					return nil, errors.New("expected TRUE or FALSE after HEADER =")
				}
			case Delimiter:
				// DELIMITER = ','
				token = parser.lexer.NextToken()
				if token.Type != Equals {
					return nil, errors.New("expected '=' after DELIMITER")
				}
				token = parser.lexer.NextToken()
				if token.Type != String {
					return nil, errors.New("expected string after DELIMITER =")
				}
				stmt.Delimiter = token.Value
			case AwsKey:
				// AWS_KEY = '...'
				token = parser.lexer.NextToken()
				if token.Type != Equals {
					return nil, errors.New("expected '=' after AWS_KEY")
				}
				token = parser.lexer.NextToken()
				if token.Type != String {
					return nil, errors.New("expected string after AWS_KEY =")
				}
				stmt.S3AccessKey = token.Value
			case AwsSecret:
				// AWS_SECRET = '...'
				token = parser.lexer.NextToken()
				if token.Type != Equals {
					return nil, errors.New("expected '=' after AWS_SECRET")
				}
				token = parser.lexer.NextToken()
				if token.Type != String {
					return nil, errors.New("expected string after AWS_SECRET =")
				}
				stmt.S3SecretKey = token.Value
			case AwsRegion:
				// AWS_REGION = '...'
				token = parser.lexer.NextToken()
				if token.Type != Equals {
					return nil, errors.New("expected '=' after AWS_REGION")
				}
				token = parser.lexer.NextToken()
				if token.Type != String {
					return nil, errors.New("expected string after AWS_REGION =")
				}
				stmt.S3Region = token.Value
			default:
				return nil, errors.New("expected HEADER, DELIMITER, AWS_KEY, AWS_SECRET, or AWS_REGION in WITH clause")
			}

			// Check for comma or closing paren
			token = parser.lexer.NextToken()
			if token.Type == ParenClose {
				break
			} else if token.Type != Comma {
				return nil, errors.New("expected ',' or ')' in WITH clause")
			}
		}
	}

	return stmt, nil
}
