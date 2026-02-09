package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	commitdb "github.com/nickyhof/CommitDB/v2"
	"github.com/nickyhof/CommitDB/v2/core"
	"github.com/nickyhof/CommitDB/v2/engine"
	"github.com/nickyhof/CommitDB/v2/persistence"
)

const (
	PromptColor  = "\033[36m" // Cyan
	ErrorColor   = "\033[31m" // Red
	SuccessColor = "\033[32m" // Green
	ResetColor   = "\033[0m"
	BoldColor    = "\033[1m"
)

// Version is set at build time via -ldflags
var Version = "dev"

// CLI holds the CLI state
type CLI struct {
	engine   *engine.Engine
	database string // current database context
}

func main() {
	baseDir := flag.String("dir", "", "Base directory for the database")
	gitUrl := flag.String("url", "", "Git URL for the database")
	sqlFile := flag.String("f", "", "SQL file to execute (non-interactive)")
	sqlExec := flag.String("e", "", "Execute SQL statement and exit")
	userName := flag.String("name", "CommitDB", "User name for Git commits")
	userEmail := flag.String("email", "cli@commitdb.local", "User email for Git commits")

	flag.Parse()

	// Determine if we're in interactive mode
	interactive := *sqlExec == "" && *sqlFile == "" && isTerminal()

	if interactive {
		printBanner()
	}

	var Instance commitdb.Instance

	if *baseDir == "" {
		if interactive {
			fmt.Printf("%sUsing memory persistence%s\n", SuccessColor, ResetColor)
		}
		p, err := persistence.NewMemoryPersistence()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		Instance = *commitdb.Open(&p)
	} else {
		if interactive {
			fmt.Printf("%sUsing file persistence: %s%s\n", SuccessColor, *baseDir, ResetColor)
		}
		var gitUrlPtr *string
		if *gitUrl != "" {
			gitUrlPtr = gitUrl
		}
		p, err := persistence.NewFilePersistence(*baseDir, gitUrlPtr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		Instance = *commitdb.Open(&p)
	}

	e := Instance.Engine(core.Identity{
		Name:  *userName,
		Email: *userEmail,
	})

	cli := &CLI{
		engine: e,
	}

	// Execute SQL directly if provided
	if *sqlExec != "" {
		result, err := cli.engine.Execute(*sqlExec)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		result.Display()
		return
	}

	// Execute SQL file if provided
	if *sqlFile != "" {
		err := cli.importFile(*sqlFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error importing file: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Read from stdin pipe if not a terminal
	if !interactive {
		cli.runPipe()
		return
	}

	cli.run()
}

// isTerminal returns true if stdin is a terminal (not piped)
func isTerminal() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return true
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

func printBanner() {
	fmt.Println()
	bannerWidth := 39 // inner width of the banner box
	versionLine := fmt.Sprintf("CommitDB v%s", Version)
	padding := bannerWidth - len(versionLine) - 2 // -2 for "  " margins
	if padding < 0 {
		padding = 0
	}
	leftPad := padding / 2
	rightPad := padding - leftPad

	fmt.Printf("%s%s╔═══════════════════════════════════════╗%s\n", BoldColor, PromptColor, ResetColor)
	fmt.Printf("%s%s║ %*s%s%*s ║%s\n", BoldColor, PromptColor, leftPad, "", versionLine, rightPad, "", ResetColor)
	fmt.Printf("%s%s║   Git-backed SQL Database Engine      ║%s\n", BoldColor, PromptColor, ResetColor)
	fmt.Printf("%s%s╚═══════════════════════════════════════╝%s\n", BoldColor, PromptColor, ResetColor)
	fmt.Println()
	fmt.Println("Type .help for commands, .quit to exit")
	fmt.Println()
}

func (cli *CLI) run() {
	reader := bufio.NewReader(os.Stdin)
	var multiLineBuffer strings.Builder

	for {
		// Show prompt
		prompt := cli.getPrompt(multiLineBuffer.Len() > 0)
		fmt.Print(prompt)

		// Read input
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("\n%sGoodbye!%s\n", SuccessColor, ResetColor)
			return
		}

		input = strings.TrimSuffix(input, "\n")
		input = strings.TrimSuffix(input, "\r")

		// Handle empty input
		if strings.TrimSpace(input) == "" {
			continue
		}

		// Check for special commands (only when not in multi-line mode)
		if multiLineBuffer.Len() == 0 && strings.HasPrefix(input, ".") {
			if cli.handleCommand(input) {
				continue
			}
		}

		// Multi-line support: accumulate until we see a semicolon
		multiLineBuffer.WriteString(input)

		// Check if the statement is complete (ends with ;)
		trimmed := strings.TrimSpace(multiLineBuffer.String())
		if !strings.HasSuffix(trimmed, ";") {
			multiLineBuffer.WriteString(" ")
			continue
		}

		// Execute the complete statement
		sql := strings.TrimSuffix(trimmed, ";")
		multiLineBuffer.Reset()

		if strings.TrimSpace(sql) == "" {
			continue
		}

		// Execute SQL
		result, err := cli.engine.Execute(sql)
		if err != nil {
			fmt.Printf("%s✗ Error: %v%s\n", ErrorColor, err, ResetColor)
		} else {
			result.Display()
		}
	}
}

// runPipe reads SQL from stdin (piped input) and executes each statement
func (cli *CLI) runPipe() {
	var buf strings.Builder
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		buf.WriteString(scanner.Text())
		buf.WriteString("\n")
	}

	statements := splitStatements(buf.String())
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}

		result, err := cli.engine.Execute(stmt)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		result.Display()
	}
}

func (cli *CLI) getPrompt(multiLine bool) string {
	if multiLine {
		return fmt.Sprintf("%s   ...>%s ", PromptColor, ResetColor)
	}

	dbPart := ""
	if cli.database != "" {
		dbPart = fmt.Sprintf(" (%s)", cli.database)
	}

	return fmt.Sprintf("%scommitdb%s>%s ", PromptColor, dbPart, ResetColor)
}

func (cli *CLI) handleCommand(input string) bool {
	cmd := strings.ToLower(strings.TrimSpace(input))
	parts := strings.Fields(cmd)

	if len(parts) == 0 {
		return true
	}

	switch parts[0] {
	case ".quit", ".exit", ".q":
		fmt.Printf("%sGoodbye!%s\n", SuccessColor, ResetColor)
		os.Exit(0)

	case ".help", ".h", ".?":
		cli.printHelp()

	case ".tables":
		if len(parts) > 1 {
			cli.showTables(parts[1])
		} else if cli.database != "" {
			cli.showTables(cli.database)
		} else {
			fmt.Printf("%s✗ Usage: .tables <database>%s\n", ErrorColor, ResetColor)
		}

	case ".databases", ".dbs":
		cli.showDatabases()

	case ".use":
		if len(parts) > 1 {
			cli.database = parts[1]
			fmt.Printf("%s✓ Using database: %s%s\n", SuccessColor, cli.database, ResetColor)
		} else {
			fmt.Printf("%s✗ Usage: .use <database>%s\n", ErrorColor, ResetColor)
		}

	case ".clear", ".cls":
		fmt.Print("\033[H\033[2J")

	case ".version":
		fmt.Printf("CommitDB version %s\n", Version)

	case ".import":
		if len(parts) > 1 {
			err := cli.importFile(parts[1])
			if err != nil {
				fmt.Printf("%s✗ Error: %v%s\n", ErrorColor, err, ResetColor)
			}
		} else {
			fmt.Printf("%s✗ Usage: .import <file.sql>%s\n", ErrorColor, ResetColor)
		}

	default:
		fmt.Printf("%s✗ Unknown command: %s (type .help for commands)%s\n", ErrorColor, parts[0], ResetColor)
	}

	return true
}

func (cli *CLI) printHelp() {
	fmt.Println()
	fmt.Printf("%s%sSpecial Commands:%s\n", BoldColor, PromptColor, ResetColor)
	fmt.Println("  .help, .h        Show this help message")
	fmt.Println("  .quit, .exit     Exit the CLI")
	fmt.Println("  .databases       List all databases")
	fmt.Println("  .tables <db>     List tables in a database")
	fmt.Println("  .use <db>        Set the current database context")
	fmt.Println("  .import <file>   Execute SQL statements from a file")
	fmt.Println("  .clear           Clear the screen")
	fmt.Println("  .version         Show version info")
	fmt.Println()
	fmt.Printf("%s%sSQL Commands:%s\n", BoldColor, PromptColor, ResetColor)
	fmt.Println("  CREATE DATABASE <name>;")
	fmt.Println("  CREATE TABLE <db>.<table> (<column> <type>, ...);")
	fmt.Println("  DROP DATABASE <name>;")
	fmt.Println("  DROP TABLE <db>.<table>;")
	fmt.Println("  INSERT INTO <db>.<table> (<cols>) VALUES (<vals>);")
	fmt.Println("  SELECT <cols> FROM <db>.<table> [WHERE ...] [ORDER BY ...] [LIMIT n];")
	fmt.Println("  UPDATE <db>.<table> SET <col>=<val> WHERE <pk>=<val>;")
	fmt.Println("  DELETE FROM <db>.<table> WHERE <pk>=<val>;")
	fmt.Println("  DESCRIBE <db>.<table>;")
	fmt.Println("  SHOW DATABASES;")
	fmt.Println("  SHOW TABLES IN <db>;")
	fmt.Println()
	fmt.Printf("%s%sAggregates:%s SUM, AVG, MIN, MAX, COUNT, GROUP BY\n", BoldColor, PromptColor, ResetColor)
	fmt.Printf("%s%sJoins:%s INNER JOIN, LEFT JOIN, RIGHT JOIN\n", BoldColor, PromptColor, ResetColor)
	fmt.Println()
}

func (cli *CLI) showDatabases() {
	result, err := cli.engine.Execute("SHOW DATABASES")
	if err != nil {
		fmt.Printf("%s✗ Error: %v%s\n", ErrorColor, err, ResetColor)
		return
	}
	result.Display()
}

func (cli *CLI) showTables(database string) {
	result, err := cli.engine.Execute(fmt.Sprintf("SHOW TABLES IN %s", database))
	if err != nil {
		fmt.Printf("%s✗ Error: %v%s\n", ErrorColor, err, ResetColor)
		return
	}
	result.Display()
}

// importFile reads and executes SQL statements from a file
func (cli *CLI) importFile(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	content := string(data)
	statements := splitStatements(content)

	successCount := 0
	errorCount := 0

	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}

		result, err := cli.engine.Execute(stmt)
		if err != nil {
			fmt.Printf("%s[%d] ✗ %s%s\n", ErrorColor, i+1, truncate(stmt, 50), ResetColor)
			fmt.Printf("      Error: %v\n", err)
			errorCount++
		} else {
			successCount++
			// Compact output based on result type
			switch r := result.(type) {
			case engine.CommitResult:
				var details []string
				if r.DatabasesCreated > 0 {
					details = append(details, fmt.Sprintf("%d db created", r.DatabasesCreated))
				}
				if r.DatabasesDeleted > 0 {
					details = append(details, fmt.Sprintf("%d db deleted", r.DatabasesDeleted))
				}
				if r.TablesCreated > 0 {
					details = append(details, fmt.Sprintf("%d table created", r.TablesCreated))
				}
				if r.TablesDeleted > 0 {
					details = append(details, fmt.Sprintf("%d table deleted", r.TablesDeleted))
				}
				if r.RecordsWritten > 0 {
					details = append(details, fmt.Sprintf("%d written", r.RecordsWritten))
				}
				if r.RecordsDeleted > 0 {
					details = append(details, fmt.Sprintf("%d deleted", r.RecordsDeleted))
				}
				detailStr := ""
				if len(details) > 0 {
					detailStr = " (" + strings.Join(details, ", ") + ")"
				}
				fmt.Printf("%s[%d] ✓ %s%s%s\n", SuccessColor, i+1, truncate(stmt, 50), detailStr, ResetColor)
			case engine.QueryResult:
				fmt.Printf("%s[%d] ✓ %s (%d rows)%s\n", SuccessColor, i+1, truncate(stmt, 50), r.RecordsRead, ResetColor)
			default:
				fmt.Printf("%s[%d] ✓ %s%s\n", SuccessColor, i+1, truncate(stmt, 50), ResetColor)
			}
		}
	}

	fmt.Printf("\n%s✓ Import complete: %d succeeded, %d failed%s\n",
		SuccessColor, successCount, errorCount, ResetColor)

	return nil
}

// splitStatements splits SQL content into individual statements
func splitStatements(content string) []string {
	var statements []string
	var current strings.Builder
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(content); i++ {
		ch := content[i]

		// Handle string literals
		if (ch == '\'' || ch == '"') && (i == 0 || content[i-1] != '\\') {
			if !inString {
				inString = true
				stringChar = ch
			} else if ch == stringChar {
				inString = false
			}
		}

		// Handle comments
		if !inString && ch == '-' && i+1 < len(content) && content[i+1] == '-' {
			// Skip to end of line
			for i < len(content) && content[i] != '\n' {
				i++
			}
			continue
		}

		// Statement separator
		if !inString && ch == ';' {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
			continue
		}

		current.WriteByte(ch)
	}

	// Handle last statement without semicolon
	stmt := strings.TrimSpace(current.String())
	if stmt != "" {
		statements = append(statements, stmt)
	}

	return statements
}

// truncate shortens a string to max length with ellipsis
func truncate(s string, max int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}
