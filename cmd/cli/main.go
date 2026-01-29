package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nickyhof/CommitDB"
	"github.com/nickyhof/CommitDB/core"
	"github.com/nickyhof/CommitDB/db"
	"github.com/nickyhof/CommitDB/ps"
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
	engine      *db.Engine
	history     []string
	historyFile string
	database    string // current database context
}

func main() {
	baseDir := flag.String("baseDir", "", "Base directory for the database")
	gitUrl := flag.String("gitUrl", "", "Git URL for the database")
	sqlFile := flag.String("sqlFile", "", "SQL file to execute (non-interactive)")
	userName := flag.String("name", "CommitDB", "User name for Git commits")
	userEmail := flag.String("email", "cli@commitdb.local", "User email for Git commits")
	flag.Parse()

	printBanner()

	var Instance CommitDB.Instance

	if *baseDir == "" {
		fmt.Printf("%sUsing memory persistence%s\n", SuccessColor, ResetColor)
		persistence, err := ps.NewMemoryPersistence()
		if err != nil {
			fmt.Printf("%sError: %v%s\n", ErrorColor, err, ResetColor)
			return
		}
		Instance = *CommitDB.Open(&persistence)
	} else {
		fmt.Printf("%sUsing file persistence: %s%s\n", SuccessColor, *baseDir, ResetColor)
		var gitUrlPtr *string
		if *gitUrl != "" {
			gitUrlPtr = gitUrl
		}
		persistence, err := ps.NewFilePersistence(*baseDir, gitUrlPtr)
		if err != nil {
			fmt.Printf("%sError: %v%s\n", ErrorColor, err, ResetColor)
			return
		}
		Instance = *CommitDB.Open(&persistence)
	}

	engine := Instance.Engine(core.Identity{
		Name:  *userName,
		Email: *userEmail,
	})

	cli := &CLI{
		engine:      engine,
		history:     make([]string, 0),
		historyFile: getHistoryPath(),
	}

	cli.loadHistory()

	// Execute SQL file if provided
	if *sqlFile != "" {
		err := cli.importFile(*sqlFile)
		if err != nil {
			fmt.Printf("%sError importing file: %v%s\n", ErrorColor, err, ResetColor)
			os.Exit(1)
		}
		return
	}

	cli.run()
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

		// Add to history
		cli.addToHistory(sql + ";")

		// Execute SQL
		result, err := cli.engine.Execute(sql)
		if err != nil {
			fmt.Printf("%s✗ Error: %v%s\n", ErrorColor, err, ResetColor)
		} else {
			result.Display()
		}
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
		cli.saveHistory()
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

	case ".history":
		cli.printHistory()

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
	fmt.Println("  .history         Show command history")
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

func (cli *CLI) addToHistory(cmd string) {
	// Don't add duplicates of the last command
	if len(cli.history) > 0 && cli.history[len(cli.history)-1] == cmd {
		return
	}
	cli.history = append(cli.history, cmd)

	// Limit history size
	if len(cli.history) > 1000 {
		cli.history = cli.history[len(cli.history)-1000:]
	}
}

func (cli *CLI) printHistory() {
	if len(cli.history) == 0 {
		fmt.Println("No command history")
		return
	}

	start := 0
	if len(cli.history) > 20 {
		start = len(cli.history) - 20
	}

	for i := start; i < len(cli.history); i++ {
		fmt.Printf("  %3d  %s\n", i+1, cli.history[i])
	}
}

func getHistoryPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".commitdb_history")
}

func (cli *CLI) loadHistory() {
	if cli.historyFile == "" {
		return
	}

	file, err := os.Open(cli.historyFile)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		cli.history = append(cli.history, scanner.Text())
	}
}

func (cli *CLI) saveHistory() {
	if cli.historyFile == "" {
		return
	}

	file, err := os.Create(cli.historyFile)
	if err != nil {
		return
	}
	defer file.Close()

	// Save last 1000 entries
	start := 0
	if len(cli.history) > 1000 {
		start = len(cli.history) - 1000
	}

	for i := start; i < len(cli.history); i++ {
		_, _ = file.WriteString(cli.history[i] + "\n")
	}
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
			case db.CommitResult:
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
			case db.QueryResult:
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
