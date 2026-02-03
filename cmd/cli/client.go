package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"
)

// ServerClient provides a TCP client for connecting to a CommitDB server.
type ServerClient struct {
	addr      string
	conn      net.Conn
	reader    *bufio.Reader
	tlsConfig *tls.Config
	token     string
}

// NewServerClient creates a new server client.
func NewServerClient(addr string, useTLS bool, token string) *ServerClient {
	client := &ServerClient{
		addr:  addr,
		token: token,
	}
	if useTLS {
		client.tlsConfig = &tls.Config{
			InsecureSkipVerify: false,
		}
	}
	return client
}

// Connect establishes a connection to the server.
func (c *ServerClient) Connect() error {
	var conn net.Conn
	var err error

	if c.tlsConfig != nil {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", c.addr, c.tlsConfig)
	} else {
		conn, err = net.DialTimeout("tcp", c.addr, 10*time.Second)
	}
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", c.addr, err)
	}

	c.conn = conn
	c.reader = bufio.NewReader(conn)

	// Authenticate if token provided
	if c.token != "" {
		resp, err := c.sendQuery("AUTH JWT " + c.token)
		if err != nil {
			c.conn.Close()
			return fmt.Errorf("authentication failed: %w", err)
		}
		if !resp.Success {
			c.conn.Close()
			return fmt.Errorf("authentication failed: %s", resp.Error)
		}
	}

	return nil
}

// Close closes the connection.
func (c *ServerClient) Close() error {
	if c.conn != nil {
		c.conn.Write([]byte("quit\n"))
		return c.conn.Close()
	}
	return nil
}

// Response types matching server protocol
type serverResponse struct {
	Success bool            `json:"success"`
	Error   string          `json:"error,omitempty"`
	Type    string          `json:"type,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
}

type queryResult struct {
	Columns         []string   `json:"columns"`
	Data            [][]string `json:"data"`
	RecordsRead     int        `json:"records_read"`
	ExecutionTimeMs float64    `json:"execution_time_ms"`
	ExecutionOps    int        `json:"execution_ops"`
}

type commitResult struct {
	DatabasesCreated int     `json:"databases_created,omitempty"`
	DatabasesDeleted int     `json:"databases_deleted,omitempty"`
	TablesCreated    int     `json:"tables_created,omitempty"`
	TablesDeleted    int     `json:"tables_deleted,omitempty"`
	RecordsWritten   int     `json:"records_written,omitempty"`
	RecordsDeleted   int     `json:"records_deleted,omitempty"`
	ExecutionTimeMs  float64 `json:"execution_time_ms"`
	ExecutionOps     int     `json:"execution_ops"`
}

// Execute sends a query to the server and returns the result.
func (c *ServerClient) Execute(sql string) (Result, error) {
	resp, err := c.sendQuery(sql)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("%s", resp.Error)
	}

	// Parse the result based on type
	switch resp.Type {
	case "query":
		var qr queryResult
		if err := json.Unmarshal(resp.Result, &qr); err != nil {
			return nil, fmt.Errorf("failed to parse query result: %w", err)
		}
		return &remoteQueryResult{qr}, nil

	case "commit":
		var cr commitResult
		if err := json.Unmarshal(resp.Result, &cr); err != nil {
			return nil, fmt.Errorf("failed to parse commit result: %w", err)
		}
		return &remoteCommitResult{cr}, nil

	default:
		return &remoteOKResult{}, nil
	}
}

func (c *ServerClient) sendQuery(query string) (*serverResponse, error) {
	// Send query with newline
	_, err := c.conn.Write([]byte(query + "\n"))
	if err != nil {
		return nil, fmt.Errorf("failed to send query: %w", err)
	}

	// Read response line
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Parse JSON response
	var resp serverResponse
	if err := json.Unmarshal([]byte(line), &resp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &resp, nil
}

// Result interface for displayable results
type Result interface {
	Display()
}

// remoteQueryResult wraps a query result from the server
type remoteQueryResult struct {
	data queryResult
}

func (r *remoteQueryResult) Display() {
	if len(r.data.Columns) == 0 {
		fmt.Printf("%s✓ Query executed (no results)%s\n", SuccessColor, ResetColor)
		return
	}

	// Calculate column widths
	widths := make([]int, len(r.data.Columns))
	for i, col := range r.data.Columns {
		widths[i] = len(col)
	}
	for _, row := range r.data.Data {
		for i, val := range row {
			if i < len(widths) && len(val) > widths[i] {
				widths[i] = len(val)
			}
		}
	}

	// Print header
	printRow(r.data.Columns, widths)
	printSeparator(widths)

	// Print rows
	for _, row := range r.data.Data {
		printRow(row, widths)
	}

	// Print footer
	fmt.Printf("%s✓ %d rows (%.2fms)%s\n", SuccessColor, r.data.RecordsRead, r.data.ExecutionTimeMs, ResetColor)
}

// remoteCommitResult wraps a commit result from the server
type remoteCommitResult struct {
	data commitResult
}

func (r *remoteCommitResult) Display() {
	var parts []string

	if r.data.DatabasesCreated > 0 {
		parts = append(parts, fmt.Sprintf("%d db created", r.data.DatabasesCreated))
	}
	if r.data.DatabasesDeleted > 0 {
		parts = append(parts, fmt.Sprintf("%d db deleted", r.data.DatabasesDeleted))
	}
	if r.data.TablesCreated > 0 {
		parts = append(parts, fmt.Sprintf("%d table created", r.data.TablesCreated))
	}
	if r.data.TablesDeleted > 0 {
		parts = append(parts, fmt.Sprintf("%d table deleted", r.data.TablesDeleted))
	}
	if r.data.RecordsWritten > 0 {
		parts = append(parts, fmt.Sprintf("%d written", r.data.RecordsWritten))
	}
	if r.data.RecordsDeleted > 0 {
		parts = append(parts, fmt.Sprintf("%d deleted", r.data.RecordsDeleted))
	}

	msg := "OK"
	if len(parts) > 0 {
		msg = strings.Join(parts, ", ")
	}

	fmt.Printf("%s✓ %s (%.2fms)%s\n", SuccessColor, msg, r.data.ExecutionTimeMs, ResetColor)
}

// remoteOKResult for commands that don't return data
type remoteOKResult struct{}

func (r *remoteOKResult) Display() {
	fmt.Printf("%s✓ OK%s\n", SuccessColor, ResetColor)
}

// Helper functions for table printing
func printRow(values []string, widths []int) {
	fmt.Print("│")
	for i, val := range values {
		if i < len(widths) {
			fmt.Printf(" %-*s │", widths[i], val)
		}
	}
	fmt.Println()
}

func printSeparator(widths []int) {
	fmt.Print("├")
	for i, w := range widths {
		fmt.Print(strings.Repeat("─", w+2))
		if i < len(widths)-1 {
			fmt.Print("┼")
		}
	}
	fmt.Println("┤")
}
