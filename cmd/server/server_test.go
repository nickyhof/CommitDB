package main

import (
	"bufio"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/nickyhof/CommitDB"
	"github.com/nickyhof/CommitDB/core"
	"github.com/nickyhof/CommitDB/ps"
)

func setupTestServer(t *testing.T) (*Server, func()) {
	persistence, err := ps.NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}
	instance := CommitDB.Open(&persistence)
	identity := core.Identity{Name: "test", Email: "test@test.com"}

	server := NewServer(instance, identity)
	if err := server.Start(":0"); err != nil { // :0 picks a free port
		t.Fatalf("Failed to start server: %v", err)
	}

	return server, func() {
		server.Stop()
	}
}

func sendQuery(t *testing.T, addr, query string) Response {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Send query
	_, err = conn.Write([]byte(query + "\n"))
	if err != nil {
		t.Fatalf("Failed to send query: %v", err)
	}

	// Read response
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	var resp Response
	if err := json.Unmarshal([]byte(line), &resp); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	return resp
}

func TestServerStartStop(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	if server.Addr() == "" {
		t.Error("Expected non-empty address")
	}
}

func TestServerCreateDatabase(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	resp := sendQuery(t, server.Addr(), "CREATE DATABASE testdb")
	if !resp.Success {
		t.Errorf("Expected success, got error: %s", resp.Error)
	}
	if resp.Type != "commit" {
		t.Errorf("Expected commit type, got: %s", resp.Type)
	}

	var cr CommitResponse
	if err := json.Unmarshal(resp.Result, &cr); err != nil {
		t.Fatalf("Failed to parse commit result: %v", err)
	}
	if cr.DatabasesCreated != 1 {
		t.Errorf("Expected 1 database created, got: %d", cr.DatabasesCreated)
	}
}

func TestServerCreateTableAndInsert(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Create database
	resp := sendQuery(t, server.Addr(), "CREATE DATABASE mydb")
	if !resp.Success {
		t.Fatalf("Failed to create database: %s", resp.Error)
	}

	// Create table
	resp = sendQuery(t, server.Addr(), "CREATE TABLE mydb.users (id INT PRIMARY KEY, name STRING)")
	if !resp.Success {
		t.Fatalf("Failed to create table: %s", resp.Error)
	}

	// Insert record
	resp = sendQuery(t, server.Addr(), "INSERT INTO mydb.users (id, name) VALUES (1, 'Alice')")
	if !resp.Success {
		t.Fatalf("Failed to insert: %s", resp.Error)
	}

	var cr CommitResponse
	json.Unmarshal(resp.Result, &cr)
	if cr.RecordsWritten != 1 {
		t.Errorf("Expected 1 record written, got: %d", cr.RecordsWritten)
	}
}

func TestServerSelect(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Setup
	sendQuery(t, server.Addr(), "CREATE DATABASE selectdb")
	sendQuery(t, server.Addr(), "CREATE TABLE selectdb.items (id INT PRIMARY KEY, value STRING)")
	sendQuery(t, server.Addr(), "INSERT INTO selectdb.items (id, value) VALUES (1, 'one')")
	sendQuery(t, server.Addr(), "INSERT INTO selectdb.items (id, value) VALUES (2, 'two')")

	// Query
	resp := sendQuery(t, server.Addr(), "SELECT * FROM selectdb.items")
	if !resp.Success {
		t.Fatalf("Failed to select: %s", resp.Error)
	}
	if resp.Type != "query" {
		t.Errorf("Expected query type, got: %s", resp.Type)
	}

	var qr QueryResponse
	if err := json.Unmarshal(resp.Result, &qr); err != nil {
		t.Fatalf("Failed to parse query result: %v", err)
	}
	if len(qr.Data) != 2 {
		t.Errorf("Expected 2 rows, got: %d", len(qr.Data))
	}
	if qr.RecordsRead != 2 {
		t.Errorf("Expected 2 records read, got: %d", qr.RecordsRead)
	}
}

func TestServerError(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	resp := sendQuery(t, server.Addr(), "SELECT * FROM nonexistent.table")
	if resp.Success {
		t.Error("Expected failure for non-existent table")
	}
	if resp.Error == "" {
		t.Error("Expected error message")
	}
}

func TestServerSyntaxError(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	resp := sendQuery(t, server.Addr(), "SELEKT * FROM foo.bar")
	if resp.Success {
		t.Error("Expected failure for syntax error")
	}
	if resp.Error == "" {
		t.Error("Expected error message")
	}
}

func TestServerPersistentConnection(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Connect once
	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Send multiple queries on same connection
	queries := []string{
		"CREATE DATABASE persistdb",
		"CREATE TABLE persistdb.test (id INT PRIMARY KEY)",
		"INSERT INTO persistdb.test (id) VALUES (1)",
		"SELECT * FROM persistdb.test",
	}

	for _, query := range queries {
		_, err = conn.Write([]byte(query + "\n"))
		if err != nil {
			t.Fatalf("Failed to send query '%s': %v", query, err)
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read response for '%s': %v", query, err)
		}

		var resp Response
		if err := json.Unmarshal([]byte(line), &resp); err != nil {
			t.Fatalf("Failed to parse response for '%s': %v", query, err)
		}

		if !resp.Success {
			t.Errorf("Query '%s' failed: %s", query, resp.Error)
		}
	}
}
