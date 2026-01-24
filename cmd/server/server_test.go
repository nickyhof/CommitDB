package main

import (
	"bufio"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
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

// setupAuthTestServer creates a server with authentication enabled
func setupAuthTestServer(t *testing.T, secret string) (*Server, func()) {
	persistence, err := ps.NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}
	instance := CommitDB.Open(&persistence)

	authConfig := &AuthConfig{
		Enabled:   true,
		JWTSecret: secret,
	}

	server := NewServerWithAuth(instance, authConfig)
	if err := server.Start(":0"); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	return server, func() {
		server.Stop()
	}
}

func TestAuthRequired(t *testing.T) {
	server, cleanup := setupAuthTestServer(t, "test-secret")
	defer cleanup()

	// Try to query without authenticating
	resp := sendQuery(t, server.Addr(), "CREATE DATABASE testdb")
	if resp.Success {
		t.Error("Expected failure when not authenticated")
	}
	if !containsString(resp.Error, "authentication required") {
		t.Errorf("Expected 'authentication required' error, got: %s", resp.Error)
	}
}

func TestAuthWithValidJWT(t *testing.T) {
	secret := "test-secret"
	server, cleanup := setupAuthTestServer(t, secret)
	defer cleanup()

	// Create a valid JWT token
	token := createTestJWT(t, secret, "Test User", "test@example.com")

	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Send AUTH command
	_, err = conn.Write([]byte("AUTH JWT " + token + "\n"))
	if err != nil {
		t.Fatalf("Failed to send auth: %v", err)
	}

	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read auth response: %v", err)
	}

	var resp Response
	if err := json.Unmarshal([]byte(line), &resp); err != nil {
		t.Fatalf("Failed to parse auth response: %v", err)
	}

	if !resp.Success {
		t.Errorf("Auth failed: %s", resp.Error)
	}
	if resp.Type != "auth" {
		t.Errorf("Expected 'auth' type, got: %s", resp.Type)
	}

	// Parse auth response
	var authResp AuthResponse
	if err := json.Unmarshal(resp.Result, &authResp); err != nil {
		t.Fatalf("Failed to parse auth result: %v", err)
	}
	if !authResp.Authenticated {
		t.Error("Expected authenticated to be true")
	}
	if authResp.Identity != "Test User <test@example.com>" {
		t.Errorf("Expected identity 'Test User <test@example.com>', got: %s", authResp.Identity)
	}

	// Now query should work
	_, err = conn.Write([]byte("CREATE DATABASE authtest\n"))
	if err != nil {
		t.Fatalf("Failed to send query: %v", err)
	}

	line, err = reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read query response: %v", err)
	}

	if err := json.Unmarshal([]byte(line), &resp); err != nil {
		t.Fatalf("Failed to parse query response: %v", err)
	}

	if !resp.Success {
		t.Errorf("Query after auth failed: %s", resp.Error)
	}
}

func TestAuthWithInvalidJWT(t *testing.T) {
	server, cleanup := setupAuthTestServer(t, "test-secret")
	defer cleanup()

	// Create token with wrong secret
	wrongToken := createTestJWT(t, "wrong-secret", "Test User", "test@example.com")

	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Send AUTH command with invalid token
	_, err = conn.Write([]byte("AUTH JWT " + wrongToken + "\n"))
	if err != nil {
		t.Fatalf("Failed to send auth: %v", err)
	}

	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read auth response: %v", err)
	}

	var resp Response
	if err := json.Unmarshal([]byte(line), &resp); err != nil {
		t.Fatalf("Failed to parse auth response: %v", err)
	}

	if resp.Success {
		t.Error("Expected auth to fail with wrong secret")
	}
	if resp.Error == "" {
		t.Error("Expected error message")
	}
}

// createTestJWT creates a JWT token for testing
func createTestJWT(t *testing.T, secret, name, email string) string {
	// Using github.com/golang-jwt/jwt/v5
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"name":  name,
		"email": email,
		"exp":   time.Now().Add(time.Hour).Unix(),
	})

	tokenString, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("Failed to create test JWT: %v", err)
	}
	return tokenString
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsString(s[1:], substr) || s[:len(substr)] == substr)
}

// TestIdentityInCommitsUnauthenticated verifies the default identity is used in Git commits
// when auth is disabled
func TestIdentityInCommitsUnauthenticated(t *testing.T) {
	persistence, err := ps.NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}
	instance := CommitDB.Open(&persistence)
	defaultIdentity := core.Identity{Name: "Default User", Email: "default@test.com"}

	server := NewServer(instance, defaultIdentity)
	if err := server.Start(":0"); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	// Execute a mutation query
	resp := sendQuery(t, server.Addr(), "CREATE DATABASE testdb_identity1")
	if !resp.Success {
		t.Fatalf("Query failed: %s", resp.Error)
	}

	// Check the commit author
	txn := persistence.LatestTransaction()
	expectedAuthor := "Default User <default@test.com>"
	if txn.Author != expectedAuthor {
		t.Errorf("Expected commit author '%s', got '%s'", expectedAuthor, txn.Author)
	}
}

// TestIdentityInCommitsAuthenticated verifies the JWT identity is used in Git commits
func TestIdentityInCommitsAuthenticated(t *testing.T) {
	secret := "test-secret-for-identity"

	persistence, err := ps.NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}
	instance := CommitDB.Open(&persistence)

	authConfig := &AuthConfig{
		Enabled:   true,
		JWTSecret: secret,
	}
	server := NewServerWithAuth(instance, authConfig)
	if err := server.Start(":0"); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	// Create JWT with specific identity
	jwtName := "JWT Test User"
	jwtEmail := "jwtuser@example.com"
	token := createTestJWT(t, secret, jwtName, jwtEmail)

	conn, err := net.DialTimeout("tcp", server.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Authenticate
	_, err = conn.Write([]byte("AUTH JWT " + token + "\n"))
	if err != nil {
		t.Fatalf("Failed to send auth: %v", err)
	}
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read auth response: %v", err)
	}
	var authResp Response
	json.Unmarshal([]byte(line), &authResp)
	if !authResp.Success {
		t.Fatalf("Auth failed: %s", authResp.Error)
	}

	// Execute a mutation query
	_, err = conn.Write([]byte("CREATE DATABASE testdb_identity2\n"))
	if err != nil {
		t.Fatalf("Failed to send query: %v", err)
	}
	line, err = reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}
	var resp Response
	json.Unmarshal([]byte(line), &resp)
	if !resp.Success {
		t.Fatalf("Query failed: %s", resp.Error)
	}

	// Check the commit author matches JWT identity
	txn := persistence.LatestTransaction()
	expectedAuthor := jwtName + " <" + jwtEmail + ">"
	if txn.Author != expectedAuthor {
		t.Errorf("Expected commit author '%s', got '%s'", expectedAuthor, txn.Author)
	}
}

// === TLS Tests ===

// setupTLSTestServer creates a server with TLS enabled using test certificates
func setupTLSTestServer(t *testing.T) (*Server, string, string, func()) {
	t.Helper()

	// Create temporary directory for test certificates
	tmpDir := t.TempDir()
	certFile := tmpDir + "/cert.pem"
	keyFile := tmpDir + "/key.pem"

	// Generate self-signed test certificate
	generateTestCertificate(t, certFile, keyFile)

	persistence, err := ps.NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}
	instance := CommitDB.Open(&persistence)
	identity := core.Identity{Name: "test", Email: "test@test.com"}

	server := NewServer(instance, identity)
	if err := server.StartTLS(":0", certFile, keyFile); err != nil {
		t.Fatalf("Failed to start TLS server: %v", err)
	}

	return server, certFile, keyFile, func() {
		server.Stop()
	}
}

// generateTestCertificate creates a self-signed certificate for testing
func generateTestCertificate(t *testing.T, certFile, keyFile string) {
	t.Helper()

	// Generate a private key
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(time.Hour),
		KeyUsage:  x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
		},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
		DNSNames:    []string{"localhost"},
	}

	// Create self-signed certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("Failed to create certificate: %v", err)
	}

	// Write certificate to file
	certOut, err := os.Create(certFile)
	if err != nil {
		t.Fatalf("Failed to create cert file: %v", err)
	}
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	certOut.Close()

	// Write private key to file
	keyOut, err := os.Create(keyFile)
	if err != nil {
		t.Fatalf("Failed to create key file: %v", err)
	}
	pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	keyOut.Close()
}

func TestTLSServerStartStop(t *testing.T) {
	server, _, _, cleanup := setupTLSTestServer(t)
	defer cleanup()

	if server.Addr() == "" {
		t.Error("Expected non-empty address")
	}
	if !server.TLSEnabled() {
		t.Error("Expected TLS to be enabled")
	}
}

func TestTLSServerConnection(t *testing.T) {
	server, certFile, _, cleanup := setupTLSTestServer(t)
	defer cleanup()

	// Load certificate for client
	certPool := x509.NewCertPool()
	certData, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatalf("Failed to read cert: %v", err)
	}
	certPool.AppendCertsFromPEM(certData)

	// Connect with TLS
	tlsConfig := &tls.Config{
		RootCAs:    certPool,
		ServerName: "localhost",
	}

	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", server.Addr(), tlsConfig)
	if err != nil {
		t.Fatalf("Failed to connect with TLS: %v", err)
	}
	defer conn.Close()

	// Send a query
	_, err = conn.Write([]byte("CREATE DATABASE tlstest\n"))
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

	if !resp.Success {
		t.Errorf("Query failed: %s", resp.Error)
	}
	if resp.Type != "commit" {
		t.Errorf("Expected commit type, got: %s", resp.Type)
	}
}

func TestTLSServerInvalidCert(t *testing.T) {
	server, _, _, cleanup := setupTLSTestServer(t)
	defer cleanup()

	// Try to connect without proper certificate verification
	// This should fail because we're not providing the right CA
	tlsConfig := &tls.Config{
		ServerName: "localhost",
		// Empty RootCAs - will use system CAs which won't include our self-signed cert
	}

	_, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", server.Addr(), tlsConfig)
	if err == nil {
		t.Error("Expected TLS connection to fail with invalid certificate")
	}
}

func TestTLSServerWithInsecureSkipVerify(t *testing.T) {
	server, _, _, cleanup := setupTLSTestServer(t)
	defer cleanup()

	// Connect with InsecureSkipVerify (dev mode)
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 2 * time.Second}, "tcp", server.Addr(), tlsConfig)
	if err != nil {
		t.Fatalf("Failed to connect with TLS (insecure): %v", err)
	}
	defer conn.Close()

	// Send a simple query
	_, err = conn.Write([]byte("SHOW DATABASES\n"))
	if err != nil {
		t.Fatalf("Failed to send query: %v", err)
	}

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	var resp Response
	if err := json.Unmarshal([]byte(line), &resp); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if !resp.Success {
		t.Errorf("Query failed: %s", resp.Error)
	}
}
