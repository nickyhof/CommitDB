package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/nickyhof/CommitDB"
	"github.com/nickyhof/CommitDB/core"
	"github.com/nickyhof/CommitDB/db"
)

// Server is a TCP SQL server that exposes the CommitDB engine.
type Server struct {
	listener        net.Listener
	instance        *CommitDB.Instance
	defaultIdentity core.Identity
	authConfig      *AuthConfig
	tlsEnabled      bool
	done            chan struct{}
	wg              sync.WaitGroup
}

// NewServer creates a new SQL server with the given CommitDB instance.
// The defaultIdentity is used when auth is disabled or for anonymous connections.
func NewServer(instance *CommitDB.Instance, identity core.Identity) *Server {
	return &Server{
		instance:        instance,
		defaultIdentity: identity,
		done:            make(chan struct{}),
	}
}

// NewServerWithAuth creates a new SQL server with authentication enabled.
func NewServerWithAuth(instance *CommitDB.Instance, authConfig *AuthConfig) *Server {
	return &Server{
		instance:   instance,
		authConfig: authConfig,
		done:       make(chan struct{}),
	}
}

// Start begins listening for connections on the specified address.
func (s *Server) Start(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}
	s.listener = listener

	log.Printf("SQL Server listening on %s", addr)
	if s.authConfig != nil && s.authConfig.Enabled {
		log.Printf("Authentication: enabled (JWT)")
	} else {
		log.Printf("Authentication: disabled (using default identity)")
	}

	go s.acceptLoop()
	return nil
}

// StartTLS begins listening for TLS-encrypted connections on the specified address.
func (s *Server) StartTLS(addr, certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("failed to load TLS certificate: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	listener, err := tls.Listen("tcp", addr, tlsConfig)
	if err != nil {
		return fmt.Errorf("failed to start TLS server: %w", err)
	}
	s.listener = listener
	s.tlsEnabled = true

	log.Printf("SQL Server listening on %s (TLS enabled)", addr)
	if s.authConfig != nil && s.authConfig.Enabled {
		log.Printf("Authentication: enabled (JWT)")
	} else {
		log.Printf("Authentication: disabled (using default identity)")
	}

	go s.acceptLoop()
	return nil
}

// TLSEnabled returns whether the server is using TLS.
func (s *Server) TLSEnabled() bool {
	return s.tlsEnabled
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() error {
	close(s.done)
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
	return nil
}

// Addr returns the server's listening address.
func (s *Server) Addr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.done:
				return
			default:
				log.Printf("Accept error: %v", err)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// connContext holds per-connection state including auth and engine.
type connContext struct {
	state  *ConnectionState
	engine *db.Engine
	mu     sync.Mutex
}

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	log.Printf("Client connected: %s", conn.RemoteAddr())

	// Initialize connection context
	ctx := &connContext{
		state: &ConnectionState{},
	}

	// If auth is not enabled, pre-authenticate with default identity
	if s.authConfig == nil || !s.authConfig.Enabled {
		ctx.state.identity = &s.defaultIdentity
		ctx.state.authenticated = true
		ctx.engine = s.instance.Engine(s.defaultIdentity)
	}

	reader := bufio.NewReader(conn)

	for {
		select {
		case <-s.done:
			return
		default:
		}

		// Read until newline (one query per line)
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Read error from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}

		query := strings.TrimSpace(line)
		if query == "" {
			continue
		}

		// Handle special commands
		if strings.ToLower(query) == "quit" || strings.ToLower(query) == "exit" {
			log.Printf("Client disconnected: %s", conn.RemoteAddr())
			return
		}

		// Check for AUTH command
		if strings.HasPrefix(strings.ToUpper(query), "AUTH ") {
			response := s.handleAuthCommand(query, ctx)
			s.sendResponse(conn, response)
			continue
		}

		// Check if authenticated
		if !ctx.state.authenticated {
			response := Response{
				Success: false,
				Error:   "authentication required: send AUTH JWT <token>",
			}
			s.sendResponse(conn, response)
			continue
		}

		// Execute query with connection's engine
		response := s.executeQueryWithEngine(query, ctx.engine)
		s.sendResponse(conn, response)
	}
}

// handleAuthCommand processes AUTH commands
func (s *Server) handleAuthCommand(query string, ctx *connContext) Response {
	// If auth is not configured, allow simple identity declaration
	if s.authConfig == nil || !s.authConfig.Enabled {
		return Response{
			Success: false,
			Error:   "authentication not enabled on this server",
		}
	}

	response := s.handleAuth(query, ctx.state)

	// If auth succeeded, create engine with new identity
	if response.Success && ctx.state.identity != nil {
		ctx.mu.Lock()
		ctx.engine = s.instance.Engine(*ctx.state.identity)
		ctx.mu.Unlock()
	}

	return response
}

func (s *Server) sendResponse(conn net.Conn, response Response) {
	data, err := EncodeResponse(response)
	if err != nil {
		log.Printf("Failed to encode response: %v", err)
		return
	}

	_, err = conn.Write(data)
	if err != nil {
		log.Printf("Write error to %s: %v", conn.RemoteAddr(), err)
	}
}

func (s *Server) executeQueryWithEngine(query string, engine *db.Engine) Response {
	result, err := engine.Execute(query)
	if err != nil {
		return Response{
			Success: false,
			Error:   err.Error(),
		}
	}

	switch r := result.(type) {
	case db.QueryResult:
		qr := QueryResponse{
			Columns:     r.Columns,
			Data:        r.Data,
			RecordsRead: r.RecordsRead,
			TimeMs:      r.ExecutionTimeSec * 1000,
		}
		data, _ := json.Marshal(qr)
		return Response{
			Success: true,
			Type:    "query",
			Result:  data,
		}

	case db.CommitResult:
		cr := CommitResponse{
			DatabasesCreated: r.DatabasesCreated,
			DatabasesDeleted: r.DatabasesDeleted,
			TablesCreated:    r.TablesCreated,
			TablesDeleted:    r.TablesDeleted,
			RecordsWritten:   r.RecordsWritten,
			RecordsDeleted:   r.RecordsDeleted,
			TimeMs:           r.ExecutionTimeSec * 1000,
		}
		data, _ := json.Marshal(cr)
		return Response{
			Success: true,
			Type:    "commit",
			Result:  data,
		}

	default:
		return Response{
			Success: true,
			Type:    "unknown",
		}
	}
}
