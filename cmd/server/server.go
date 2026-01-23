package main

import (
	"bufio"
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
	listener net.Listener
	instance *CommitDB.Instance
	identity core.Identity
	mu       sync.Mutex
	engine   *db.Engine
	done     chan struct{}
	wg       sync.WaitGroup
}

// NewServer creates a new SQL server with the given CommitDB instance.
func NewServer(instance *CommitDB.Instance, identity core.Identity) *Server {
	return &Server{
		instance: instance,
		identity: identity,
		engine:   instance.Engine(identity),
		done:     make(chan struct{}),
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

	go s.acceptLoop()
	return nil
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

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	log.Printf("Client connected: %s", conn.RemoteAddr())

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

		// Execute query
		response := s.executeQuery(query)

		// Send response
		data, err := EncodeResponse(response)
		if err != nil {
			log.Printf("Failed to encode response: %v", err)
			continue
		}

		_, err = conn.Write(data)
		if err != nil {
			log.Printf("Write error to %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}

func (s *Server) executeQuery(query string) Response {
	s.mu.Lock()
	defer s.mu.Unlock()

	result, err := s.engine.Execute(query)
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
