// Package main provides a TCP SQL server for CommitDB.
package main

import (
	"encoding/json"
)

// Request represents a SQL query from the client.
type Request struct {
	Query string `json:"query"`
}

// Response represents the server's response to a query.
type Response struct {
	Success bool            `json:"success"`
	Error   string          `json:"error,omitempty"`
	Type    string          `json:"type,omitempty"` // "query" or "commit"
	Result  json.RawMessage `json:"result,omitempty"`
}

// QueryResponse contains tabular query results.
type QueryResponse struct {
	Columns     []string   `json:"columns"`
	Data        [][]string `json:"data"`
	RecordsRead int        `json:"records_read"`
	TimeMs      float64    `json:"time_ms"`
}

// CommitResponse contains mutation operation results.
type CommitResponse struct {
	DatabasesCreated int     `json:"databases_created,omitempty"`
	DatabasesDeleted int     `json:"databases_deleted,omitempty"`
	TablesCreated    int     `json:"tables_created,omitempty"`
	TablesDeleted    int     `json:"tables_deleted,omitempty"`
	RecordsWritten   int     `json:"records_written,omitempty"`
	RecordsDeleted   int     `json:"records_deleted,omitempty"`
	TimeMs           float64 `json:"time_ms"`
}

// EncodeResponse serializes a Response to JSON with a newline.
func EncodeResponse(resp Response) ([]byte, error) {
	data, err := json.Marshal(resp)
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

// DecodeRequest parses a JSON request from a byte slice.
func DecodeRequest(data []byte) (Request, error) {
	var req Request
	err := json.Unmarshal(data, &req)
	return req, err
}
