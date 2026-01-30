package core

import "time"

// View represents a SQL view (virtual or materialized)
type View struct {
	Database     string    `json:"database"`
	Name         string    `json:"name"`
	Query        string    `json:"query"`        // The SELECT statement defining the view
	Materialized bool      `json:"materialized"` // True if this is a materialized view
	Columns      []Column  `json:"columns"`      // Inferred column schema
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"` // Last refresh time for materialized views
}
