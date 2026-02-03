package core

type ColumnType int

const (
	StringType ColumnType = iota
	IntType
	FloatType
	BoolType
	TextType
	DateType
	TimestampType
	JsonType
)

type Column struct {
	Name       string     `json:"name"`
	Type       ColumnType `json:"type"`
	PrimaryKey bool       `json:"primaryKey"`
}

type Table struct {
	Database string   `json:"database"`
	Name     string   `json:"name"`
	Columns  []Column `json:"columns"`
}
