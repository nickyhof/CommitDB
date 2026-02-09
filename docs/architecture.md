# Architecture

## Package Structure

```
CommitDB/
├── cmd/
│   └── commitdb/          # Interactive CLI application
├── core/                 # Public: Domain types (Identity)
├── engine/               # Public: SQL execution engine
│   ├── engine.go         # Core router
│   ├── select.go         # SELECT, aggregates, functions
│   ├── dml.go            # INSERT, UPDATE, DELETE
│   ├── ddl.go            # CREATE/DROP TABLE/DB
│   ├── branch.go         # Branching/merge
│   └── view.go           # Views, time-travel
├── persistence/          # Public: Git-backed storage
├── internal/
│   ├── sql/              # SQL parser (internal)
│   ├── ops/              # Table operations (internal)
│   └── compare/          # Value comparison (internal)
├── tests/                # Integration tests
└── docs/                 # Documentation
```

## Components

### Engine (`engine/`)

The SQL engine handles:

- Query parsing (SQL → AST)
- Query planning
- Execution against storage
- Result formatting

### Persistence (`persistence/`)

Git-backed storage with:

- Tables stored as JSON files
- Each transaction = Git commit
- Branches for isolation
- Tags for snapshots

## Data Flow

```
┌─────────────┐     ┌─────────────┐
│  Go App /   │────▶│   Engine    │
│    CLI      │◀────│   (op/)     │
└─────────────┘     └─────────────┘
                          │
                          ▼
                    ┌─────────────┐
                    │ Persistence │
                    │    (ps/)    │
                    └─────────────┘
                          │
                          ▼
                    ┌─────────────┐
                    │ Git Repo    │
                    │ (.git/)     │
                    └─────────────┘
```

## Git Storage Format

Each database is a directory:

```
data/
└── mydb/               # Database
    ├── users.json      # Table: array of records
    ├── orders.json     
    └── .indexes/       # Index files
        └── idx_email.json
```

Table format:

```json
{
  "schema": {
    "columns": [
      {"name": "id", "type": "INT", "primaryKey": true},
      {"name": "name", "type": "STRING"}
    ]
  },
  "records": [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"}
  ]
}
```

## Performance Optimizations

### Git Plumbing API (v2.0.0)

Bypasses high-level Git commands for ~10x faster writes:

- Direct blob/tree creation
- Batch commits
- Memory-mapped file access
