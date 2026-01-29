# Architecture

## Package Structure

```
CommitDB/
├── cmd/
│   ├── cli/          # Interactive CLI application
│   └── server/       # TCP server
├── op/               # SQL operations engine
│   ├── engine.go     # Main query executor
│   ├── parser.go     # SQL parser
│   └── planner.go    # Query planner
├── ps/               # Persistence layer
│   ├── persistence.go  # Git-backed storage
│   ├── plumbing.go     # Low-level Git operations
│   └── batch.go        # Batch commit operations
├── bindings/         # C shared library bindings
├── clients/
│   └── python/       # Python client
├── tests/            # Tests and benchmarks
└── scripts/          # Build and CI scripts
```

## Components

### Engine (`op/`)

The SQL engine handles:

- Query parsing (SQL → AST)
- Query planning
- Execution against storage
- Result formatting

### Persistence (`ps/`)

Git-backed storage with:

- Tables stored as JSON files
- Each transaction = Git commit
- Branches for isolation
- Tags for snapshots

### Server (`cmd/server/`)

TCP server supporting:

- JSON protocol
- TLS encryption
- JWT authentication
- Connection management

## Data Flow

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Client    │────▶│   Server    │────▶│   Engine    │
│ (Python/Go) │◀────│  (TCP/TLS)  │◀────│   (op/)     │
└─────────────┘     └─────────────┘     └─────────────┘
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
