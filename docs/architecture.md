# Architecture

## Package Structure

```
CommitDB/
├── cmd/
│   └── commitdb/         # Interactive CLI application
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
┌─────────────┐     ┌──────────────┐
│  Go App /   │────▶│    Engine    │
│    CLI      │◀────│  (engine/)   │
└─────────────┘     └──────────────┘
                          │
                          ▼
                    ┌──────────────┐
                    │ Persistence  │
                    │(persistence/)│
                    └──────────────┘
                          │
                          ▼
                    ┌──────────────┐
                    │  Git Repo    │
                    │   (.git/)    │
                    └──────────────┘
```

## Storage Format

See [Storage Format Specification](storage-format.md) for the full file and directory layout.

## Performance Optimizations

### Git Plumbing API

All CRUD operations bypass the Git worktree and shell out to zero external processes. Instead, blobs, trees, and commits are created directly through the Git object store, yielding ~10x faster writes compared to worktree-based operations.

### Batch Tree Updates

Multi-record writes (e.g. `INSERT` with multiple rows, `COPY INTO`) group all changes into a single `batchUpdateTree` call, building one new tree and one commit regardless of row count. This keeps write latency nearly constant as batch size grows.

### Single-Pass Tree Scanning

`ScanDirect` resolves HEAD → commit → root tree once per query, then walks only the target table's subtree to read every record. This eliminates the N+1 object-resolution overhead of reading rows individually.

### O(1) Primary Key Lookups

`WHERE pk = value` queries walk the Git tree directly to the record blob instead of scanning the table, providing constant-time reads without loading an index.
