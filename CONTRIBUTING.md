# Contributing to CommitDB

Thank you for your interest in contributing to CommitDB!

## Development Setup

```bash
git clone https://github.com/nickyhof/CommitDB.git
cd CommitDB
make deps
make test
```

**Requirements:** Go 1.23+ and [golangci-lint](https://golangci-lint.run/welcome/install/) v2.

## Making Changes

1. Create a branch from `main`
2. Make your changes
3. Ensure all checks pass:
   ```bash
   make test       # All tests pass
   make lint       # Zero lint issues
   make build      # CLI builds
   ```
4. Update `CHANGELOG.md` under the `[Unreleased]` section
5. Open a pull request

## Code Standards

- **Linting**: `golangci-lint run ./...` must produce zero issues — no suppressions
- **Naming**: Follow Go conventions (`ID` not `Id`, `URL` not `Url`, `JSON` not `Json`)
- **Receivers**: Use short, consistent receiver names (e.g., `p` for `*Persistence`, `e` for `*Engine`)
- **Comments**: Exported types and functions must have `// TypeName ...` doc comments
- **Package docs**: One `doc.go` per package with `// Package name ...` comment

## Testing

- Add tests for new features in the appropriate `_test.go` file
- Integration tests go in `tests/integration_test.go`
- Run benchmarks with `make bench` when performance is relevant
- Use both memory and file persistence modes where applicable

## Project Structure

```
cmd/commitdb/       CLI entry point
core/               Shared types (Identity, ColumnType, etc.)
engine/             SQL execution engine
internal/sql/       Lexer and parser
internal/ops/       Row-level operations
internal/compare/   Value comparison
persistence/        Git-backed storage layer
tests/              Integration and benchmark tests
```

## Reporting Issues

Open an issue on GitHub with:
- What you expected vs what happened
- Steps to reproduce
- Go version and OS
