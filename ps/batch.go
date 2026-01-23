package ps

import (
	"fmt"
	"time"

	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/nickyhof/CommitDB/core"
)

// Operation represents a single write operation in a transaction
type Operation struct {
	Type     OperationType
	Database string
	Table    string
	Key      string
	Data     []byte
}

type OperationType int

const (
	WriteOp OperationType = iota
	DeleteOp
)

// TransactionBuilder allows batching multiple write operations into a single commit
type TransactionBuilder struct {
	persistence *Persistence
	operations  []Operation
	started     bool
}

// BeginTransaction creates a new transaction builder for batching operations
func (persistence *Persistence) BeginTransaction() (*TransactionBuilder, error) {
	if err := persistence.ensureInitialized(); err != nil {
		return nil, err
	}

	return &TransactionBuilder{
		persistence: persistence,
		operations:  make([]Operation, 0),
		started:     true,
	}, nil
}

// AddWrite adds a write operation to the transaction batch
func (tb *TransactionBuilder) AddWrite(database, table, key string, data []byte) error {
	if !tb.started {
		return fmt.Errorf("transaction not started")
	}

	tb.operations = append(tb.operations, Operation{
		Type:     WriteOp,
		Database: database,
		Table:    table,
		Key:      key,
		Data:     data,
	})

	return nil
}

// AddDelete adds a delete operation to the transaction batch
func (tb *TransactionBuilder) AddDelete(database, table, key string) error {
	if !tb.started {
		return fmt.Errorf("transaction not started")
	}

	tb.operations = append(tb.operations, Operation{
		Type:     DeleteOp,
		Database: database,
		Table:    table,
		Key:      key,
	})

	return nil
}

// Commit applies all batched operations in a single git commit
func (tb *TransactionBuilder) Commit(identity core.Identity) (Transaction, error) {
	if !tb.started {
		return Transaction{}, fmt.Errorf("transaction not started")
	}

	if len(tb.operations) == 0 {
		return Transaction{}, fmt.Errorf("no operations to commit")
	}

	wt, err := tb.persistence.repo.Worktree()
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to get worktree: %w", err)
	}

	// Apply all operations
	for _, op := range tb.operations {
		path := fmt.Sprintf("%s/%s/%s", op.Database, op.Table, op.Key)

		switch op.Type {
		case WriteOp:
			if err := util.WriteFile(wt.Filesystem, path, op.Data, 0o644); err != nil {
				return Transaction{}, fmt.Errorf("failed to write %s: %w", path, err)
			}
			if _, err := wt.Add(path); err != nil {
				return Transaction{}, fmt.Errorf("failed to stage %s: %w", path, err)
			}
		case DeleteOp:
			wt.Remove(path)
		}
	}

	// Create single commit for all operations
	message := fmt.Sprintf("Batch transaction: %d operation(s)", len(tb.operations))
	commit, err := wt.Commit(message, &git.CommitOptions{
		Author: &object.Signature{
			Name:  identity.Name,
			Email: identity.Email,
			When:  time.Now(),
		},
	})
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to commit: %w", err)
	}

	obj, err := tb.persistence.repo.CommitObject(commit)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to get commit object: %w", err)
	}

	// Mark transaction as completed
	tb.started = false
	tb.operations = nil

	return Transaction{
		Id:   obj.Hash.String(),
		When: obj.Committer.When,
	}, nil
}

// Rollback discards all batched operations without committing
func (tb *TransactionBuilder) Rollback() {
	tb.started = false
	tb.operations = nil
}

// OperationCount returns the number of pending operations
func (tb *TransactionBuilder) OperationCount() int {
	return len(tb.operations)
}
