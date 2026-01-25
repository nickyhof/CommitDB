package ps

import (
	"fmt"

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

// Commit applies all batched operations in a single git commit using plumbing API
// Uses batch tree update for efficient multi-operation commits
func (tb *TransactionBuilder) Commit(identity core.Identity) (Transaction, error) {
	if !tb.started {
		return Transaction{}, fmt.Errorf("transaction not started")
	}

	if len(tb.operations) == 0 {
		return Transaction{}, fmt.Errorf("no operations to commit")
	}

	// Get current tree
	currentTree, err := tb.persistence.getCurrentTree()
	if err != nil {
		return Transaction{}, err
	}

	// Build list of changes
	changes := make([]TreeChange, 0, len(tb.operations))
	for _, op := range tb.operations {
		opPath := fmt.Sprintf("%s/%s/%s", op.Database, op.Table, op.Key)

		switch op.Type {
		case WriteOp:
			blobHash, err := tb.persistence.createBlob(op.Data)
			if err != nil {
				return Transaction{}, fmt.Errorf("failed to create blob for %s: %w", opPath, err)
			}
			changes = append(changes, TreeChange{
				Path:     opPath,
				BlobHash: blobHash,
				IsDelete: false,
			})
		case DeleteOp:
			changes = append(changes, TreeChange{
				Path:     opPath,
				IsDelete: true,
			})
		}
	}

	// Apply all changes in single tree operation
	newTree, err := tb.persistence.batchUpdateTree(currentTree, changes)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to update tree: %w", err)
	}

	// Create single commit for all operations
	message := fmt.Sprintf("Batch transaction: %d operation(s)", len(tb.operations))
	txn, err := tb.persistence.createCommitDirect(newTree, identity, message)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to commit: %w", err)
	}

	// Sync worktree
	if err := tb.persistence.syncWorktree(); err != nil {
		return Transaction{}, fmt.Errorf("failed to sync worktree: %w", err)
	}

	// Mark transaction as completed
	tb.started = false
	tb.operations = nil

	return txn, nil
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
