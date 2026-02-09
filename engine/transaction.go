package engine

import (
	"time"
)

func (engine *Engine) executeBeginStatement() (CommitResult, error) {
	startTime := time.Now()

	// Create a new transaction builder
	_, err := engine.Persistence.BeginTransaction()
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    1,
	}, nil
}

func (engine *Engine) executeCommitStatement() (CommitResult, error) {
	startTime := time.Now()

	// Note: In a full implementation, we'd track the current transaction and commit it
	// For now, this is a no-op since each statement auto-commits

	return CommitResult{
		Transaction:     engine.Persistence.LatestTransaction(),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    1,
	}, nil
}

func (engine *Engine) executeRollbackStatement() (CommitResult, error) {
	startTime := time.Now()

	// Note: In a full implementation, we'd track the current transaction and rollback
	// For now, this is a no-op

	return CommitResult{
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    1,
	}, nil
}
