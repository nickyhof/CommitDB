package engine

import (
	"fmt"
	"time"

	"github.com/nickyhof/CommitDB/v2/internal/sql"
	"github.com/nickyhof/CommitDB/v2/persistence"
)

func (engine *Engine) executeCreateBranchStatement(statement sql.CreateBranchStatement) (CommitResult, error) {
	startTime := time.Now()

	var from *persistence.Transaction
	if statement.FromTxnID != "" {
		from = &persistence.Transaction{ID: statement.FromTxnID}
	}

	err := engine.Persistence.Branch(statement.Name, from)
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		Transaction:     engine.Persistence.LatestTransaction(),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    1,
	}, nil
}

func (engine *Engine) executeCheckoutStatement(statement sql.CheckoutStatement) (CommitResult, error) {
	startTime := time.Now()

	err := engine.Persistence.Checkout(statement.Branch)
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		Transaction:     engine.Persistence.LatestTransaction(),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    1,
	}, nil
}

func (engine *Engine) executeMergeStatement(statement sql.MergeStatement) (Result, error) {
	startTime := time.Now()

	opts := persistence.DefaultMergeOptions()
	if statement.ManualResolution {
		opts.Strategy = persistence.MergeStrategyManual
	}

	result, err := engine.Persistence.MergeWithOptions(statement.SourceBranch, engine.Identity, opts)
	if err != nil {
		return CommitResult{}, err
	}

	// If pending (manual mode with conflicts)
	if result.Pending {
		// Return query result showing conflicts
		data := make([][]string, len(result.Unresolved))
		for i, conflict := range result.Unresolved {
			data[i] = []string{
				conflict.Database,
				conflict.Table,
				conflict.Key,
				string(conflict.HeadVal),
				string(conflict.SourceVal),
			}
		}
		return QueryResult{
			Columns:         []string{"Database", "Table", "Key", "HEAD", "SOURCE"},
			Data:            data,
			RecordsRead:     len(data),
			ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		}, nil
	}

	return CommitResult{
		Transaction:     result.Transaction,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    1,
	}, nil
}

func (engine *Engine) executeShowBranchesStatement(statement sql.ShowBranchesStatement) (QueryResult, error) {
	startTime := time.Now()

	branches, err := engine.Persistence.ListBranches()
	if err != nil {
		return QueryResult{}, err
	}

	currentBranch, _ := engine.Persistence.CurrentBranch()

	data := make([][]string, len(branches))
	for i, branch := range branches {
		isCurrent := ""
		if branch == currentBranch {
			isCurrent = "*"
		}
		data[i] = []string{branch, isCurrent}
	}

	return QueryResult{
		Transaction:     engine.Persistence.LatestTransaction(),
		Columns:         []string{"Branch", "Current"},
		Data:            data,
		RecordsRead:     len(data),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    len(data),
	}, nil
}

func (engine *Engine) executeShowTransactionsStatement(statement sql.ShowTransactionsStatement) (QueryResult, error) {
	startTime := time.Now()

	transactions, err := engine.Persistence.ListTransactions(statement.Limit)
	if err != nil {
		return QueryResult{}, err
	}

	data := make([][]string, len(transactions))
	for i, txn := range transactions {
		data[i] = []string{
			txn.ID,
			txn.When.Format(time.RFC3339),
			txn.Message,
			txn.Author,
		}
	}

	return QueryResult{
		Transaction:     engine.Persistence.LatestTransaction(),
		Columns:         []string{"ID", "Timestamp", "Message", "Author"},
		Data:            data,
		RecordsRead:     len(data),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    len(data),
	}, nil
}

func (engine *Engine) executeShowMergeConflictsStatement() (QueryResult, error) {
	startTime := time.Now()

	pending := engine.Persistence.GetPendingMerge()
	if pending == nil {
		return QueryResult{
			Columns:         []string{"Database", "Table", "Key", "HEAD", "SOURCE"},
			Data:            [][]string{},
			RecordsRead:     0,
			ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		}, nil
	}

	data := make([][]string, len(pending.Unresolved))
	for i, conflict := range pending.Unresolved {
		data[i] = []string{
			conflict.Database,
			conflict.Table,
			conflict.Key,
			string(conflict.HeadVal),
			string(conflict.SourceVal),
		}
	}

	return QueryResult{
		Columns:         []string{"Database", "Table", "Key", "HEAD", "SOURCE"},
		Data:            data,
		RecordsRead:     len(data),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeResolveConflictStatement(statement sql.ResolveConflictStatement) (QueryResult, error) {
	startTime := time.Now()

	pending := engine.Persistence.GetPendingMerge()
	if pending == nil {
		return QueryResult{}, fmt.Errorf("no pending merge")
	}

	var resolution []byte
	switch statement.Resolution {
	case "HEAD":
		// Find HEAD value from pending conflicts
		for _, c := range pending.Unresolved {
			if c.Database == statement.Database && c.Table == statement.Table && c.Key == statement.Key {
				resolution = c.HeadVal
				break
			}
		}
	case "SOURCE":
		// Find SOURCE value from pending conflicts
		for _, c := range pending.Unresolved {
			if c.Database == statement.Database && c.Table == statement.Table && c.Key == statement.Key {
				resolution = c.SourceVal
				break
			}
		}
	default:
		resolution = []byte(statement.Resolution)
	}

	err := engine.Persistence.ResolveConflict(statement.Database, statement.Table, statement.Key, resolution)
	if err != nil {
		return QueryResult{}, err
	}

	// Return remaining conflicts count
	remaining := len(engine.Persistence.GetPendingMerge().Unresolved)
	return QueryResult{
		Columns:         []string{"Resolved", "Remaining"},
		Data:            [][]string{{fmt.Sprintf("%s.%s.%s", statement.Database, statement.Table, statement.Key), fmt.Sprintf("%d", remaining)}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeCommitMergeStatement() (CommitResult, error) {
	startTime := time.Now()

	txn, err := engine.Persistence.CompleteMerge(engine.Identity)
	if err != nil {
		return CommitResult{}, err
	}

	return CommitResult{
		Transaction:     txn,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
		ExecutionOps:    1,
	}, nil
}

func (engine *Engine) executeAbortMergeStatement() (QueryResult, error) {
	startTime := time.Now()

	err := engine.Persistence.AbortMerge()
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:         []string{"Status"},
		Data:            [][]string{{"Merge aborted"}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}
