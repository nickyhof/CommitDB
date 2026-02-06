package engine

import (
	"fmt"

	"github.com/nickyhof/CommitDB/v2/core"
	"github.com/nickyhof/CommitDB/v2/persistence"
	"github.com/nickyhof/CommitDB/v2/internal/sql"
)

type Engine struct {
	*persistence.Persistence
	QueryContext
}

func NewEngine(persistence *persistence.Persistence, identity core.Identity) *Engine {
	return &Engine{
		Persistence:  persistence,
		QueryContext: QueryContext{Identity: identity},
	}
}

func (engine *Engine) Execute(query string) (Result, error) {
	parser := sql.NewParser(query)
	statement, err := parser.Parse()
	if err != nil {
		return nil, err
	}

	switch statement.Type() {
	case sql.SelectStatementType:
		return engine.executeSelectStatement(statement.(sql.SelectStatement))
	case sql.InsertStatementType:
		return engine.executeInsertStatement(statement.(sql.InsertStatement))
	case sql.UpdateStatementType:
		return engine.executeUpdateStatement(statement.(sql.UpdateStatement))
	case sql.DeleteStatementType:
		return engine.executeDeleteStatement(statement.(sql.DeleteStatement))
	case sql.CreateTableStatementType:
		return engine.executeCreateTableStatement(statement.(sql.CreateTableStatement))
	case sql.DropTableStatementType:
		return engine.executeDropTableStatement(statement.(sql.DropTableStatement))
	case sql.CreateDatabaseStatementType:
		return engine.executeCreateDatabaseStatement(statement.(sql.CreateDatabaseStatement))
	case sql.DropDatabaseStatementType:
		return engine.executeDropDatabaseStatement(statement.(sql.DropDatabaseStatement))
	case sql.CreateIndexStatementType:
		return engine.executeCreateIndexStatement(statement.(sql.CreateIndexStatement))
	case sql.DropIndexStatementType:
		return engine.executeDropIndexStatement(statement.(sql.DropIndexStatement))
	case sql.AlterTableStatementType:
		return engine.executeAlterTableStatement(statement.(sql.AlterTableStatement))
	case sql.BeginStatementType:
		return engine.executeBeginStatement()
	case sql.CommitStatementType:
		return engine.executeCommitStatement()
	case sql.RollbackStatementType:
		return engine.executeRollbackStatement()
	case sql.DescribeStatementType:
		return engine.executeDescribeStatement(statement.(sql.DescribeStatement))
	case sql.ShowDatabasesStatementType:
		return engine.executeShowDatabasesStatement(statement.(sql.ShowDatabasesStatement))
	case sql.ShowTablesStatementType:
		return engine.executeShowTablesStatement(statement.(sql.ShowTablesStatement))
	case sql.ShowIndexesStatementType:
		return engine.executeShowIndexesStatement(statement.(sql.ShowIndexesStatement))
	case sql.CreateBranchStatementType:
		return engine.executeCreateBranchStatement(statement.(sql.CreateBranchStatement))
	case sql.CheckoutStatementType:
		return engine.executeCheckoutStatement(statement.(sql.CheckoutStatement))
	case sql.MergeStatementType:
		return engine.executeMergeStatement(statement.(sql.MergeStatement))
	case sql.ShowBranchesStatementType:
		return engine.executeShowBranchesStatement(statement.(sql.ShowBranchesStatement))
	case sql.ShowTransactionsStatementType:
		return engine.executeShowTransactionsStatement(statement.(sql.ShowTransactionsStatement))
	case sql.ShowMergeConflictsStatementType:
		return engine.executeShowMergeConflictsStatement()
	case sql.ResolveConflictStatementType:
		return engine.executeResolveConflictStatement(statement.(sql.ResolveConflictStatement))
	case sql.CommitMergeStatementType:
		return engine.executeCommitMergeStatement()
	case sql.AbortMergeStatementType:
		return engine.executeAbortMergeStatement()
	case sql.AddRemoteStatementType:
		return engine.executeAddRemoteStatement(statement.(sql.AddRemoteStatement))
	case sql.ShowRemotesStatementType:
		return engine.executeShowRemotesStatement()
	case sql.DropRemoteStatementType:
		return engine.executeDropRemoteStatement(statement.(sql.DropRemoteStatement))
	case sql.PushStatementType:
		return engine.executePushStatement(statement.(sql.PushStatement))
	case sql.PullStatementType:
		return engine.executePullStatement(statement.(sql.PullStatement))
	case sql.FetchStatementType:
		return engine.executeFetchStatement(statement.(sql.FetchStatement))
	case sql.CopyStatementType:
		return engine.executeCopyStatement(statement.(sql.CopyStatement))
	case sql.CreateShareStatementType:
		return engine.executeCreateShareStatement(statement.(sql.CreateShareStatement))
	case sql.SyncShareStatementType:
		return engine.executeSyncShareStatement(statement.(sql.SyncShareStatement))
	case sql.DropShareStatementType:
		return engine.executeDropShareStatement(statement.(sql.DropShareStatement))
	case sql.ShowSharesStatementType:
		return engine.executeShowSharesStatement()
	case sql.CreateViewStatementType:
		return engine.executeCreateViewStatement(statement.(sql.CreateViewStatement))
	case sql.DropViewStatementType:
		return engine.executeDropViewStatement(statement.(sql.DropViewStatement))
	case sql.ShowViewsStatementType:
		return engine.executeShowViewsStatement(statement.(sql.ShowViewsStatement))
	case sql.RefreshViewStatementType:
		return engine.executeRefreshViewStatement(statement.(sql.RefreshViewStatement))
	default:
		return nil, fmt.Errorf("unsupported statement type: %v", statement.Type())
	}
}
