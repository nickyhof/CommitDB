package engine

import (
	"fmt"
	"strings"
	"time"

	"github.com/nickyhof/CommitDB/v2/internal/sql"
	"github.com/nickyhof/CommitDB/v2/persistence"
)

// Remote Git operations

func (engine *Engine) executeAddRemoteStatement(statement sql.AddRemoteStatement) (QueryResult, error) {
	startTime := time.Now()

	err := engine.Persistence.AddRemote(statement.Name, statement.URL)
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:         []string{"Status"},
		Data:            [][]string{{fmt.Sprintf("Remote '%s' added", statement.Name)}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeShowRemotesStatement() (QueryResult, error) {
	startTime := time.Now()

	remotes, err := engine.Persistence.ListRemotes()
	if err != nil {
		return QueryResult{}, err
	}

	data := make([][]string, len(remotes))
	for i, remote := range remotes {
		urls := strings.Join(remote.URLs, ", ")
		data[i] = []string{remote.Name, urls}
	}

	return QueryResult{
		Columns:         []string{"Name", "URLs"},
		Data:            data,
		RecordsRead:     len(remotes),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeDropRemoteStatement(statement sql.DropRemoteStatement) (QueryResult, error) {
	startTime := time.Now()

	err := engine.Persistence.RemoveRemote(statement.Name)
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:         []string{"Status"},
		Data:            [][]string{{fmt.Sprintf("Remote '%s' removed", statement.Name)}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executePushStatement(statement sql.PushStatement) (QueryResult, error) {
	startTime := time.Now()

	auth := convertAuthConfig(statement.Auth)
	err := engine.Persistence.Push(statement.Remote, statement.Branch, auth)
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:         []string{"Status"},
		Data:            [][]string{{fmt.Sprintf("Pushed to '%s'", statement.Remote)}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executePullStatement(statement sql.PullStatement) (QueryResult, error) {
	startTime := time.Now()

	auth := convertAuthConfig(statement.Auth)
	err := engine.Persistence.Pull(statement.Remote, statement.Branch, auth)
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:         []string{"Status"},
		Data:            [][]string{{fmt.Sprintf("Pulled from '%s'", statement.Remote)}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeFetchStatement(statement sql.FetchStatement) (QueryResult, error) {
	startTime := time.Now()

	auth := convertAuthConfig(statement.Auth)
	err := engine.Persistence.Fetch(statement.Remote, auth)
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:         []string{"Status"},
		Data:            [][]string{{fmt.Sprintf("Fetched from '%s'", statement.Remote)}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

// convertAuthConfig converts sql.AuthConfig to persistence.RemoteAuth
func convertAuthConfig(auth *sql.AuthConfig) *persistence.RemoteAuth {
	if auth == nil {
		return nil
	}

	if auth.Token != "" {
		return &persistence.RemoteAuth{
			Type:  persistence.AuthTypeToken,
			Token: auth.Token,
		}
	}

	if auth.SSHKeyPath != "" {
		return &persistence.RemoteAuth{
			Type:       persistence.AuthTypeSSH,
			KeyPath:    auth.SSHKeyPath,
			Passphrase: auth.Passphrase,
		}
	}

	if auth.Username != "" {
		return &persistence.RemoteAuth{
			Type:     persistence.AuthTypeBasic,
			Username: auth.Username,
			Password: auth.Password,
		}
	}

	return nil
}

// Share operations

func (engine *Engine) executeCreateShareStatement(statement sql.CreateShareStatement) (QueryResult, error) {
	startTime := time.Now()

	auth := convertAuthConfig(statement.Auth)
	err := engine.Persistence.CreateShare(statement.Name, statement.URL, auth, engine.Identity)
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:         []string{"Status"},
		Data:            [][]string{{fmt.Sprintf("Share '%s' created from '%s'", statement.Name, statement.URL)}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeSyncShareStatement(statement sql.SyncShareStatement) (QueryResult, error) {
	startTime := time.Now()

	auth := convertAuthConfig(statement.Auth)
	err := engine.Persistence.SyncShare(statement.Name, auth)
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:         []string{"Status"},
		Data:            [][]string{{fmt.Sprintf("Share '%s' synced", statement.Name)}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeDropShareStatement(statement sql.DropShareStatement) (QueryResult, error) {
	startTime := time.Now()

	err := engine.Persistence.DropShare(statement.Name, engine.Identity)
	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{
		Columns:         []string{"Status"},
		Data:            [][]string{{fmt.Sprintf("Share '%s' dropped", statement.Name)}},
		RecordsRead:     1,
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}

func (engine *Engine) executeShowSharesStatement() (QueryResult, error) {
	startTime := time.Now()

	shares, err := engine.Persistence.ListShares()
	if err != nil {
		return QueryResult{}, err
	}

	data := make([][]string, len(shares))
	for i, share := range shares {
		data[i] = []string{share.Name, share.URL}
	}

	return QueryResult{
		Columns:         []string{"Name", "URL"},
		Data:            data,
		RecordsRead:     len(shares),
		ExecutionTimeMs: float64(time.Since(startTime).Milliseconds()),
	}, nil
}
