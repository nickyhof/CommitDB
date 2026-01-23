package ps

import (
	"encoding/json"
	"fmt"
	"iter"
	"strings"
	"time"

	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/nickyhof/CommitDB/core"
)

func (persistence *Persistence) CreateDatabase(database core.Database, identity core.Identity) (txn Transaction, err error) {
	if err := persistence.ensureInitialized(); err != nil {
		return Transaction{}, err
	}

	wt, err := persistence.repo.Worktree()
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to get worktree: %w", err)
	}

	path := fmt.Sprintf("%s.database", database.Name)

	tableBytes, err := json.Marshal(database)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to marshal database: %w", err)
	}

	if err := util.WriteFile(wt.Filesystem, path, tableBytes, 0o644); err != nil {
		return Transaction{}, fmt.Errorf("failed to write database file: %w", err)
	}

	if _, err := wt.Add(path); err != nil {
		return Transaction{}, fmt.Errorf("failed to stage database file: %w", err)
	}

	commit, err := wt.Commit("Creating database", &git.CommitOptions{
		Author: &object.Signature{
			Name:  identity.Name,
			Email: identity.Email,
			When:  time.Now(),
		},
	})
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to commit: %w", err)
	}

	obj, err := persistence.repo.CommitObject(commit)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to get commit object: %w", err)
	}

	return Transaction{
		Id:   obj.Hash.String(),
		When: obj.Committer.When,
	}, nil
}

func (persistence *Persistence) GetDatabase(name string) (d *core.Database, err error) {
	if err := persistence.ensureInitialized(); err != nil {
		return nil, err
	}

	wt, err := persistence.repo.Worktree()
	if err != nil {
		return nil, fmt.Errorf("failed to get worktree: %w", err)
	}

	path := fmt.Sprintf("%s.database", name)

	data, err := util.ReadFile(wt.Filesystem, path)
	if err != nil {
		return nil, fmt.Errorf("database %s does not exist: %w", name, err)
	}

	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("failed to unmarshal database: %w", err)
	}

	return d, nil
}

func (persistence *Persistence) DropDatabase(name string, identity core.Identity) (txn Transaction, err error) {
	if err := persistence.ensureInitialized(); err != nil {
		return Transaction{}, err
	}

	wt, err := persistence.repo.Worktree()
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to get worktree: %w", err)
	}

	wt.Remove(fmt.Sprintf("%s.database", name))
	wt.Remove(fmt.Sprintf("%s", name))

	commit, err := wt.Commit("Dropping database", &git.CommitOptions{
		Author: &object.Signature{
			Name:  identity.Name,
			Email: identity.Email,
			When:  time.Now(),
		},
	})
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to commit: %w", err)
	}

	obj, err := persistence.repo.CommitObject(commit)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to get commit object: %w", err)
	}

	return Transaction{
		Id:   obj.Hash.String(),
		When: obj.Committer.When,
	}, nil
}

func (persistence *Persistence) CreateTable(table core.Table, identity core.Identity) (txn Transaction, err error) {
	if err := persistence.ensureInitialized(); err != nil {
		return Transaction{}, err
	}

	wt, err := persistence.repo.Worktree()
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to get worktree: %w", err)
	}

	path := fmt.Sprintf("%s/%s.table", table.Database, table.Name)

	tableBytes, err := json.Marshal(table)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to marshal table: %w", err)
	}

	if err := util.WriteFile(wt.Filesystem, path, tableBytes, 0o644); err != nil {
		return Transaction{}, fmt.Errorf("failed to write table file: %w", err)
	}

	if _, err := wt.Add(path); err != nil {
		return Transaction{}, fmt.Errorf("failed to stage table file: %w", err)
	}

	commit, err := wt.Commit("Creating table", &git.CommitOptions{
		Author: &object.Signature{
			Name:  identity.Name,
			Email: identity.Email,
			When:  time.Now(),
		},
	})
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to commit: %w", err)
	}

	obj, err := persistence.repo.CommitObject(commit)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to get commit object: %w", err)
	}

	return Transaction{
		Id:   obj.Hash.String(),
		When: obj.Committer.When,
	}, nil
}

func (persistence *Persistence) GetTable(database string, table string) (t *core.Table, err error) {
	if err := persistence.ensureInitialized(); err != nil {
		return nil, err
	}

	wt, err := persistence.repo.Worktree()
	if err != nil {
		return nil, fmt.Errorf("failed to get worktree: %w", err)
	}

	path := fmt.Sprintf("%s/%s.table", database, table)

	data, err := util.ReadFile(wt.Filesystem, path)
	if err != nil {
		return nil, fmt.Errorf("table %s.%s does not exist: %w", database, table, err)
	}

	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("failed to unmarshal table: %w", err)
	}

	return t, nil
}

func (persistence *Persistence) DropTable(database string, table string, identity core.Identity) (txn Transaction, err error) {
	if err := persistence.ensureInitialized(); err != nil {
		return Transaction{}, err
	}

	wt, err := persistence.repo.Worktree()
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to get worktree: %w", err)
	}

	wt.Remove(fmt.Sprintf("%s/%s.table", database, table))
	wt.Remove(fmt.Sprintf("%s/%s", database, table))

	commit, err := wt.Commit("Dropping table", &git.CommitOptions{
		Author: &object.Signature{
			Name:  identity.Name,
			Email: identity.Email,
			When:  time.Now(),
		},
	})
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to commit: %w", err)
	}

	obj, err := persistence.repo.CommitObject(commit)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to get commit object: %w", err)
	}

	return Transaction{
		Id:   obj.Hash.String(),
		When: obj.Committer.When,
	}, nil
}

func (persistence *Persistence) SaveRecord(database string, table string, records map[string][]byte, identity core.Identity) (txn Transaction, err error) {
	if err := persistence.ensureInitialized(); err != nil {
		return Transaction{}, err
	}

	wt, err := persistence.repo.Worktree()
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to get worktree: %w", err)
	}

	for key, data := range records {
		path := fmt.Sprintf("%s/%s/%s", database, table, key)
		if err := util.WriteFile(wt.Filesystem, path, data, 0o644); err != nil {
			return Transaction{}, fmt.Errorf("failed to write record %s: %w", key, err)
		}
		if _, err := wt.Add(path); err != nil {
			return Transaction{}, fmt.Errorf("failed to stage record %s: %w", key, err)
		}
	}

	commit, err := wt.Commit("Saving record(s)", &git.CommitOptions{
		Author: &object.Signature{
			Name:  identity.Name,
			Email: identity.Email,
			When:  time.Now(),
		},
	})
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to commit: %w", err)
	}

	obj, err := persistence.repo.CommitObject(commit)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to get commit object: %w", err)
	}

	return Transaction{
		Id:   obj.Hash.String(),
		When: obj.Committer.When,
	}, nil
}

func (persistence *Persistence) DeleteRecord(database string, table string, key string, identity core.Identity) (txn Transaction, err error) {
	if err := persistence.ensureInitialized(); err != nil {
		return Transaction{}, err
	}

	wt, err := persistence.repo.Worktree()
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to get worktree: %w", err)
	}

	path := fmt.Sprintf("%s/%s/%s", database, table, key)
	wt.Remove(path)

	commit, err := wt.Commit("Deleting record", &git.CommitOptions{
		Author: &object.Signature{
			Name:  identity.Name,
			Email: identity.Email,
			When:  time.Now(),
		},
	})
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to commit: %w", err)
	}

	obj, err := persistence.repo.CommitObject(commit)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to get commit object: %w", err)
	}

	return Transaction{
		Id:   obj.Hash.String(),
		When: obj.Committer.When,
	}, nil
}

func (persistence *Persistence) GetRecord(database string, table string, key string) (data []byte, exists bool) {
	if !persistence.IsInitialized() {
		return nil, false
	}

	wt, err := persistence.repo.Worktree()
	if err != nil {
		return nil, false
	}

	path := fmt.Sprintf("%s/%s/%s", database, table, key)

	data, err = util.ReadFile(wt.Filesystem, path)
	if err != nil {
		return nil, false
	}

	return data, true
}

func (persistence *Persistence) ListDatabases() []string {
	if !persistence.IsInitialized() {
		return nil
	}

	wt, err := persistence.repo.Worktree()
	if err != nil {
		return nil
	}

	entries, err := wt.Filesystem.ReadDir(".")
	if err != nil {
		return nil
	}

	var databases []string
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() != ".git" {
			databases = append(databases, entry.Name())
		}
	}

	return databases
}

func (persistence *Persistence) ListTables(database string) []string {
	if !persistence.IsInitialized() {
		return nil
	}

	wt, err := persistence.repo.Worktree()
	if err != nil {
		return nil
	}

	entries, err := wt.Filesystem.ReadDir(database)
	if err != nil {
		return nil
	}

	var tables []string
	for _, entry := range entries {
		if entry.Type().IsRegular() && strings.HasSuffix(entry.Name(), ".table") {
			tables = append(tables, strings.TrimSuffix(entry.Name(), ".table"))
		}
	}

	return tables
}

func (persistence *Persistence) ListRecordKeys(database string, table string) []string {
	if !persistence.IsInitialized() {
		return nil
	}

	wt, err := persistence.repo.Worktree()
	if err != nil {
		return nil
	}

	path := fmt.Sprintf("%s/%s", database, table)

	entries, err := wt.Filesystem.ReadDir(path)
	if err != nil {
		return nil
	}

	var keys []string
	for _, entry := range entries {
		if !entry.IsDir() {
			keys = append(keys, entry.Name())
		}
	}

	return keys
}

func (persistence *Persistence) Scan(database string, table string, filterExpr *func(key string, value []byte) bool) iter.Seq2[string, []byte] {
	keys := persistence.ListRecordKeys(database, table)

	currentIndex := 0

	return func(yield func(key string, value []byte) bool) {
		for currentIndex < len(keys) {
			key := keys[currentIndex]
			value, _ := persistence.GetRecord(database, table, key)

			currentIndex++

			if filterExpr != nil && !(*filterExpr)(key, value) {
				continue
			}

			if !yield(key, value) {
				return
			}
		}
	}
}
