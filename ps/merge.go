package ps

import (
	"fmt"
	"time"

	"github.com/go-git/go-billy/v6/util"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/nickyhof/CommitDB/core"
)

// MergeStrategy defines how to handle merge
type MergeStrategy string

const (
	// MergeStrategyFastForwardOnly only allows fast-forward merges
	MergeStrategyFastForwardOnly MergeStrategy = "fast-forward-only"
	// MergeStrategyRowLevel performs row-level merge for diverged branches (LWW auto-resolve)
	MergeStrategyRowLevel MergeStrategy = "row-level"
	// MergeStrategyManual pauses on conflicts for manual resolution
	MergeStrategyManual MergeStrategy = "manual"
)

// MergeOptions configures merge behavior
type MergeOptions struct {
	Strategy MergeStrategy
}

// DefaultMergeOptions returns the default merge options (row-level merge)
func DefaultMergeOptions() MergeOptions {
	return MergeOptions{
		Strategy: MergeStrategyRowLevel,
	}
}

// RecordConflict represents a conflict where both branches modified the same record
type RecordConflict struct {
	Database  string
	Table     string
	Key       string
	BaseVal   []byte // nil if record didn't exist at base
	HeadVal   []byte // nil if deleted in HEAD
	SourceVal []byte // nil if deleted in SOURCE
	Resolved  []byte // the resolved value (LWW winner)
}

// MergeResult contains information about a completed or pending merge
type MergeResult struct {
	Transaction   Transaction
	MergedRecords int
	FastForward   bool
	Conflicts     []RecordConflict // conflicts that were auto-resolved (LWW)
	Unresolved    []RecordConflict // conflicts requiring manual resolution
	MergeID       string           // ID to resume pending merge (empty if complete)
	Pending       bool             // true if merge is pending manual resolution
}

// PendingMerge stores state for a merge awaiting manual conflict resolution
type PendingMerge struct {
	MergeID       string            `json:"merge_id"`
	HeadCommit    string            `json:"head_commit"`
	SourceCommit  string            `json:"source_commit"`
	SourceBranch  string            `json:"source_branch"`
	BaseCommit    string            `json:"base_commit"`
	Resolved      map[string][]byte `json:"resolved"`       // key -> resolved data
	Unresolved    []RecordConflict  `json:"unresolved"`     // remaining conflicts
	MergedRecords map[string][]byte `json:"merged_records"` // non-conflicting merged data
	CreatedAt     time.Time         `json:"created_at"`
}

// findMergeBase finds the common ancestor commit of two commits
func (p *Persistence) findMergeBase(headHash, sourceHash plumbing.Hash) (*object.Commit, error) {
	headCommit, err := p.repo.CommitObject(headHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get HEAD commit: %w", err)
	}

	sourceCommit, err := p.repo.CommitObject(sourceHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get source commit: %w", err)
	}

	// Walk both commit histories to find common ancestor
	// Build ancestor set from HEAD
	headAncestors := make(map[plumbing.Hash]bool)
	headIter := object.NewCommitIterCTime(headCommit, nil, nil)
	err = headIter.ForEach(func(c *object.Commit) error {
		headAncestors[c.Hash] = true
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to iterate HEAD history: %w", err)
	}

	// Find first source ancestor that's also in HEAD ancestors
	var mergeBase *object.Commit
	sourceIter := object.NewCommitIterCTime(sourceCommit, nil, nil)
	err = sourceIter.ForEach(func(c *object.Commit) error {
		if headAncestors[c.Hash] {
			mergeBase = c
			return fmt.Errorf("found") // break iteration
		}
		return nil
	})

	if mergeBase == nil {
		return nil, fmt.Errorf("no common ancestor found")
	}

	return mergeBase, nil
}

// getRecordsAtCommit reads all records from a table at a specific commit
func (p *Persistence) getRecordsAtCommit(commit *object.Commit, database, table string) (map[string][]byte, error) {
	tree, err := commit.Tree()
	if err != nil {
		return nil, err
	}

	records := make(map[string][]byte)
	tablePath := fmt.Sprintf("%s/%s", database, table)

	// Try to find the table directory
	tableTree, err := tree.Tree(tablePath)
	if err != nil {
		// Table doesn't exist at this commit
		return records, nil
	}

	// Read all record files
	for _, entry := range tableTree.Entries {
		if entry.Name == "_meta.json" {
			continue
		}

		file, err := tableTree.File(entry.Name)
		if err != nil {
			continue
		}

		content, err := file.Contents()
		if err != nil {
			continue
		}

		// Remove .json extension for key
		key := entry.Name
		if len(key) > 5 && key[len(key)-5:] == ".json" {
			key = key[:len(key)-5]
		}

		records[key] = []byte(content)
	}

	return records, nil
}

// mergeRecordMaps performs three-way merge of record maps
// Returns merged records and any conflicts that were auto-resolved
func mergeRecordMaps(
	base, head, source map[string][]byte,
	headTime, sourceTime time.Time,
) (merged map[string][]byte, conflicts []RecordConflict) {
	merged = make(map[string][]byte)
	conflicts = []RecordConflict{}

	// Collect all keys from all three states
	allKeys := make(map[string]bool)
	for k := range base {
		allKeys[k] = true
	}
	for k := range head {
		allKeys[k] = true
	}
	for k := range source {
		allKeys[k] = true
	}

	for key := range allKeys {
		baseVal, inBase := base[key]
		headVal, inHead := head[key]
		sourceVal, inSource := source[key]

		// Case 1: Only in HEAD (added in HEAD or deleted in SOURCE)
		if inHead && !inSource && !inBase {
			merged[key] = headVal
			continue
		}

		// Case 2: Only in SOURCE (added in SOURCE or deleted in HEAD)
		if inSource && !inHead && !inBase {
			merged[key] = sourceVal
			continue
		}

		// Case 3: In BASE but deleted in one branch
		if inBase && !inHead && inSource {
			// Deleted in HEAD, check if modified in SOURCE
			if string(baseVal) == string(sourceVal) {
				// Not modified in SOURCE, delete wins
				continue
			}
			// Modified in SOURCE, conflict - LWW
			if sourceTime.After(headTime) {
				merged[key] = sourceVal
			}
			// else deleted in HEAD wins (later)
			conflicts = append(conflicts, RecordConflict{
				Key:       key,
				BaseVal:   baseVal,
				HeadVal:   nil,
				SourceVal: sourceVal,
				Resolved:  merged[key],
			})
			continue
		}

		if inBase && inHead && !inSource {
			// Deleted in SOURCE, check if modified in HEAD
			if string(baseVal) == string(headVal) {
				// Not modified in HEAD, delete wins
				continue
			}
			// Modified in HEAD, conflict - LWW
			if headTime.After(sourceTime) {
				merged[key] = headVal
			}
			// else deleted in SOURCE wins (later)
			conflicts = append(conflicts, RecordConflict{
				Key:       key,
				BaseVal:   baseVal,
				HeadVal:   headVal,
				SourceVal: nil,
				Resolved:  merged[key],
			})
			continue
		}

		// Case 4: Both have the record
		if inHead && inSource {
			headUnchanged := inBase && string(baseVal) == string(headVal)
			sourceUnchanged := inBase && string(baseVal) == string(sourceVal)

			if headUnchanged && sourceUnchanged {
				// Neither changed
				merged[key] = headVal
			} else if headUnchanged {
				// Only SOURCE changed
				merged[key] = sourceVal
			} else if sourceUnchanged {
				// Only HEAD changed
				merged[key] = headVal
			} else if string(headVal) == string(sourceVal) {
				// Both changed to same value
				merged[key] = headVal
			} else {
				// Both changed to different values - LWW
				if sourceTime.After(headTime) {
					merged[key] = sourceVal
				} else {
					merged[key] = headVal
				}
				conflicts = append(conflicts, RecordConflict{
					Key:       key,
					BaseVal:   baseVal,
					HeadVal:   headVal,
					SourceVal: sourceVal,
					Resolved:  merged[key],
				})
			}
		}
	}

	return merged, conflicts
}

// performRowLevelMerge executes a three-way row-level merge
func (p *Persistence) performRowLevelMerge(
	headCommit, sourceCommit, baseCommit *object.Commit,
	identity core.Identity,
) (MergeResult, error) {
	result := MergeResult{}

	wt, err := p.repo.Worktree()
	if err != nil {
		return result, fmt.Errorf("failed to get worktree: %w", err)
	}

	// Get list of all databases/tables from all three commits
	databases := p.collectDatabases(headCommit, sourceCommit, baseCommit)

	for _, dbName := range databases {
		tables := p.collectTables(dbName, headCommit, sourceCommit, baseCommit)

		for _, tableName := range tables {
			baseRecords, _ := p.getRecordsAtCommit(baseCommit, dbName, tableName)
			headRecords, _ := p.getRecordsAtCommit(headCommit, dbName, tableName)
			sourceRecords, _ := p.getRecordsAtCommit(sourceCommit, dbName, tableName)

			merged, conflicts := mergeRecordMaps(
				baseRecords, headRecords, sourceRecords,
				headCommit.Committer.When, sourceCommit.Committer.When,
			)

			// Update conflicts with table info
			for i := range conflicts {
				conflicts[i].Database = dbName
				conflicts[i].Table = tableName
			}
			result.Conflicts = append(result.Conflicts, conflicts...)

			// Write merged records to worktree
			for key, data := range merged {
				path := fmt.Sprintf("%s/%s/%s.json", dbName, tableName, key)
				if err := util.WriteFile(wt.Filesystem, path, data, 0644); err != nil {
					return result, fmt.Errorf("failed to write merged record: %w", err)
				}
				result.MergedRecords++
			}

			// Remove records that should be deleted (in head/source but not in merged)
			for key := range headRecords {
				if _, inMerged := merged[key]; !inMerged {
					path := fmt.Sprintf("%s/%s/%s.json", dbName, tableName, key)
					wt.Filesystem.Remove(path)
				}
			}
		}
	}

	// Stage all changes
	if _, err := wt.Add("."); err != nil {
		return result, fmt.Errorf("failed to stage changes: %w", err)
	}

	// Create merge commit
	msg := "Merge branch into current"
	txn, err := p.createMergeCommit(msg, sourceCommit.Hash, identity)
	if err != nil {
		return result, err
	}

	result.Transaction = txn
	return result, nil
}

// createMergeCommit creates a commit (simplified - using regular commit for now)
func (p *Persistence) createMergeCommit(
	message string,
	sourceHash plumbing.Hash,
	identity core.Identity,
) (Transaction, error) {
	wt, err := p.repo.Worktree()
	if err != nil {
		return Transaction{}, err
	}

	// Get the current status
	status, err := wt.Status()
	if err != nil {
		return Transaction{}, err
	}

	// Check if there are changes to commit
	if status.IsClean() {
		// No changes, just update branch to source
		headRef, _ := p.repo.Head()
		if headRef.Name().IsBranch() {
			newRef := plumbing.NewHashReference(headRef.Name(), sourceHash)
			p.repo.Storer.SetReference(newRef)
		}
		commit, _ := p.repo.CommitObject(sourceHash)
		return Transaction{
			Id:   sourceHash.String(),
			When: commit.Committer.When,
		}, nil
	}

	// Create the merge commit
	hash, err := wt.Commit(message, &git.CommitOptions{
		Author: &object.Signature{
			Name:  identity.Name,
			Email: identity.Email,
			When:  time.Now(),
		},
	})
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to create merge commit: %w", err)
	}

	commit, _ := p.repo.CommitObject(hash)
	return Transaction{
		Id:   hash.String(),
		When: commit.Committer.When,
	}, nil
}

// collectDatabases returns all database names across commits
func (p *Persistence) collectDatabases(commits ...*object.Commit) []string {
	dbSet := make(map[string]bool)

	for _, commit := range commits {
		tree, err := commit.Tree()
		if err != nil {
			continue
		}

		for _, entry := range tree.Entries {
			if entry.Mode == filemode.Dir {
				dbSet[entry.Name] = true
			}
		}
	}

	dbs := make([]string, 0, len(dbSet))
	for db := range dbSet {
		dbs = append(dbs, db)
	}
	return dbs
}

// collectTables returns all table names for a database across commits
func (p *Persistence) collectTables(database string, commits ...*object.Commit) []string {
	tableSet := make(map[string]bool)

	for _, commit := range commits {
		tree, err := commit.Tree()
		if err != nil {
			continue
		}

		dbTree, err := tree.Tree(database)
		if err != nil {
			continue
		}

		for _, entry := range dbTree.Entries {
			if entry.Mode == filemode.Dir {
				tableSet[entry.Name] = true
			}
		}
	}

	tables := make([]string, 0, len(tableSet))
	for table := range tableSet {
		tables = append(tables, table)
	}
	return tables
}

// MergeWithOptions merges source branch into current with specified options
func (p *Persistence) MergeWithOptions(source string, identity core.Identity, opts MergeOptions) (MergeResult, error) {
	if err := p.ensureInitialized(); err != nil {
		return MergeResult{}, err
	}

	headRef, err := p.repo.Head()
	if err != nil {
		return MergeResult{}, fmt.Errorf("failed to get HEAD: %w", err)
	}

	sourceBranchRef := plumbing.NewBranchReferenceName(source)
	sourceRef, err := p.repo.Reference(sourceBranchRef, true)
	if err != nil {
		return MergeResult{}, fmt.Errorf("branch '%s' not found: %w", source, err)
	}

	sourceCommit, err := p.repo.CommitObject(sourceRef.Hash())
	if err != nil {
		return MergeResult{}, err
	}
	headCommit, err := p.repo.CommitObject(headRef.Hash())
	if err != nil {
		return MergeResult{}, err
	}

	// Check if source is ancestor of HEAD (already merged)
	isAncestor, err := sourceCommit.IsAncestor(headCommit)
	if err != nil {
		return MergeResult{}, fmt.Errorf("failed to check ancestry: %w", err)
	}
	if isAncestor {
		return MergeResult{
			Transaction: Transaction{Id: headRef.Hash().String(), When: headCommit.Committer.When},
			FastForward: true,
		}, nil
	}

	// Check if can fast-forward
	canFF, _ := headCommit.IsAncestor(sourceCommit)
	if canFF {
		// Fast-forward merge
		wt, err := p.repo.Worktree()
		if err != nil {
			return MergeResult{}, err
		}

		err = wt.Reset(&git.ResetOptions{
			Mode:   git.HardReset,
			Commit: sourceRef.Hash(),
		})
		if err != nil {
			return MergeResult{}, fmt.Errorf("failed to fast-forward: %w", err)
		}

		if headRef.Name().IsBranch() {
			newRef := plumbing.NewHashReference(headRef.Name(), sourceRef.Hash())
			p.repo.Storer.SetReference(newRef)
		}

		return MergeResult{
			Transaction: Transaction{Id: sourceRef.Hash().String(), When: sourceCommit.Committer.When},
			FastForward: true,
		}, nil
	}

	// Branches have diverged
	if opts.Strategy == MergeStrategyFastForwardOnly {
		return MergeResult{}, fmt.Errorf("cannot fast-forward merge - branches have diverged")
	}

	// Row-level merge
	baseCommit, err := p.findMergeBase(headRef.Hash(), sourceRef.Hash())
	if err != nil {
		return MergeResult{}, fmt.Errorf("failed to find merge base: %w", err)
	}

	// For manual strategy, check for conflicts first
	if opts.Strategy == MergeStrategyManual {
		return p.performManualMerge(headCommit, sourceCommit, baseCommit, source, identity)
	}

	return p.performRowLevelMerge(headCommit, sourceCommit, baseCommit, identity)
}

// performManualMerge sets up a merge that pauses for manual conflict resolution
func (p *Persistence) performManualMerge(
	headCommit, sourceCommit, baseCommit *object.Commit,
	sourceBranch string,
	identity core.Identity,
) (MergeResult, error) {

	// Get list of all databases/tables from all three commits
	databases := p.collectDatabases(headCommit, sourceCommit, baseCommit)

	allMerged := make(map[string][]byte)
	var allConflicts []RecordConflict

	for _, dbName := range databases {
		tables := p.collectTables(dbName, headCommit, sourceCommit, baseCommit)

		for _, tableName := range tables {
			baseRecords, _ := p.getRecordsAtCommit(baseCommit, dbName, tableName)
			headRecords, _ := p.getRecordsAtCommit(headCommit, dbName, tableName)
			sourceRecords, _ := p.getRecordsAtCommit(sourceCommit, dbName, tableName)

			merged, conflicts := mergeRecordMapsManual(
				baseRecords, headRecords, sourceRecords,
			)

			// Store merged records with full path
			for key, data := range merged {
				fullKey := fmt.Sprintf("%s.%s.%s", dbName, tableName, key)
				allMerged[fullKey] = data
			}

			// Update conflicts with table info
			for i := range conflicts {
				conflicts[i].Database = dbName
				conflicts[i].Table = tableName
			}
			allConflicts = append(allConflicts, conflicts...)
		}
	}

	// If no conflicts, complete merge immediately
	if len(allConflicts) == 0 {
		return p.performRowLevelMerge(headCommit, sourceCommit, baseCommit, identity)
	}

	// Store pending merge state
	mergeID := fmt.Sprintf("merge-%d", time.Now().UnixNano())
	p.pendingMerge = &PendingMerge{
		MergeID:       mergeID,
		HeadCommit:    headCommit.Hash.String(),
		SourceCommit:  sourceCommit.Hash.String(),
		SourceBranch:  sourceBranch,
		BaseCommit:    baseCommit.Hash.String(),
		Resolved:      make(map[string][]byte),
		Unresolved:    allConflicts,
		MergedRecords: allMerged,
		CreatedAt:     time.Now(),
	}

	return MergeResult{
		Unresolved: allConflicts,
		MergeID:    mergeID,
		Pending:    true,
	}, nil
}

// mergeRecordMapsManual performs three-way merge but does NOT auto-resolve conflicts
func mergeRecordMapsManual(
	base, head, source map[string][]byte,
) (merged map[string][]byte, conflicts []RecordConflict) {
	merged = make(map[string][]byte)
	conflicts = []RecordConflict{}

	allKeys := make(map[string]bool)
	for k := range base {
		allKeys[k] = true
	}
	for k := range head {
		allKeys[k] = true
	}
	for k := range source {
		allKeys[k] = true
	}

	for key := range allKeys {
		baseVal, inBase := base[key]
		headVal, inHead := head[key]
		sourceVal, inSource := source[key]

		// Non-conflicting cases
		if inHead && !inSource && !inBase {
			merged[key] = headVal
			continue
		}
		if inSource && !inHead && !inBase {
			merged[key] = sourceVal
			continue
		}
		if inBase && !inHead && inSource && string(baseVal) == string(sourceVal) {
			continue // delete wins
		}
		if inBase && inHead && !inSource && string(baseVal) == string(headVal) {
			continue // delete wins
		}

		// Check if both have it and it's the same
		if inHead && inSource {
			headUnchanged := inBase && string(baseVal) == string(headVal)
			sourceUnchanged := inBase && string(baseVal) == string(sourceVal)

			if headUnchanged && sourceUnchanged {
				merged[key] = headVal
				continue
			} else if headUnchanged {
				merged[key] = sourceVal
				continue
			} else if sourceUnchanged {
				merged[key] = headVal
				continue
			} else if string(headVal) == string(sourceVal) {
				merged[key] = headVal
				continue
			}
		}

		// This is a conflict - don't auto-resolve
		conflicts = append(conflicts, RecordConflict{
			Key:       key,
			BaseVal:   baseVal,
			HeadVal:   headVal,
			SourceVal: sourceVal,
		})
	}

	return merged, conflicts
}

// GetPendingMerge returns the current pending merge, if any
func (p *Persistence) GetPendingMerge() *PendingMerge {
	return p.pendingMerge
}

// ResolveConflict resolves a single conflict in a pending merge
func (p *Persistence) ResolveConflict(database, table, key string, resolution []byte) error {
	if p.pendingMerge == nil {
		return fmt.Errorf("no pending merge")
	}

	fullKey := fmt.Sprintf("%s.%s.%s", database, table, key)

	// Find and remove from unresolved
	found := false
	for i, conflict := range p.pendingMerge.Unresolved {
		if conflict.Database == database && conflict.Table == table && conflict.Key == key {
			p.pendingMerge.Unresolved = append(
				p.pendingMerge.Unresolved[:i],
				p.pendingMerge.Unresolved[i+1:]...,
			)
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("conflict not found: %s", fullKey)
	}

	// Store resolution
	p.pendingMerge.Resolved[fullKey] = resolution
	return nil
}

// CompleteMerge finishes a pending merge after all conflicts are resolved
func (p *Persistence) CompleteMerge(identity core.Identity) (Transaction, error) {
	if p.pendingMerge == nil {
		return Transaction{}, fmt.Errorf("no pending merge")
	}

	if len(p.pendingMerge.Unresolved) > 0 {
		return Transaction{}, fmt.Errorf("%d unresolved conflicts remaining", len(p.pendingMerge.Unresolved))
	}

	wt, err := p.repo.Worktree()
	if err != nil {
		return Transaction{}, err
	}

	// Write all merged records
	for fullKey, data := range p.pendingMerge.MergedRecords {
		parts := splitKey(fullKey)
		if len(parts) != 3 {
			continue
		}
		path := fmt.Sprintf("%s/%s/%s.json", parts[0], parts[1], parts[2])
		if err := util.WriteFile(wt.Filesystem, path, data, 0644); err != nil {
			return Transaction{}, err
		}
	}

	// Write resolved conflicts
	for fullKey, data := range p.pendingMerge.Resolved {
		parts := splitKey(fullKey)
		if len(parts) != 3 {
			continue
		}
		path := fmt.Sprintf("%s/%s/%s.json", parts[0], parts[1], parts[2])
		if err := util.WriteFile(wt.Filesystem, path, data, 0644); err != nil {
			return Transaction{}, err
		}
	}

	// Stage and commit
	if _, err := wt.Add("."); err != nil {
		return Transaction{}, err
	}

	msg := fmt.Sprintf("Merge branch '%s' (manual resolution)", p.pendingMerge.SourceBranch)
	hash, err := wt.Commit(msg, &git.CommitOptions{
		Author: &object.Signature{
			Name:  identity.Name,
			Email: identity.Email,
			When:  time.Now(),
		},
	})
	if err != nil {
		return Transaction{}, err
	}

	commit, _ := p.repo.CommitObject(hash)

	// Clear pending merge
	p.pendingMerge = nil

	return Transaction{
		Id:   hash.String(),
		When: commit.Committer.When,
	}, nil
}

// AbortMerge cancels a pending merge
func (p *Persistence) AbortMerge() error {
	if p.pendingMerge == nil {
		return fmt.Errorf("no pending merge")
	}

	// Reset worktree to HEAD
	wt, err := p.repo.Worktree()
	if err != nil {
		return err
	}

	headRef, _ := p.repo.Head()
	err = wt.Reset(&git.ResetOptions{
		Mode:   git.HardReset,
		Commit: headRef.Hash(),
	})

	p.pendingMerge = nil
	return err
}

// splitKey splits "db.table.key" into parts
func splitKey(key string) []string {
	parts := make([]string, 0, 3)
	start := 0
	for i, c := range key {
		if c == '.' {
			parts = append(parts, key[start:i])
			start = i + 1
		}
	}
	parts = append(parts, key[start:])
	return parts
}
