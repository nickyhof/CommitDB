package ps

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/nickyhof/CommitDB/core"
)

// createBlob creates a blob object directly in the object store without filesystem I/O
func (p *Persistence) createBlob(data []byte) (plumbing.Hash, error) {
	obj := p.repo.Storer.NewEncodedObject()
	obj.SetType(plumbing.BlobObject)
	obj.SetSize(int64(len(data)))

	writer, err := obj.Writer()
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("failed to create blob writer: %w", err)
	}

	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return plumbing.ZeroHash, fmt.Errorf("failed to write blob data: %w", err)
	}
	writer.Close()

	hash, err := p.repo.Storer.SetEncodedObject(obj)
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("failed to store blob: %w", err)
	}

	return hash, nil
}

// getCurrentTree returns the tree hash from the current HEAD commit.
// Returns ZeroHash if repository has no commits yet.
func (p *Persistence) getCurrentTree() (plumbing.Hash, error) {
	headRef, err := p.repo.Head()
	if err != nil {
		// No commits yet - return zero hash
		return plumbing.ZeroHash, nil
	}

	commit, err := p.repo.CommitObject(headRef.Hash())
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("failed to get head commit: %w", err)
	}

	return commit.TreeHash, nil
}

// getTreeEntries reads all entries from an existing tree, returning a map of path -> hash/mode
func (p *Persistence) getTreeEntries(treeHash plumbing.Hash) (map[string]object.TreeEntry, error) {
	entries := make(map[string]object.TreeEntry)

	if treeHash == plumbing.ZeroHash {
		return entries, nil
	}

	tree, err := object.GetTree(p.repo.Storer, treeHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get tree: %w", err)
	}

	for _, entry := range tree.Entries {
		entries[entry.Name] = entry
	}

	return entries, nil
}

// buildTreeFromEntries creates a tree object from a list of entries
func (p *Persistence) buildTreeFromEntries(entries []object.TreeEntry) (plumbing.Hash, error) {
	// Sort entries by name (Git requirement)
	sort.Slice(entries, func(i, j int) bool {
		// Directories are sorted with trailing slash for comparison
		nameI := entries[i].Name
		nameJ := entries[j].Name
		if entries[i].Mode == filemode.Dir {
			nameI += "/"
		}
		if entries[j].Mode == filemode.Dir {
			nameJ += "/"
		}
		return nameI < nameJ
	})

	tree := &object.Tree{Entries: entries}

	obj := p.repo.Storer.NewEncodedObject()
	if err := tree.Encode(obj); err != nil {
		return plumbing.ZeroHash, fmt.Errorf("failed to encode tree: %w", err)
	}

	hash, err := p.repo.Storer.SetEncodedObject(obj)
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("failed to store tree: %w", err)
	}

	return hash, nil
}

// updateTreePath updates or creates a blob at the given path in the tree.
// Path can be nested like "database/table/key".
// Returns the new root tree hash.
func (p *Persistence) updateTreePath(rootTreeHash plumbing.Hash, filePath string, blobHash plumbing.Hash) (plumbing.Hash, error) {
	parts := strings.Split(filePath, "/")
	return p.updateTreePathRecursive(rootTreeHash, parts, blobHash)
}

func (p *Persistence) updateTreePathRecursive(treeHash plumbing.Hash, pathParts []string, blobHash plumbing.Hash) (plumbing.Hash, error) {
	if len(pathParts) == 0 {
		return plumbing.ZeroHash, fmt.Errorf("empty path")
	}

	// Get existing entries
	entries, err := p.getTreeEntries(treeHash)
	if err != nil {
		return plumbing.ZeroHash, err
	}

	name := pathParts[0]

	if len(pathParts) == 1 {
		// Leaf node - add/update the blob
		entries[name] = object.TreeEntry{
			Name: name,
			Mode: filemode.Regular,
			Hash: blobHash,
		}
	} else {
		// Intermediate directory - recurse
		var subTreeHash plumbing.Hash
		if existing, ok := entries[name]; ok && existing.Mode == filemode.Dir {
			subTreeHash = existing.Hash
		} else {
			subTreeHash = plumbing.ZeroHash
		}

		newSubTreeHash, err := p.updateTreePathRecursive(subTreeHash, pathParts[1:], blobHash)
		if err != nil {
			return plumbing.ZeroHash, err
		}

		entries[name] = object.TreeEntry{
			Name: name,
			Mode: filemode.Dir,
			Hash: newSubTreeHash,
		}
	}

	// Convert map to slice and build new tree
	entrySlice := make([]object.TreeEntry, 0, len(entries))
	for _, entry := range entries {
		entrySlice = append(entrySlice, entry)
	}

	return p.buildTreeFromEntries(entrySlice)
}

// deleteTreePath removes a blob at the given path from the tree.
// Returns the new root tree hash.
func (p *Persistence) deleteTreePath(rootTreeHash plumbing.Hash, filePath string) (plumbing.Hash, error) {
	parts := strings.Split(filePath, "/")
	return p.deleteTreePathRecursive(rootTreeHash, parts)
}

func (p *Persistence) deleteTreePathRecursive(treeHash plumbing.Hash, pathParts []string) (plumbing.Hash, error) {
	if len(pathParts) == 0 {
		return plumbing.ZeroHash, fmt.Errorf("empty path")
	}

	entries, err := p.getTreeEntries(treeHash)
	if err != nil {
		return plumbing.ZeroHash, err
	}

	name := pathParts[0]

	if len(pathParts) == 1 {
		// Delete the entry
		delete(entries, name)
	} else {
		// Recurse into subdirectory
		existing, ok := entries[name]
		if !ok || existing.Mode != filemode.Dir {
			// Path doesn't exist, nothing to delete
			return treeHash, nil
		}

		newSubTreeHash, err := p.deleteTreePathRecursive(existing.Hash, pathParts[1:])
		if err != nil {
			return plumbing.ZeroHash, err
		}

		if newSubTreeHash == plumbing.ZeroHash {
			// Subtree is now empty, remove directory entry
			delete(entries, name)
		} else {
			entries[name] = object.TreeEntry{
				Name: name,
				Mode: filemode.Dir,
				Hash: newSubTreeHash,
			}
		}
	}

	if len(entries) == 0 {
		return plumbing.ZeroHash, nil
	}

	// Convert map to slice and build new tree
	entrySlice := make([]object.TreeEntry, 0, len(entries))
	for _, entry := range entries {
		entrySlice = append(entrySlice, entry)
	}

	return p.buildTreeFromEntries(entrySlice)
}

// TreeChange represents a single change to apply to a tree
type TreeChange struct {
	Path     string        // File path (e.g., "db/table/key")
	BlobHash plumbing.Hash // Blob hash to set (ZeroHash = delete)
	IsDelete bool          // True if this is a deletion
}

// batchUpdateTree applies multiple changes to a tree in a single operation.
// This is more efficient than calling updateTreePath repeatedly because it
// builds the tree structure once instead of rebuilding intermediate trees.
func (p *Persistence) batchUpdateTree(rootTreeHash plumbing.Hash, changes []TreeChange) (plumbing.Hash, error) {
	if len(changes) == 0 {
		return rootTreeHash, nil
	}

	// Group changes by top-level directory
	grouped := make(map[string][]TreeChange)
	leafChanges := make([]TreeChange, 0)

	for _, change := range changes {
		parts := strings.Split(change.Path, "/")
		if len(parts) == 1 {
			// Leaf change at root level
			leafChanges = append(leafChanges, change)
		} else {
			// Group by first directory
			dir := parts[0]
			subChange := TreeChange{
				Path:     strings.Join(parts[1:], "/"),
				BlobHash: change.BlobHash,
				IsDelete: change.IsDelete,
			}
			grouped[dir] = append(grouped[dir], subChange)
		}
	}

	// Get current tree entries
	entries, err := p.getTreeEntries(rootTreeHash)
	if err != nil {
		return plumbing.ZeroHash, err
	}

	// Apply leaf changes at this level
	for _, change := range leafChanges {
		name := change.Path
		if change.IsDelete {
			delete(entries, name)
		} else {
			entries[name] = object.TreeEntry{
				Name: name,
				Mode: filemode.Regular,
				Hash: change.BlobHash,
			}
		}
	}

	// Recursively apply grouped changes to subdirectories
	for dir, subChanges := range grouped {
		var subTreeHash plumbing.Hash
		if existing, ok := entries[dir]; ok && existing.Mode == filemode.Dir {
			subTreeHash = existing.Hash
		} else {
			subTreeHash = plumbing.ZeroHash
		}

		newSubTreeHash, err := p.batchUpdateTree(subTreeHash, subChanges)
		if err != nil {
			return plumbing.ZeroHash, err
		}

		if newSubTreeHash == plumbing.ZeroHash {
			// Subtree is now empty, remove directory entry
			delete(entries, dir)
		} else {
			entries[dir] = object.TreeEntry{
				Name: dir,
				Mode: filemode.Dir,
				Hash: newSubTreeHash,
			}
		}
	}

	if len(entries) == 0 {
		return plumbing.ZeroHash, nil
	}

	// Convert map to slice and build new tree
	entrySlice := make([]object.TreeEntry, 0, len(entries))
	for _, entry := range entries {
		entrySlice = append(entrySlice, entry)
	}

	return p.buildTreeFromEntries(entrySlice)
}

// createCommitDirect creates a commit object directly without using worktree.
// If the new tree hash is identical to the current HEAD's tree hash, no commit
// is created and an empty Transaction is returned (avoiding empty commits).
func (p *Persistence) createCommitDirect(treeHash plumbing.Hash, identity core.Identity, message string) (Transaction, error) {
	// Handle empty tree case - create an actual empty tree object
	actualTreeHash := treeHash
	if treeHash == plumbing.ZeroHash {
		emptyTree := &object.Tree{Entries: []object.TreeEntry{}}
		obj := p.repo.Storer.NewEncodedObject()
		if err := emptyTree.Encode(obj); err != nil {
			return Transaction{}, fmt.Errorf("failed to encode empty tree: %w", err)
		}
		var err error
		actualTreeHash, err = p.repo.Storer.SetEncodedObject(obj)
		if err != nil {
			return Transaction{}, fmt.Errorf("failed to store empty tree: %w", err)
		}
	}

	// Get parent commit and check if tree has changed
	var parentHashes []plumbing.Hash
	headRef, err := p.repo.Head()
	if err == nil {
		parentHashes = []plumbing.Hash{headRef.Hash()}

		// Compare with current tree - skip commit if no changes
		currentTreeHash, err := p.getCurrentTree()
		if err == nil && currentTreeHash == actualTreeHash {
			// No changes - return empty transaction without creating commit
			return Transaction{}, nil
		}
	}

	sig := object.Signature{
		Name:  identity.Name,
		Email: identity.Email,
		When:  time.Now(),
	}

	commit := &object.Commit{
		Author:       sig,
		Committer:    sig,
		Message:      message,
		TreeHash:     actualTreeHash,
		ParentHashes: parentHashes,
	}

	obj := p.repo.Storer.NewEncodedObject()
	if err := commit.Encode(obj); err != nil {
		return Transaction{}, fmt.Errorf("failed to encode commit: %w", err)
	}

	commitHash, err := p.repo.Storer.SetEncodedObject(obj)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to store commit: %w", err)
	}

	// Update HEAD reference
	// Determine the branch name - use HEAD's target even if no commits exist
	branchName := plumbing.Master
	if headRef != nil && headRef.Name().IsBranch() {
		branchName = headRef.Name()
	} else {
		// HEAD might point to an unborn branch (no commits yet)
		// Try to resolve the symbolic HEAD to get the target branch
		head, err := p.repo.Storer.Reference(plumbing.HEAD)
		if err == nil && head.Type() == plumbing.SymbolicReference {
			branchName = head.Target()
		}
	}

	ref := plumbing.NewHashReference(branchName, commitHash)
	if err := p.repo.Storer.SetReference(ref); err != nil {
		return Transaction{}, fmt.Errorf("failed to update HEAD: %w", err)
	}

	return Transaction{
		Id:   commitHash.String(),
		When: sig.When,
	}, nil
}

// SaveRecordDirect saves records using low-level plumbing API (no worktree)
// Uses batch tree update for efficient multi-record operations
func (p *Persistence) SaveRecordDirect(database, table string, records map[string][]byte, identity core.Identity) (Transaction, error) {
	if err := p.ensureInitialized(); err != nil {
		return Transaction{}, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Get current tree
	currentTree, err := p.getCurrentTree()
	if err != nil {
		return Transaction{}, err
	}

	// Build list of changes
	changes := make([]TreeChange, 0, len(records))
	for key, data := range records {
		// Create blob
		blobHash, err := p.createBlob(data)
		if err != nil {
			return Transaction{}, fmt.Errorf("failed to create blob for %s: %w", key, err)
		}

		changes = append(changes, TreeChange{
			Path:     path.Join(database, table, key),
			BlobHash: blobHash,
			IsDelete: false,
		})
	}

	// Apply all changes in single tree operation
	newTree, err := p.batchUpdateTree(currentTree, changes)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to update tree: %w", err)
	}

	// Create commit
	txn, err := p.createCommitDirect(newTree, identity, "Saving record(s)")
	if err != nil {
		return Transaction{}, err
	}

	// Sync worktree to match the new commit (for read compatibility)
	if err := p.syncWorktree(); err != nil {
		return Transaction{}, fmt.Errorf("failed to sync worktree: %w", err)
	}

	return txn, nil
}

// syncWorktree updates the worktree filesystem to match HEAD
// For memory mode, this is skipped since reads use Git tree directly
func (p *Persistence) syncWorktree() error {
	// Skip sync in memory mode - reads go directly to Git tree
	if p.isMemoryMode {
		return nil
	}

	wt, err := p.repo.Worktree()
	if err != nil {
		return err
	}

	headRef, err := p.repo.Head()
	if err != nil {
		// No HEAD means empty repo - nothing to sync
		return nil
	}

	// Check if tree is empty (happens after DROP operations)
	commit, err := p.repo.CommitObject(headRef.Hash())
	if err != nil {
		return err
	}

	tree, err := commit.Tree()
	if err != nil {
		return err
	}

	// If tree is empty, manually clean the filesystem instead of reset
	// (git reset fails with "base dir cannot be removed" on empty tree)
	if len(tree.Entries) == 0 {
		// Remove all files from filesystem manually
		fs := wt.Filesystem
		entries, err := fs.ReadDir("/")
		if err != nil {
			return nil // Dir might not exist, that's fine
		}
		for _, entry := range entries {
			if entry.Name() != ".git" {
				fs.Remove(entry.Name())
			}
		}
		return nil
	}

	return wt.Reset(&git.ResetOptions{
		Mode:   git.HardReset,
		Commit: headRef.Hash(),
	})
}

// getRecordDirectUnlocked is the internal unlocked version of GetRecordDirect.
// Must be called with p.mu already held.
func (p *Persistence) getRecordDirectUnlocked(database, table, key string) ([]byte, bool) {
	headRef, err := p.repo.Head()
	if err != nil {
		return nil, false
	}

	commit, err := p.repo.CommitObject(headRef.Hash())
	if err != nil {
		return nil, false
	}

	tree, err := commit.Tree()
	if err != nil {
		return nil, false
	}

	filePath := path.Join(database, table, key)
	file, err := tree.File(filePath)
	if err != nil {
		return nil, false
	}

	content, err := file.Contents()
	if err != nil {
		return nil, false
	}

	return []byte(content), true
}

// GetRecordDirect reads a record directly from the Git tree (bypasses worktree filesystem)
func (p *Persistence) GetRecordDirect(database, table, key string) ([]byte, bool) {
	if !p.IsInitialized() {
		return nil, false
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	headRef, err := p.repo.Head()
	if err != nil {
		return nil, false
	}

	commit, err := p.repo.CommitObject(headRef.Hash())
	if err != nil {
		return nil, false
	}

	tree, err := commit.Tree()
	if err != nil {
		return nil, false
	}

	// Navigate to database/table/key
	filePath := path.Join(database, table, key)
	file, err := tree.File(filePath)
	if err != nil {
		return nil, false
	}

	content, err := file.Contents()
	if err != nil {
		return nil, false
	}

	return []byte(content), true
}

// DeleteRecordDirect deletes a record using low-level plumbing API
func (p *Persistence) DeleteRecordDirect(database, table, key string, identity core.Identity) (Transaction, error) {
	if err := p.ensureInitialized(); err != nil {
		return Transaction{}, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Get current tree
	currentTree, err := p.getCurrentTree()
	if err != nil {
		return Transaction{}, err
	}

	if currentTree == plumbing.ZeroHash {
		return Transaction{}, fmt.Errorf("no records exist")
	}

	// Delete from tree
	recordPath := path.Join(database, table, key)
	newTree, err := p.deleteTreePath(currentTree, recordPath)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to delete from tree: %w", err)
	}

	// Create commit
	txn, err := p.createCommitDirect(newTree, identity, "Deleting record")
	if err != nil {
		return Transaction{}, err
	}

	// Sync worktree
	if err := p.syncWorktree(); err != nil {
		return Transaction{}, fmt.Errorf("failed to sync worktree: %w", err)
	}

	return txn, nil
}

// CopyRecordsDirect copies records between tables using low-level plumbing API
// Uses batch tree update for efficient multi-record operations
func (p *Persistence) CopyRecordsDirect(srcDatabase, srcTable, dstDatabase, dstTable string, identity core.Identity) (Transaction, error) {
	if err := p.ensureInitialized(); err != nil {
		return Transaction{}, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Get source keys using internal unlocked method
	srcPath := fmt.Sprintf("%s/%s", srcDatabase, srcTable)
	entries, err := p.listEntriesDirectUnlocked(srcPath)
	if err != nil {
		return Transaction{}, nil // Nothing to copy
	}

	var keys []string
	for _, entry := range entries {
		if !entry.IsDir {
			keys = append(keys, entry.Name)
		}
	}

	if len(keys) == 0 {
		return Transaction{}, nil // Nothing to copy
	}

	// Get current tree
	currentTree, err := p.getCurrentTree()
	if err != nil {
		return Transaction{}, err
	}

	// Build list of changes
	changes := make([]TreeChange, 0, len(keys))
	for _, key := range keys {
		// Use internal unlocked method
		data, exists := p.getRecordDirectUnlocked(srcDatabase, srcTable, key)
		if !exists {
			continue
		}

		// Create blob
		blobHash, err := p.createBlob(data)
		if err != nil {
			return Transaction{}, fmt.Errorf("failed to create blob for %s: %w", key, err)
		}

		changes = append(changes, TreeChange{
			Path:     path.Join(dstDatabase, dstTable, key),
			BlobHash: blobHash,
			IsDelete: false,
		})
	}

	// Apply all changes in single tree operation
	newTree, err := p.batchUpdateTree(currentTree, changes)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to update tree: %w", err)
	}

	// Create commit
	txn, err := p.createCommitDirect(newTree, identity, "Copying records")
	if err != nil {
		return Transaction{}, err
	}

	// Sync worktree
	if err := p.syncWorktree(); err != nil {
		return Transaction{}, fmt.Errorf("failed to sync worktree: %w", err)
	}

	return txn, nil
}

// WriteFileDirect writes a single file to the repository using plumbing API
func (p *Persistence) WriteFileDirect(filePath string, data []byte, identity core.Identity, message string) (Transaction, error) {
	if err := p.ensureInitialized(); err != nil {
		return Transaction{}, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Get current tree
	currentTree, err := p.getCurrentTree()
	if err != nil {
		return Transaction{}, err
	}

	// Create blob
	blobHash, err := p.createBlob(data)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to create blob: %w", err)
	}

	// Update tree
	newTree, err := p.updateTreePath(currentTree, filePath, blobHash)
	if err != nil {
		return Transaction{}, fmt.Errorf("failed to update tree: %w", err)
	}

	// Create commit
	txn, err := p.createCommitDirect(newTree, identity, message)
	if err != nil {
		return Transaction{}, err
	}

	// Sync worktree
	if err := p.syncWorktree(); err != nil {
		return Transaction{}, fmt.Errorf("failed to sync worktree: %w", err)
	}

	return txn, nil
}

// DeletePathDirect deletes one or more paths from the repository using plumbing API
func (p *Persistence) DeletePathDirect(paths []string, identity core.Identity, message string) (Transaction, error) {
	if err := p.ensureInitialized(); err != nil {
		return Transaction{}, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Get current tree
	currentTree, err := p.getCurrentTree()
	if err != nil {
		return Transaction{}, err
	}

	if currentTree == plumbing.ZeroHash {
		return Transaction{}, fmt.Errorf("no content exists")
	}

	// Delete each path
	newTree := currentTree
	for _, filePath := range paths {
		newTree, err = p.deleteTreePath(newTree, filePath)
		if err != nil {
			return Transaction{}, fmt.Errorf("failed to delete %s: %w", filePath, err)
		}
	}

	// Create commit
	txn, err := p.createCommitDirect(newTree, identity, message)
	if err != nil {
		return Transaction{}, err
	}

	// Sync worktree
	if err := p.syncWorktree(); err != nil {
		return Transaction{}, fmt.Errorf("failed to sync worktree: %w", err)
	}

	return txn, nil
}

// ReadFileDirect reads a file directly from the Git tree (bypasses worktree filesystem)
func (p *Persistence) ReadFileDirect(filePath string) ([]byte, error) {
	if !p.IsInitialized() {
		return nil, fmt.Errorf("not initialized")
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	headRef, err := p.repo.Head()
	if err != nil {
		return nil, fmt.Errorf("no commits yet")
	}

	commit, err := p.repo.CommitObject(headRef.Hash())
	if err != nil {
		return nil, fmt.Errorf("failed to get commit: %w", err)
	}

	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("failed to get tree: %w", err)
	}

	file, err := tree.File(filePath)
	if err != nil {
		return nil, fmt.Errorf("file not found: %w", err)
	}

	content, err := file.Contents()
	if err != nil {
		return nil, fmt.Errorf("failed to read contents: %w", err)
	}

	return []byte(content), nil
}

// TreeEntry represents a directory entry from the Git tree
type TreeEntry struct {
	Name  string
	IsDir bool
}

// listEntriesDirectUnlocked is the internal unlocked version of ListEntriesDirect.
// Must be called with p.mu already held.
func (p *Persistence) listEntriesDirectUnlocked(dirPath string) ([]TreeEntry, error) {
	headRef, err := p.repo.Head()
	if err != nil {
		return nil, nil // No commits yet = empty directory
	}

	commit, err := p.repo.CommitObject(headRef.Hash())
	if err != nil {
		return nil, fmt.Errorf("failed to get commit: %w", err)
	}

	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("failed to get tree: %w", err)
	}

	var targetTree *object.Tree
	if dirPath == "" || dirPath == "." {
		targetTree = tree
	} else {
		targetTree, err = tree.Tree(dirPath)
		if err != nil {
			return nil, nil // Directory doesn't exist = empty
		}
	}

	var entries []TreeEntry
	for _, entry := range targetTree.Entries {
		entries = append(entries, TreeEntry{
			Name:  entry.Name,
			IsDir: entry.Mode == filemode.Dir,
		})
	}

	return entries, nil
}

// ListEntriesDirect lists directory entries directly from the Git tree
func (p *Persistence) ListEntriesDirect(dirPath string) ([]TreeEntry, error) {
	if !p.IsInitialized() {
		return nil, fmt.Errorf("not initialized")
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	headRef, err := p.repo.Head()
	if err != nil {
		return nil, nil // No commits yet = empty directory
	}

	commit, err := p.repo.CommitObject(headRef.Hash())
	if err != nil {
		return nil, fmt.Errorf("failed to get commit: %w", err)
	}

	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("failed to get tree: %w", err)
	}

	// If dirPath is empty or ".", list root entries
	var targetTree *object.Tree
	if dirPath == "" || dirPath == "." {
		targetTree = tree
	} else {
		targetTree, err = tree.Tree(dirPath)
		if err != nil {
			return nil, nil // Directory doesn't exist = empty
		}
	}

	var entries []TreeEntry
	for _, entry := range targetTree.Entries {
		entries = append(entries, TreeEntry{
			Name:  entry.Name,
			IsDir: entry.Mode == filemode.Dir,
		})
	}

	return entries, nil
}

// resolveTransaction converts a transaction ID (commit hash) to a commit object.
// Supports both full and abbreviated commit hashes.
func (p *Persistence) resolveTransaction(transactionID string) (*object.Commit, error) {
	hash := plumbing.NewHash(transactionID)

	// Try exact hash first
	commit, err := p.repo.CommitObject(hash)
	if err == nil {
		return commit, nil
	}

	// For short hashes, try to resolve by iterating commits
	if len(transactionID) >= 4 && len(transactionID) < 40 {
		iter, err := p.repo.Log(&git.LogOptions{All: true})
		if err != nil {
			return nil, fmt.Errorf("failed to iterate commits: %w", err)
		}
		defer iter.Close()

		var found *object.Commit
		err = iter.ForEach(func(c *object.Commit) error {
			if strings.HasPrefix(c.Hash.String(), transactionID) {
				found = c
				return fmt.Errorf("found") // break iteration
			}
			return nil
		})

		if found != nil {
			return found, nil
		}
	}

	return nil, fmt.Errorf("transaction not found: %s", transactionID)
}

// GetRecordAtTransaction reads a record as it existed at a specific transaction (commit).
func (p *Persistence) GetRecordAtTransaction(database, table, key, transactionID string) ([]byte, bool, error) {
	if !p.IsInitialized() {
		return nil, false, nil
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	commit, err := p.resolveTransaction(transactionID)
	if err != nil {
		return nil, false, err
	}

	tree, err := commit.Tree()
	if err != nil {
		return nil, false, fmt.Errorf("failed to get tree: %w", err)
	}

	// Navigate to database/table/key
	filePath := path.Join(database, table, key)
	file, err := tree.File(filePath)
	if err != nil {
		return nil, false, nil // Record doesn't exist at this transaction
	}

	content, err := file.Contents()
	if err != nil {
		return nil, false, fmt.Errorf("failed to read file: %w", err)
	}

	return []byte(content), true, nil
}

// ListRecordsAtTransaction lists all records in a table as they existed at a specific transaction.
func (p *Persistence) ListRecordsAtTransaction(database, table, transactionID string) ([]string, error) {
	if !p.IsInitialized() {
		return nil, nil
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	commit, err := p.resolveTransaction(transactionID)
	if err != nil {
		return nil, err
	}

	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("failed to get tree: %w", err)
	}

	// Navigate to database/table directory
	dirPath := path.Join(database, table)
	tableTree, err := tree.Tree(dirPath)
	if err != nil {
		return nil, nil // Table doesn't exist at this transaction
	}

	var keys []string
	for _, entry := range tableTree.Entries {
		if entry.Mode.IsFile() {
			keys = append(keys, entry.Name)
		}
	}

	return keys, nil
}

// GetTableAtTransaction retrieves table metadata as it existed at a specific transaction.
func (p *Persistence) GetTableAtTransaction(database, table, transactionID string) (*core.Table, error) {
	if !p.IsInitialized() {
		return nil, fmt.Errorf("persistence not initialized")
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	commit, err := p.resolveTransaction(transactionID)
	if err != nil {
		return nil, err
	}

	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("failed to get tree: %w", err)
	}

	// Read table metadata from database/table.table (matches crud.go storage pattern)
	metaPath := path.Join(database, table+".table")
	file, err := tree.File(metaPath)
	if err != nil {
		return nil, fmt.Errorf("table not found at transaction %s", transactionID)
	}

	content, err := file.Contents()
	if err != nil {
		return nil, fmt.Errorf("failed to read table metadata: %w", err)
	}

	var tbl core.Table
	if err := json.Unmarshal([]byte(content), &tbl); err != nil {
		return nil, fmt.Errorf("failed to parse table metadata: %w", err)
	}

	return &tbl, nil
}
