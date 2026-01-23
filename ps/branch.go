package ps

import (
	"fmt"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/nickyhof/CommitDB/core"
)

// Branch creates a new branch at current HEAD or at a specific transaction
func (p *Persistence) Branch(name string, from *Transaction) error {
	if err := p.ensureInitialized(); err != nil {
		return err
	}

	var hash plumbing.Hash

	if from != nil {
		hash = plumbing.NewHash(from.Id)
	} else {
		headRef, err := p.repo.Head()
		if err != nil {
			return fmt.Errorf("failed to get HEAD: %w", err)
		}
		hash = headRef.Hash()
	}

	branchRef := plumbing.NewBranchReferenceName(name)
	ref := plumbing.NewHashReference(branchRef, hash)

	return p.repo.Storer.SetReference(ref)
}

// Checkout switches to an existing branch
func (p *Persistence) Checkout(name string) error {
	if err := p.ensureInitialized(); err != nil {
		return err
	}

	wt, err := p.repo.Worktree()
	if err != nil {
		return fmt.Errorf("failed to get worktree: %w", err)
	}

	branchRef := plumbing.NewBranchReferenceName(name)

	return wt.Checkout(&git.CheckoutOptions{
		Branch: branchRef,
	})
}

// Merge merges the source branch into the current branch.
// Uses row-level merge strategy for diverged branches (Last-Writer-Wins).
func (p *Persistence) Merge(source string, identity core.Identity) (Transaction, error) {
	result, err := p.MergeWithOptions(source, identity, DefaultMergeOptions())
	if err != nil {
		return Transaction{}, err
	}
	return result.Transaction, nil
}

// ListBranches returns all branch names
func (p *Persistence) ListBranches() ([]string, error) {
	if err := p.ensureInitialized(); err != nil {
		return nil, err
	}

	branches := []string{}

	refs, err := p.repo.Branches()
	if err != nil {
		return nil, fmt.Errorf("failed to list branches: %w", err)
	}

	refs.ForEach(func(ref *plumbing.Reference) error {
		branches = append(branches, ref.Name().Short())
		return nil
	})

	return branches, nil
}

// CurrentBranch returns the name of the current branch
func (p *Persistence) CurrentBranch() (string, error) {
	if err := p.ensureInitialized(); err != nil {
		return "", err
	}

	headRef, err := p.repo.Head()
	if err != nil {
		return "", fmt.Errorf("failed to get HEAD: %w", err)
	}

	if headRef.Name().IsBranch() {
		return headRef.Name().Short(), nil
	}

	// Detached HEAD
	return "", fmt.Errorf("HEAD is detached at %s", headRef.Hash().String()[:7])
}

// DeleteBranch deletes a branch
func (p *Persistence) DeleteBranch(name string) error {
	if err := p.ensureInitialized(); err != nil {
		return err
	}

	// Check if trying to delete current branch
	currentBranch, err := p.CurrentBranch()
	if err == nil && currentBranch == name {
		return fmt.Errorf("cannot delete the currently checked out branch '%s'", name)
	}

	branchRef := plumbing.NewBranchReferenceName(name)

	return p.repo.Storer.RemoveReference(branchRef)
}

// RenameBranch renames a branch
func (p *Persistence) RenameBranch(oldName, newName string) error {
	if err := p.ensureInitialized(); err != nil {
		return err
	}

	cfg, err := p.repo.Config()
	if err != nil {
		return err
	}

	// Add new branch config
	cfg.Branches[newName] = cfg.Branches[oldName]

	// Get the old branch hash
	oldBranchRef := plumbing.NewBranchReferenceName(oldName)
	oldRef, err := p.repo.Reference(oldBranchRef, true)
	if err != nil {
		return fmt.Errorf("branch '%s' not found: %w", oldName, err)
	}

	// Create new reference
	newBranchRef := plumbing.NewBranchReferenceName(newName)
	newRef := plumbing.NewHashReference(newBranchRef, oldRef.Hash())
	if err := p.repo.Storer.SetReference(newRef); err != nil {
		return err
	}

	// Delete old reference
	if err := p.repo.Storer.RemoveReference(oldBranchRef); err != nil {
		return err
	}

	// Update config
	delete(cfg.Branches, oldName)

	return p.repo.SetConfig(cfg)
}
