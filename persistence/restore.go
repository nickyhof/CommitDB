package persistence

import (
	"fmt"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
)

func (p *Persistence) Snapshot(name string, asof *Transaction) error {
	if asof != nil {
		_, err := p.repo.CreateTag(name, plumbing.NewHash(asof.ID), nil)

		return err
	} else {
		headRef, _ := p.repo.Head()

		_, err := p.repo.CreateTag(name, headRef.Hash(), nil)

		return err
	}
}

func (p *Persistence) Recover(name string) error {
	fmt.Println("Recovering to snapshot:", name)

	wt, err := p.repo.Worktree()
	if err != nil {
		return fmt.Errorf("failed to get worktree: %w", err)
	}

	ref, err := p.repo.Tag(name)
	if err != nil {
		return fmt.Errorf("snapshot not found: %w", err)
	}

	return wt.Reset(&git.ResetOptions{
		Mode:   git.HardReset,
		Commit: ref.Hash(),
	})
}

func (p *Persistence) Restore(asof Transaction, database *string, table *string) error {
	fmt.Println("Restoring to transaction:", asof.ID)

	wt, err := p.repo.Worktree()
	if err != nil {
		return fmt.Errorf("failed to get worktree: %w", err)
	}

	sparseDirs := []string{}
	if database != nil && table != nil {
		sparseDirs = append(sparseDirs, fmt.Sprintf("%s/%s", *database, *table))
	} else if database != nil {
		sparseDirs = append(sparseDirs, *database)
	}

	return wt.Reset(&git.ResetOptions{
		Mode:       git.HardReset,
		Commit:     plumbing.NewHash(asof.ID),
		SparseDirs: sparseDirs,
	})
}
