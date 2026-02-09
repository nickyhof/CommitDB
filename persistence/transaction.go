package persistence

import (
	"fmt"
	"time"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
)

type Transaction struct {
	ID      string
	When    time.Time
	Author  string // "Name <email>" format
	Message string // Commit message
}

func (transaction Transaction) String() string {
	return fmt.Sprintf("Transaction{ID: %s, When: %s, Author: %s}", transaction.ID, transaction.When, transaction.Author)
}

func (p *Persistence) LatestTransaction() Transaction {
	headRef, err := p.repo.Head()
	if err != nil || headRef == nil {
		// No commits yet
		return Transaction{}
	}

	commit, err := p.repo.CommitObject(headRef.Hash())
	if err != nil {
		return Transaction{}
	}

	author := ""
	if commit.Author.Name != "" || commit.Author.Email != "" {
		author = fmt.Sprintf("%s <%s>", commit.Author.Name, commit.Author.Email)
	}

	return Transaction{
		ID:     headRef.Hash().String(),
		When:   commit.Committer.When,
		Author: author,
	}
}

func (p *Persistence) TransactionsSince(asof time.Time) []Transaction {
	var transactions []Transaction

	cIter, _ := p.repo.Log(&git.LogOptions{
		Since: &asof,
	})

	_ = cIter.ForEach(func(c *object.Commit) error {
		transactions = append(transactions, Transaction{
			ID:   c.Hash.String(),
			When: c.Committer.When,
		})
		return nil
	})

	return transactions
}

func (p *Persistence) TransactionsFrom(asof string) []Transaction {
	var transactions []Transaction

	cIter, _ := p.repo.Log(&git.LogOptions{
		From: plumbing.NewHash(asof),
	})

	_ = cIter.ForEach(func(c *object.Commit) error {
		transactions = append(transactions, Transaction{
			ID:   c.Hash.String(),
			When: c.Committer.When,
		})
		return nil
	})

	return transactions
}

// ListTransactions returns the most recent transactions (commits)
func (p *Persistence) ListTransactions(limit int) ([]Transaction, error) {
	if err := p.ensureInitialized(); err != nil {
		return nil, err
	}

	headRef, err := p.repo.Head()
	if err != nil {
		// No commits yet
		return []Transaction{}, nil
	}

	var transactions []Transaction

	cIter, err := p.repo.Log(&git.LogOptions{
		From: headRef.Hash(),
	})
	if err != nil {
		return nil, err
	}

	count := 0
	_ = cIter.ForEach(func(c *object.Commit) error {
		if count >= limit {
			return fmt.Errorf("stop")
		}
		author := ""
		if c.Author.Name != "" || c.Author.Email != "" {
			author = fmt.Sprintf("%s <%s>", c.Author.Name, c.Author.Email)
		}
		transactions = append(transactions, Transaction{
			ID:      c.Hash.String(),
			When:    c.Committer.When,
			Author:  author,
			Message: c.Message,
		})
		count++
		return nil
	})

	return transactions, nil
}
