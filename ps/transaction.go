package ps

import (
	"fmt"
	"time"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
)

type Transaction struct {
	Id     string
	When   time.Time
	Author string // "Name <email>" format
}

func (transaction Transaction) String() string {
	return fmt.Sprintf("Transaction{Id: %s, When: %s, Author: %s}", transaction.Id, transaction.When, transaction.Author)
}

func (persistence *Persistence) LatestTransaction() Transaction {
	headRef, err := persistence.repo.Head()
	if err != nil || headRef == nil {
		// No commits yet
		return Transaction{}
	}

	commit, err := persistence.repo.CommitObject(headRef.Hash())
	if err != nil {
		return Transaction{}
	}

	author := ""
	if commit.Author.Name != "" || commit.Author.Email != "" {
		author = fmt.Sprintf("%s <%s>", commit.Author.Name, commit.Author.Email)
	}

	return Transaction{
		Id:     headRef.Hash().String(),
		When:   commit.Committer.When,
		Author: author,
	}
}

func (persistence *Persistence) TransactionsSince(asof time.Time) []Transaction {
	var transactions []Transaction

	cIter, _ := persistence.repo.Log(&git.LogOptions{
		Since: &asof,
	})

	cIter.ForEach(func(c *object.Commit) error {
		transactions = append(transactions, Transaction{
			Id:   c.Hash.String(),
			When: c.Committer.When,
		})
		return nil
	})

	return transactions
}

func (persistence *Persistence) TransactionsFrom(asof string) []Transaction {
	var transactions []Transaction

	cIter, _ := persistence.repo.Log(&git.LogOptions{
		From: plumbing.NewHash(asof),
	})

	cIter.ForEach(func(c *object.Commit) error {
		transactions = append(transactions, Transaction{
			Id:   c.Hash.String(),
			When: c.Committer.When,
		})
		return nil
	})

	return transactions
}
