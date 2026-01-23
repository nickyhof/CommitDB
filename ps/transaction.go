package ps

import (
	"fmt"
	"time"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
)

type Transaction struct {
	Id   string
	When time.Time
}

func (transaction Transaction) String() string {
	return fmt.Sprintf("Transaction{Id: %s, When: %s}", transaction.Id, transaction.When)
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

	return Transaction{
		Id:   headRef.Hash().String(),
		When: commit.Committer.When,
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
