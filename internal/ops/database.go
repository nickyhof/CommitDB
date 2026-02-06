package ops

import (
	"github.com/nickyhof/CommitDB/v2/internal/core"
	"github.com/nickyhof/CommitDB/v2/internal/persistence"
)

type DatabaseOp struct {
	Database    core.Database
	Persistence *persistence.Persistence
}

func CreateDatabase(database core.Database, p *persistence.Persistence, identity core.Identity) (*persistence.Transaction, *DatabaseOp, error) {
	txn, err := p.CreateDatabase(database, identity)
	if err != nil {
		return nil, nil, err
	}

	return &txn, &DatabaseOp{
		Database:    database,
		Persistence: p,
	}, nil
}

func GetDatabase(name string, p *persistence.Persistence) (*DatabaseOp, error) {
	d, err := p.GetDatabase(name)
	if err != nil {
		return nil, err
	}
	return &DatabaseOp{
		Database:    *d,
		Persistence: p,
	}, nil
}

func (op *DatabaseOp) DropDatabase(identity core.Identity) (txn persistence.Transaction, err error) {
	return op.Persistence.DropDatabase(op.Database.Name, identity)
}

func (op *DatabaseOp) TableNames() []string {
	return op.Persistence.ListTables(op.Database.Name)
}

func (op *DatabaseOp) Restore(asof persistence.Transaction) error {
	return op.Persistence.Restore(asof, &op.Database.Name, nil)
}
