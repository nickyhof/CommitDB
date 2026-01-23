package op

import (
	"github.com/nickyhof/CommitDB/core"
	"github.com/nickyhof/CommitDB/ps"
)

type DatabaseOp struct {
	Database    core.Database
	Persistence *ps.Persistence
}

func CreateDatabase(database core.Database, persistence *ps.Persistence, identity core.Identity) (*ps.Transaction, *DatabaseOp, error) {
	txn, err := persistence.CreateDatabase(database, identity)
	if err != nil {
		return nil, nil, err
	}

	return &txn, &DatabaseOp{
		Database:    database,
		Persistence: persistence,
	}, nil
}

func GetDatabase(name string, persistence *ps.Persistence) (*DatabaseOp, error) {
	d, err := persistence.GetDatabase(name)
	if err != nil {
		return nil, err
	}
	return &DatabaseOp{
		Database:    *d,
		Persistence: persistence,
	}, nil
}

func (op *DatabaseOp) DropDatabase(identity core.Identity) (txn ps.Transaction, err error) {
	return op.Persistence.DropDatabase(op.Database.Name, identity)
}

func (op *DatabaseOp) TableNames() []string {
	return op.Persistence.ListTables(op.Database.Name)
}

func (op *DatabaseOp) Restore(asof ps.Transaction) error {
	return op.Persistence.Restore(asof, &op.Database.Name, nil)
}
