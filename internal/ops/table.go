package ops

import (
	"errors"
	"iter"
	"strconv"

	"github.com/nickyhof/CommitDB/v2/internal/core"
	"github.com/nickyhof/CommitDB/v2/internal/persistence"
)

type TableOp struct {
	Table       core.Table
	Persistence *persistence.Persistence
}

func CreateTable(table core.Table, p *persistence.Persistence, identity core.Identity) (*persistence.Transaction, *TableOp, error) {
	txn, err := p.CreateTable(table, identity)
	if err != nil {
		return nil, nil, err
	}

	return &txn, &TableOp{
		Table:       table,
		Persistence: p,
	}, nil
}

func GetTable(database string, tableName string, p *persistence.Persistence) (*TableOp, error) {
	table, err := p.GetTable(database, tableName)

	if err != nil {
		return nil, err
	}

	return &TableOp{
		Table:       *table,
		Persistence: p,
	}, nil
}

func (op *TableOp) PrimaryKey() (pk *string, err error) {
	for _, col := range op.Table.Columns {
		if col.PrimaryKey {
			return &col.Name, nil
		}
	}

	return nil, errors.New("no primary key found")
}

func (op *TableOp) DropTable(identity core.Identity) (txn persistence.Transaction, err error) {
	return op.Persistence.DropTable(op.Table.Database, op.Table.Name, identity)
}

func (op *TableOp) Get(key string) (value []byte, exists bool) {
	value, exists = op.Persistence.GetRecord(op.Table.Database, op.Table.Name, key)
	return
}

func (op *TableOp) GetString(key string) (valueAsString string, exists bool) {
	var value []byte
	value, exists = op.Get(key)
	if exists {
		valueAsString = string(value)
	}
	return
}

func (op *TableOp) GetInt(key string) (valueAsInt int, exists bool, err error) {
	var value string
	value, exists = op.GetString(key)
	if exists {
		valueAsInt, err = strconv.Atoi(value)
	}
	return
}

func (op *TableOp) Put(key string, value []byte, identity core.Identity) (txn persistence.Transaction, err error) {
	records := map[string][]byte{
		key: value,
	}
	return op.PutAll(records, identity)
}

func (op *TableOp) PutAll(records map[string][]byte, identity core.Identity) (txn persistence.Transaction, err error) {
	return op.Persistence.SaveRecord(op.Table.Database, op.Table.Name, records, identity)
}

func (op *TableOp) Delete(key string, identity core.Identity) (txn persistence.Transaction, err error) {
	return op.Persistence.DeleteRecord(op.Table.Database, op.Table.Name, key, identity)
}

func (op *TableOp) Count() int {
	return len(op.Keys())
}

func (op *TableOp) Keys() []string {
	return op.Persistence.ListRecordKeys(op.Table.Database, op.Table.Name)
}

func (op *TableOp) Scan() iter.Seq2[string, []byte] {
	return op.Persistence.Scan(op.Table.Database, op.Table.Name, nil)
}

func (op *TableOp) ScanWithFilter(filterExpr func(key string, value []byte) bool) iter.Seq2[string, []byte] {
	return op.Persistence.Scan(op.Table.Database, op.Table.Name, &filterExpr)
}

func (op *TableOp) Restore(asof persistence.Transaction) error {
	return op.Persistence.Restore(asof, &op.Table.Database, &op.Table.Name)
}

// CopyFrom copies all records from source table into this table in a single atomic transaction.
// Memory-efficient: records are streamed row-by-row without loading all into Go memory.
func (op *TableOp) CopyFrom(source *TableOp, identity core.Identity) (txn persistence.Transaction, err error) {
	return op.Persistence.CopyRecords(
		source.Table.Database, source.Table.Name,
		op.Table.Database, op.Table.Name,
		identity,
	)
}
