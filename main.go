package CommitDB

import (
	"github.com/nickyhof/CommitDB/core"
	"github.com/nickyhof/CommitDB/db"
	"github.com/nickyhof/CommitDB/ps"
)

type Instance struct {
	Persistence *ps.Persistence
}

func Open(persistence *ps.Persistence) *Instance {
	return &Instance{
		Persistence: persistence,
	}
}

func (instance *Instance) Engine(identity core.Identity) *db.Engine {
	return db.NewEngine(instance.Persistence, identity)
}
