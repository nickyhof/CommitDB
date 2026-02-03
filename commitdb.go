package commitdb

import (
	"github.com/nickyhof/CommitDB/internal/core"
	"github.com/nickyhof/CommitDB/internal/engine"
	"github.com/nickyhof/CommitDB/internal/persistence"
)

type Instance struct {
	Persistence *persistence.Persistence
}

func Open(p *persistence.Persistence) *Instance {
	return &Instance{
		Persistence: p,
	}
}

func (instance *Instance) Engine(identity core.Identity) *engine.Engine {
	return engine.NewEngine(instance.Persistence, identity)
}
