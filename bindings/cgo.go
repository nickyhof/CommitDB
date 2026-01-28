package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"encoding/json"
	"unsafe"

	"github.com/nickyhof/CommitDB"
	"github.com/nickyhof/CommitDB/core"
	"github.com/nickyhof/CommitDB/db"
	"github.com/nickyhof/CommitDB/ps"
)

// Handle represents an open database instance
type Handle struct {
	instance *CommitDB.Instance
	engine   *db.Engine
}

// Global handle storage (simplified - in production use a map with mutex)
var handles = make(map[int]*Handle)
var nextHandle = 1

// Response mirrors the server protocol for consistency
type Response struct {
	Success bool            `json:"success"`
	Error   string          `json:"error,omitempty"`
	Type    string          `json:"type,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
}

type QueryResponse struct {
	Columns         []string   `json:"columns"`
	Data            [][]string `json:"data"`
	RecordsRead     int        `json:"records_read"`
	ExecutionTimeMs float64    `json:"execution_time_ms"`
	ExecutionOps    int        `json:"execution_ops"`
}

type CommitResponse struct {
	DatabasesCreated int     `json:"databases_created,omitempty"`
	DatabasesDeleted int     `json:"databases_deleted,omitempty"`
	TablesCreated    int     `json:"tables_created,omitempty"`
	TablesDeleted    int     `json:"tables_deleted,omitempty"`
	RecordsWritten   int     `json:"records_written,omitempty"`
	RecordsDeleted   int     `json:"records_deleted,omitempty"`
	ExecutionTimeMs  float64 `json:"execution_time_ms"`
	ExecutionOps     int     `json:"execution_ops"`
}

//export commitdb_open_memory
func commitdb_open_memory() C.int {
	persistence, err := ps.NewMemoryPersistence()
	if err != nil {
		return -1
	}

	instance := CommitDB.Open(&persistence)
	engine := instance.Engine(core.Identity{
		Name:  "CommitDB Python",
		Email: "python@commitdb.local",
	})

	handle := nextHandle
	nextHandle++
	handles[handle] = &Handle{
		instance: instance,
		engine:   engine,
	}

	return C.int(handle)
}

//export commitdb_open_file
func commitdb_open_file(path *C.char) C.int {
	goPath := C.GoString(path)

	persistence, err := ps.NewFilePersistence(goPath, nil)
	if err != nil {
		return -1
	}

	instance := CommitDB.Open(&persistence)
	engine := instance.Engine(core.Identity{
		Name:  "CommitDB Python",
		Email: "python@commitdb.local",
	})

	handle := nextHandle
	nextHandle++
	handles[handle] = &Handle{
		instance: instance,
		engine:   engine,
	}

	return C.int(handle)
}

//export commitdb_close
func commitdb_close(handle C.int) {
	delete(handles, int(handle))
}

//export commitdb_execute
func commitdb_execute(handle C.int, query *C.char) *C.char {
	h, ok := handles[int(handle)]
	if !ok {
		return makeErrorResponse("Invalid handle")
	}

	goQuery := C.GoString(query)
	result, err := h.engine.Execute(goQuery)

	if err != nil {
		return makeErrorResponse(err.Error())
	}

	var resp Response

	switch r := result.(type) {
	case db.QueryResult:
		qr := QueryResponse{
			Columns:         r.Columns,
			Data:            r.Data,
			RecordsRead:     r.RecordsRead,
			ExecutionTimeMs: r.ExecutionTimeMs,
			ExecutionOps:    r.ExecutionOps,
		}
		data, _ := json.Marshal(qr)
		resp = Response{
			Success: true,
			Type:    "query",
			Result:  data,
		}

	case db.CommitResult:
		cr := CommitResponse{
			DatabasesCreated: r.DatabasesCreated,
			DatabasesDeleted: r.DatabasesDeleted,
			TablesCreated:    r.TablesCreated,
			TablesDeleted:    r.TablesDeleted,
			RecordsWritten:   r.RecordsWritten,
			RecordsDeleted:   r.RecordsDeleted,
			ExecutionTimeMs:  r.ExecutionTimeMs,
			ExecutionOps:     r.ExecutionOps,
		}
		data, _ := json.Marshal(cr)
		resp = Response{
			Success: true,
			Type:    "commit",
			Result:  data,
		}

	default:
		resp = Response{
			Success: true,
			Type:    "unknown",
		}
	}

	jsonData, _ := json.Marshal(resp)
	return C.CString(string(jsonData))
}

//export commitdb_free
func commitdb_free(ptr *C.char) {
	C.free(unsafe.Pointer(ptr))
}

func makeErrorResponse(msg string) *C.char {
	resp := Response{
		Success: false,
		Error:   msg,
	}
	jsonData, _ := json.Marshal(resp)
	return C.CString(string(jsonData))
}

func main() {}
