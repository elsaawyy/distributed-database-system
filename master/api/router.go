package api

import (
	"distributed-db/master/coordinator"
	"distributed-db/master/health"
	"distributed-db/master/metadata"
	"distributed-db/master/replication"
	"distributed-db/master/sharding"

	"github.com/gorilla/mux"
)

type Handler struct {
	coord          *coordinator.Coordinator
	metaManager    *metadata.Manager
	healthMon      *health.Monitor
	chunkSplitter  *sharding.ChunkSplitter
	consistentHash *sharding.ConsistentHash
	auth           *AuthMiddleware
	replicaMgr     *replication.ReplicaManager
}

func NewHandler(coord *coordinator.Coordinator, metaMgr *metadata.Manager,
	healthMon *health.Monitor, chunkSplitter *sharding.ChunkSplitter,
	consistentHash *sharding.ConsistentHash, apiKey string,
	replicaMgr *replication.ReplicaManager) *Handler {
	return &Handler{
		coord:          coord,
		metaManager:    metaMgr,
		healthMon:      healthMon,
		chunkSplitter:  chunkSplitter,
		consistentHash: consistentHash,
		auth:           NewAuthMiddleware(apiKey),
		replicaMgr:     replicaMgr,
	}
}

func (h *Handler) RegisterRoutes(r *mux.Router) {
	// Database operations
	r.HandleFunc("/v1/db/create", h.auth.Validate(h.CreateDatabase)).Methods("POST")
	r.HandleFunc("/v1/db/drop", h.auth.Validate(h.DropDatabase)).Methods("POST")

	// NEW - List databases
	r.HandleFunc("/v1/databases", h.auth.Validate(h.ListDatabases)).Methods("GET")

	// Table operations
	r.HandleFunc("/v1/table/create", h.auth.Validate(h.CreateTable)).Methods("POST")
	r.HandleFunc("/v1/table/drop", h.auth.Validate(h.DropTable)).Methods("POST")

	// NEW - List tables (supports both query param and path)
	r.HandleFunc("/v1/tables", h.auth.Validate(h.ListTables)).Methods("GET")
	r.HandleFunc("/v1/tables/{db}", h.auth.Validate(h.ListTables)).Methods("GET")

	// NEW - Get table schema
	r.HandleFunc("/v1/schema/{db}/{table}", h.auth.Validate(h.GetTableSchema)).Methods("GET")

	// Data operations
	r.HandleFunc("/v1/insert", h.auth.Validate(h.Insert)).Methods("POST")
	r.HandleFunc("/v1/select", h.auth.Validate(h.Select)).Methods("POST")
	r.HandleFunc("/v1/update", h.auth.Validate(h.Update)).Methods("POST")
	r.HandleFunc("/v1/delete", h.auth.Validate(h.Delete)).Methods("POST")

	// File upload & MapReduce
	r.HandleFunc("/v1/upload-file", h.auth.Validate(h.UploadFile)).Methods("POST")
	r.HandleFunc("/v1/mapreduce/wordcount", h.auth.Validate(h.WordCountMapReduce)).Methods("POST")

	// System
	r.HandleFunc("/v1/health", h.HealthCheck).Methods("GET")
	r.HandleFunc("/v1/workers", h.GetWorkers).Methods("GET")
}
