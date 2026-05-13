package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"distributed-db/master/health"
	"distributed-db/master/metadata"
	"distributed-db/master/sharding"
)

// Request/response structures
type CreateDBReq struct {
	Name string `json:"name"`
}

type CreateTableReq struct {
	DBName    string `json:"db_name"`
	TableName string `json:"table_name"`
	Schema    string `json:"schema"` // e.g., "id INT PRIMARY KEY, name VARCHAR(100)"
	ShardKey  string `json:"shard_key"`
}

type InsertReq struct {
	DBName    string                   `json:"db_name"`
	TableName string                   `json:"table_name"`
	Rows      []map[string]interface{} `json:"rows"`
}

type SelectReq struct {
	DBName    string `json:"db_name"`
	TableName string `json:"table_name"`
	Query     string `json:"query"` // e.g., "SELECT COUNT(*) FROM users WHERE age > 30"
}

func (h *Handler) CreateDatabase(w http.ResponseWriter, r *http.Request) {
	var req CreateDBReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if err := h.metaManager.CreateDatabase(req.Name); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "created"})
}

// CreateTable
func (h *Handler) CreateTable(w http.ResponseWriter, r *http.Request) {
	var req CreateTableReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	// Store metadata
	table := &metadata.Table{
		DBName:      req.DBName,
		TableName:   req.TableName,
		SchemaSQL:   req.Schema,
		ShardKey:    req.ShardKey,
		ShardCount:  h.consistentHash.NumVirtualNodes,
		ReplicaFact: h.replicaMgr.ReplicationFactor,
	}
	if err := h.metaManager.CreateTable(table); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Create table on ALL alive workers
	workers := h.healthMon.GetAliveWorkers()
	var wg sync.WaitGroup
	for _, worker := range workers {
		wg.Add(1)
		go func(wkr health.WorkerInfo) {
			defer wg.Done()
			createReq := map[string]interface{}{
				"db_name":    req.DBName,
				"table_name": req.TableName,
				"schema":     req.Schema,
			}
			data, _ := json.Marshal(createReq)
			resp, err := http.Post(wkr.URL+"/create_table", "application/json", bytes.NewReader(data))
			if err != nil {
				log.Printf("Failed to create table on worker %s: %v", wkr.ID, err)
			} else {
				resp.Body.Close()
			}
		}(worker)
	}
	wg.Wait()

	// **NEW: Automatically assign shards to workers**
	shardIDs := make([]int, table.ShardCount)
	for i := 0; i < table.ShardCount; i++ {
		shardIDs[i] = i
	}

	// Assign shards to workers (simple round-robin)
	for i, shardID := range shardIDs {
		workerIdx := i % len(workers)
		primaryWorker := workers[workerIdx]

		// Register primary
		h.metaManager.RegisterShard(req.DBName, req.TableName, shardID, primaryWorker.ID, true)

		// Register replica (if replication factor > 1)
		if h.replicaMgr.ReplicationFactor > 1 && len(workers) > 1 {
			replicaIdx := (workerIdx + 1) % len(workers)
			replicaWorker := workers[replicaIdx]
			h.metaManager.RegisterShard(req.DBName, req.TableName, shardID, replicaWorker.ID, false)
		}
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "table created"})
}

// Insert - routes rows to appropriate worker(s)
func (h *Handler) Insert(w http.ResponseWriter, r *http.Request) {
	var req InsertReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	// Get table metadata
	table, err := h.metaManager.GetTable(req.DBName, req.TableName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Group rows by shard based on shard key
	shardToRows := make(map[int][]map[string]interface{})

	for _, row := range req.Rows {
		shardKeyVal, ok := row[table.ShardKey]
		if !ok {
			http.Error(w, fmt.Sprintf("missing shard key: %s", table.ShardKey), http.StatusBadRequest)
			return
		}

		// Calculate shard ID using consistent hashing
		hash := h.consistentHash.Hash(fmt.Sprintf("%v", shardKeyVal))
		shardID := hash % table.ShardCount
		shardToRows[shardID] = append(shardToRows[shardID], row)
	}

	// For each shard, get primary and replica workers
	type WorkerAssignment struct {
		PrimaryURL string
		ReplicaURL string
	}
	shardToWorkers := make(map[int]WorkerAssignment)

	for shardID := range shardToRows {
		primaryWorker, err := h.metaManager.GetPrimaryWorkerForShard(req.DBName, req.TableName, shardID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		replicaWorkers, err := h.metaManager.GetReplicaWorkersForShard(req.DBName, req.TableName, shardID)
		if err != nil || len(replicaWorkers) == 0 {
			// If no replica, just use primary only
			shardToWorkers[shardID] = WorkerAssignment{PrimaryURL: primaryWorker, ReplicaURL: ""}
		} else {
			shardToWorkers[shardID] = WorkerAssignment{PrimaryURL: primaryWorker, ReplicaURL: replicaWorkers[0]}
		}
	}

	// Send to workers in parallel (primary + replica)
	var wg sync.WaitGroup
	for shardID, rows := range shardToRows {
		workers := shardToWorkers[shardID]

		// Send to primary
		wg.Add(1)
		go func(workerURL string, rowsData []map[string]interface{}, shard int) {
			defer wg.Done()
			insertReq := map[string]interface{}{
				"db_name":    req.DBName,
				"table_name": req.TableName,
				"rows":       rowsData,
				"shard_id":   shard,
			}
			data, _ := json.Marshal(insertReq)

			// Retry logic for primary
			maxRetries := 3
			for i := 0; i < maxRetries; i++ {
				resp, err := http.Post(workerURL+"/insert", "application/json", bytes.NewReader(data))
				if err == nil && resp.StatusCode == 200 {
					resp.Body.Close()
					break
				}
				if err != nil {
					log.Printf("Retry %d: Failed to send to primary %s: %v", i+1, workerURL, err)
					time.Sleep(time.Second)
				}
			}
		}(workers.PrimaryURL, rows, shardID)

		// Send to replica if exists
		if workers.ReplicaURL != "" {
			wg.Add(1)
			go func(workerURL string, rowsData []map[string]interface{}, shard int) {
				defer wg.Done()
				insertReq := map[string]interface{}{
					"db_name":    req.DBName,
					"table_name": req.TableName,
					"rows":       rowsData,
					"shard_id":   shard,
				}
				data, _ := json.Marshal(insertReq)
				http.Post(workerURL+"/insert", "application/json", bytes.NewReader(data))
			}(workers.ReplicaURL, rows, shardID)
		}
	}
	wg.Wait()

	json.NewEncoder(w).Encode(map[string]string{"status": "insert accepted"})
}

// Select - distributed query via reducer
func (h *Handler) Select(w http.ResponseWriter, r *http.Request) {
	var req SelectReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	table, err := h.metaManager.GetTable(req.DBName, req.TableName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	jobID := fmt.Sprintf("select-%d", time.Now().UnixNano())

	// Initialize reducer
	initReq := map[string]interface{}{
		"job_id": jobID,
		"type":   "sql_aggregation",
	}
	initData, _ := json.Marshal(initReq)
	http.Post(h.coord.GetReducerURL()+"/reduce/init", "application/json", bytes.NewReader(initData))

	// Get all shards and find alive workers
	workersToQuery := make(map[string]bool)

	for shardID := 0; shardID < table.ShardCount; shardID++ {
		// Try primary first
		primaryURL, err := h.metaManager.GetPrimaryWorkerForShard(req.DBName, req.TableName, shardID)
		if err == nil && primaryURL != "" {
			// Extract worker ID from URL
			workerID := extractWorkerID(primaryURL)
			if h.healthMon.IsWorkerAliveByID(workerID) {
				workersToQuery[primaryURL] = true
				continue
			}
		}

		// If primary dead, try replica
		replicas, _ := h.metaManager.GetReplicaWorkersForShard(req.DBName, req.TableName, shardID)
		for _, replicaURL := range replicas {
			workerID := extractWorkerID(replicaURL)
			if h.healthMon.IsWorkerAliveByID(workerID) {
				workersToQuery[replicaURL] = true
				break
			}
		}
	}

	// Query all selected workers
	var wg sync.WaitGroup
	for workerURL := range workersToQuery {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			selectReq := map[string]interface{}{
				"db_name":     req.DBName,
				"table_name":  req.TableName,
				"query":       req.Query,
				"job_id":      jobID,
				"reducer_url": h.coord.GetReducerURL(),
			}
			data, _ := json.Marshal(selectReq)
			http.Post(url+"/execute_select", "application/json", bytes.NewReader(data))
		}(workerURL)
	}
	wg.Wait()

	// Wait for reducer
	time.Sleep(2 * time.Second)

	// Get result from reducer
	resp, err := http.Get(h.coord.GetReducerURL() + "/reduce/result/" + jobID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	var result interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func extractWorkerID(url string) string {
	// Extract worker ID from URL (e.g., "http://localhost:8081" -> "worker1")
	if strings.Contains(url, "8081") {
		return "worker1"
	}
	if strings.Contains(url, "8082") {
		return "worker2"
	}
	return ""
}

// UploadFile - for MapReduce
func (h *Handler) UploadFile(w http.ResponseWriter, r *http.Request) {
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "file missing", http.StatusBadRequest)
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, "read error", http.StatusInternalServerError)
		return
	}

	// Split into chunks
	chunks, err := h.chunkSplitter.Split(data, header.Filename)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	jobID := fmt.Sprintf("mapreduce-%d", time.Now().UnixNano())
	workers := h.healthMon.GetAliveWorkers()

	if len(workers) == 0 {
		http.Error(w, "no workers available", http.StatusServiceUnavailable)
		return
	}

	// Distribute chunks to workers
	var wg sync.WaitGroup
	for i, chunk := range chunks {
		wg.Add(1)
		go func(idx int, chunkData sharding.DataChunk) {
			defer wg.Done()
			worker := workers[idx%len(workers)]
			storeReq := map[string]interface{}{
				"chunk_id": chunkData.ID,
				"data":     string(chunkData.Data),
			}
			data, _ := json.Marshal(storeReq)
			http.Post(worker.URL+"/store_chunk", "application/json", bytes.NewReader(data))
		}(i, chunk)
	}
	wg.Wait()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"job_id":       jobID,
		"total_chunks": len(chunks),
		"workers":      len(workers),
	})
}

// HealthCheck returns master status
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "active"})
}

func (h *Handler) GetWorkers(w http.ResponseWriter, r *http.Request) {
	workers := h.healthMon.GetAliveWorkers()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(workers)
}

func (h *Handler) Update(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "update endpoint"})
}

func (h *Handler) Delete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "delete endpoint"})
}

func (h *Handler) WordCountMapReduce(w http.ResponseWriter, r *http.Request) {
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "file missing", http.StatusBadRequest)
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, "read error", http.StatusInternalServerError)
		return
	}

	// Split into chunks
	chunks, err := h.chunkSplitter.Split(data, header.Filename)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	jobID := fmt.Sprintf("wordcount-%d", time.Now().UnixNano())
	workers := h.healthMon.GetAliveWorkers()
	reducerURL := h.coord.GetReducerURL()

	// Initialize reducer job
	initReq := map[string]interface{}{
		"job_id": jobID,
		"type":   "wordcount",
	}
	initData, _ := json.Marshal(initReq)
	http.Post(reducerURL+"/reduce/init", "application/json", bytes.NewReader(initData))

	// Distribute map tasks
	var wg sync.WaitGroup
	for i, chunk := range chunks {
		wg.Add(1)
		go func(idx int, chunkData sharding.DataChunk) {
			defer wg.Done()
			worker := workers[idx%len(workers)]
			mapReq := map[string]interface{}{
				"job_id":      jobID,
				"chunk_id":    chunkData.ID,
				"chunk_data":  string(chunkData.Data),
				"map_func":    "wordcount",
				"reducer_url": reducerURL,
			}
			data, _ := json.Marshal(mapReq)
			http.Post(worker.URL+"/map", "application/json", bytes.NewReader(data))
		}(i, chunk)
	}
	wg.Wait()

	// Wait for reducer to finish
	time.Sleep(3 * time.Second)

	// Get final result
	resp, err := http.Get(reducerURL + "/reduce/result/" + jobID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	var result interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (h *Handler) DropDatabase(w http.ResponseWriter, r *http.Request) {
	var req CreateDBReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if err := h.metaManager.DropDatabase(req.Name); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "dropped"})
}

func (h *Handler) sendWithRetry(workerURL, endpoint string, data []byte, maxRetries int) error {
	for i := 0; i < maxRetries; i++ {
		resp, err := http.Post(workerURL+endpoint, "application/json", bytes.NewReader(data))
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return nil
		}
		if err != nil {
			log.Printf("Retry %d: Failed to send to %s: %v", i+1, workerURL, err)
			time.Sleep(time.Second * time.Duration(i+1))
		}
	}
	return fmt.Errorf("failed after %d retries", maxRetries)
}
