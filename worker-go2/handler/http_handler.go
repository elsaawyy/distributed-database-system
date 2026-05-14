package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"worker-go2/executor"
	"worker-go2/storage"
)

type Handler struct {
	storage    *storage.MySQLStorage
	executor   *executor.LocalExecutor
	workerID   string
	technology string
	reducerURL string
}

func NewHandler(store *storage.MySQLStorage, exec *executor.LocalExecutor, workerID, technology, reducerURL string) *Handler {
	return &Handler{
		storage:    store,
		executor:   exec,
		workerID:   workerID,
		technology: technology,
		reducerURL: reducerURL,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/health", h.HealthCheck)
	mux.HandleFunc("/execute_select", h.ExecuteSelect)
	mux.HandleFunc("/execute_aggregate", h.ExecuteAggregate)
	mux.HandleFunc("/insert", h.Insert)
	mux.HandleFunc("/update", h.Update)
	mux.HandleFunc("/delete", h.Delete)
	mux.HandleFunc("/store_chunk", h.StoreChunk)
	mux.HandleFunc("/map", h.MapHandler)
	mux.HandleFunc("/create_table", h.CreateTable)
}

func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "ok",
		"worker_id":  h.workerID,
		"technology": h.technology,
	})
}

func (h *Handler) ExecuteSelect(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DBName     string `json:"db_name"`
		TableName  string `json:"table_name"`
		Query      string `json:"query"`
		JobID      string `json:"job_id"`
		ReducerURL string `json:"reducer_url"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Execute the query locally
	result, err := h.executor.ExecuteSelect(req.Query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Prepare partial result
	partial := map[string]interface{}{
		"worker_id":  h.workerID,
		"technology": h.technology,
		"count":      result.Count,
		"rows":       result.Rows,
		"job_id":     req.JobID,
	}

	// Send to reducer
	reducerURL := req.ReducerURL
	if reducerURL == "" {
		reducerURL = h.reducerURL
	}

	go h.sendToReducer(reducerURL, req.JobID, partial)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "processing", "job_id": req.JobID})
}

func (h *Handler) ExecuteAggregate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query string `json:"query"`
		JobID string `json:"job_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.executor.ExecuteAggregate(req.Query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (h *Handler) Insert(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DBName    string                   `json:"db_name"`
		TableName string                   `json:"table_name"`
		Rows      []map[string]interface{} `json:"rows"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Switch to the correct database
	_, err := h.storage.ExecuteUpdate(fmt.Sprintf("USE %s", req.DBName))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = h.executor.ExecuteInsert(req.TableName, req.Rows)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "inserted",
		"count":  len(req.Rows),
		"worker": h.workerID,
	})
}

func (h *Handler) Update(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DBName string `json:"db_name"`
		Query  string `json:"query"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	affected, err := h.executor.ExecuteUpdate(req.Query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "updated",
		"affected": affected,
	})
}

func (h *Handler) Delete(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DBName string `json:"db_name"`
		Query  string `json:"query"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	affected, err := h.executor.ExecuteDelete(req.Query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "deleted",
		"affected": affected,
	})
}

func (h *Handler) StoreChunk(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ChunkID string `json:"chunk_id"`
		Data    []byte `json:"data"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := h.storage.StoreChunk(req.ChunkID, req.Data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"status": "stored", "chunk_id": req.ChunkID})
}

func (h *Handler) MapHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		JobID      string `json:"job_id"`
		ChunkID    string `json:"chunk_id"`
		ChunkData  []byte `json:"chunk_data"`
		MapFunc    string `json:"map_func"`
		ReducerURL string `json:"reducer_url"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var result interface{}
	var err error

	switch req.MapFunc {
	case "wordcount":
		result, err = h.executor.MapWordCount(req.ChunkData)
	default:
		err = fmt.Errorf("unknown map function: %s", req.MapFunc)
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Send map result to reducer
	reducerURL := req.ReducerURL
	if reducerURL == "" {
		reducerURL = h.reducerURL
	}

	go h.sendToReducer(reducerURL, req.JobID, result)

	json.NewEncoder(w).Encode(map[string]string{"status": "mapped", "job_id": req.JobID})
}

func (h *Handler) CreateTable(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DBName    string `json:"db_name"`
		TableName string `json:"table_name"`
		Schema    string `json:"schema"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := h.executor.CreateTable(req.DBName, req.TableName, req.Schema)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"status": "created"})
}

func (h *Handler) sendToReducer(reducerURL, jobID string, partial interface{}) {
	payload := map[string]interface{}{
		"job_id":  jobID,
		"partial": partial,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error marshaling partial result: %v", err)
		return
	}

	resp, err := http.Post(reducerURL+"/reduce/add_partial", "application/json", bytes.NewReader(data))
	if err != nil {
		log.Printf("Error sending to reducer: %v", err)
		return
	}
	defer resp.Body.Close()
}
