package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"reducer/aggregator"
	"reducer/merge"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server struct {
		Port string `yaml:"port"`
		Name string `yaml:"name"`
	} `yaml:"server"`
	Aggregator struct {
		BufferSize    int `yaml:"buffer_size"`
		FlushInterval int `yaml:"flush_interval"`
	} `yaml:"aggregator"`
	Merge struct {
		MaxWorkers     int `yaml:"max_workers"`
		TimeoutSeconds int `yaml:"timeout_seconds"`
	} `yaml:"merge"`
}

var (
	aggregatorInstance *aggregator.Aggregator
	mergerInstance     *merge.Merger
	config             Config
)

func main() {
	// Load configuration
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Printf("Warning: Cannot read config.yaml, using defaults: %v", err)
		setDefaultConfig()
	} else {
		if err := yaml.Unmarshal(data, &config); err != nil {
			log.Printf("Error parsing config, using defaults: %v", err)
			setDefaultConfig()
		}
	}

	// Initialize aggregator and merger
	aggregatorInstance = aggregator.NewAggregator()
	mergerInstance = merge.NewMerger(config.Merge.MaxWorkers)

	// Setup HTTP routes
	http.HandleFunc("/reduce/init", handleInit)
	http.HandleFunc("/reduce/add_partial", handleAddPartial)
	http.HandleFunc("/reduce/result/", handleGetResult)
	http.HandleFunc("/reduce/merge", handleMerge)
	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/reduce/jobs", handleListJobs)

	// Start server
	server := &http.Server{
		Addr:         config.Server.Port,
		Handler:      nil,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		log.Printf("🔄 Reducer %s starting on port %s", config.Server.Name, config.Server.Port)
		log.Printf("   - Max merge workers: %d", config.Merge.MaxWorkers)
		log.Printf("   - Buffer size: %d", config.Aggregator.BufferSize)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	log.Println("Shutting down reducer...")
	server.Close()
}

func setDefaultConfig() {
	config.Server.Port = ":8090"
	config.Server.Name = "main-reducer"
	config.Aggregator.BufferSize = 100
	config.Aggregator.FlushInterval = 5
	config.Merge.MaxWorkers = 4
	config.Merge.TimeoutSeconds = 30
}

func handleInit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		JobID string `json:"job_id"`
		Type  string `json:"type"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	aggregatorInstance.InitJob(req.JobID, req.Type)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "initialized",
		"job_id": req.JobID,
	})
}

func handleAddPartial(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		JobID   string      `json:"job_id"`
		Partial interface{} `json:"partial"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if !aggregatorInstance.AddPartial(req.JobID, req.Partial) {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func handleGetResult(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Path[len("/reduce/result/"):]

	result := aggregatorInstance.GetResult(jobID)
	if result == nil {
		http.Error(w, "Job not found or no results", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func handleMerge(w http.ResponseWriter, r *http.Request) {
	var req struct {
		JobID string        `json:"job_id"`
		Data  []interface{} `json:"data"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	merged := mergerInstance.MergeResults(req.Data)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(merged)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"name":   config.Server.Name,
		"jobs":   len(aggregatorInstance.GetAllJobs()),
	})
}

func handleListJobs(w http.ResponseWriter, r *http.Request) {
	jobs := aggregatorInstance.GetAllJobs()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"jobs":  jobs,
		"count": len(jobs),
	})
}
