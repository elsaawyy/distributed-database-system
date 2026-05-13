package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB

func main() {
	// Connect to MySQL
	var err error
	dsn := "root:@tcp(localhost:3309)/?parseTime=true"
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("Warning: Could not connect to MySQL: %v", err)
		log.Println("Worker running in demo mode without database")
	} else {
		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
		if err := db.Ping(); err != nil {
			log.Printf("Cannot ping MySQL: %v", err)
		} else {
			log.Println("Connected to MySQL successfully")
			// Create worker database
			_, err = db.Exec("CREATE DATABASE IF NOT EXISTS worker1_db")
			if err != nil {
				log.Printf("Error creating database: %v", err)
			}
			_, err = db.Exec("USE worker1_db")
			if err != nil {
				log.Printf("Error using database: %v", err)
			}
		}
	}

	// Register routes
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/execute_select", executeSelectHandler)
	http.HandleFunc("/execute_aggregate", executeAggregateHandler)
	http.HandleFunc("/insert", insertHandler)
	http.HandleFunc("/update", updateHandler)
	http.HandleFunc("/delete", deleteHandler)
	http.HandleFunc("/create_table", createTableHandler)
	http.HandleFunc("/create_database", createDatabaseHandler)
	http.HandleFunc("/store_chunk", storeChunkHandler)
	http.HandleFunc("/map", mapHandler)

	log.Println("Go Worker starting on port 8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "alive",
		"worker": "go",
		"time":   time.Now().Unix(),
	})
}

func executeSelectHandler(w http.ResponseWriter, r *http.Request) {
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

	log.Printf("Executing select on worker1 for job %s", req.JobID)

	var count int
	var result interface{}

	if db != nil {
		// Switch to correct database
		_, err := db.Exec(fmt.Sprintf("USE %s", req.DBName))
		if err != nil {
			log.Printf("Error switching to database %s: %v", req.DBName, err)
			result = map[string]interface{}{
				"worker_id": "worker1",
				"tech":      "go",
				"count":     0,
				"job_id":    req.JobID,
				"error":     err.Error(),
			}
		} else {
			// Execute the query
			row := db.QueryRow(req.Query)
			err = row.Scan(&count)
			if err != nil {
				log.Printf("Query error: %v", err)
				result = map[string]interface{}{
					"worker_id": "worker1",
					"tech":      "go",
					"count":     0,
					"job_id":    req.JobID,
					"error":     err.Error(),
				}
			} else {
				result = map[string]interface{}{
					"worker_id": "worker1",
					"tech":      "go",
					"count":     count,
					"job_id":    req.JobID,
				}
			}
		}
	} else {
		// Demo mode - return mock data
		result = map[string]interface{}{
			"worker_id": "worker1",
			"tech":      "go",
			"count":     42,
			"job_id":    req.JobID,
		}
	}

	// Send to reducer if URL provided
	if req.ReducerURL != "" {
		go sendToReducer(req.ReducerURL, req.JobID, result)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "success",
		"result": result,
	})
}

func executeAggregateHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DBName    string `json:"db_name"`
		TableName string `json:"table_name"`
		Query     string `json:"query"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Executing aggregate on worker1: %s", req.Query)

	var result float64
	var err error

	if db != nil {
		_, err = db.Exec(fmt.Sprintf("USE %s", req.DBName))
		if err == nil {
			row := db.QueryRow(req.Query)
			err = row.Scan(&result)
		}
	}

	if err != nil {
		result = 0
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"result": result,
		"worker": "go",
	})
}

func insertHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DBName    string                   `json:"db_name"`
		TableName string                   `json:"table_name"`
		Rows      []map[string]interface{} `json:"rows"`
		ShardID   int                      `json:"shard_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Inserting %d rows into %s.%s (shard %d)", len(req.Rows), req.DBName, req.TableName, req.ShardID)

	if db == nil {
		http.Error(w, "database not connected", http.StatusInternalServerError)
		return
	}

	// Create database if not exists
	_, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", req.DBName))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Use the database
	_, err = db.Exec(fmt.Sprintf("USE %s", req.DBName))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Insert each row
	for _, row := range req.Rows {
		columns := make([]string, 0, len(row))
		values := make([]interface{}, 0, len(row))
		placeholders := make([]string, 0, len(row))

		for col, val := range row {
			columns = append(columns, col)
			values = append(values, val)
			placeholders = append(placeholders, "?")
		}

		colStr := strings.Join(columns, ",")
		placeholderStr := strings.Join(placeholders, ",")

		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", req.TableName, colStr, placeholderStr)
		_, err := db.Exec(query, values...)
		if err != nil {
			log.Printf("Insert error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "inserted",
		"count":  len(req.Rows),
		"worker": "go",
	})
}

func updateHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DBName string `json:"db_name"`
		Query  string `json:"query"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Executing update: %s", req.Query)

	var affected int64
	if db != nil {
		_, err := db.Exec(fmt.Sprintf("USE %s", req.DBName))
		if err == nil {
			result, err := db.Exec(req.Query)
			if err == nil {
				affected, _ = result.RowsAffected()
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "updated",
		"affected": affected,
		"worker":   "go",
	})
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DBName string `json:"db_name"`
		Query  string `json:"query"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Executing delete: %s", req.Query)

	var affected int64
	if db != nil {
		_, err := db.Exec(fmt.Sprintf("USE %s", req.DBName))
		if err == nil {
			result, err := db.Exec(req.Query)
			if err == nil {
				affected, _ = result.RowsAffected()
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "deleted",
		"affected": affected,
		"worker":   "go",
	})
}

func createTableHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DBName    string `json:"db_name"`
		TableName string `json:"table_name"`
		Schema    string `json:"schema"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Creating table %s.%s on worker1", req.DBName, req.TableName)

	if db != nil {
		// Create database if not exists
		_, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", req.DBName))
		if err != nil {
			log.Printf("Error creating database: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Use the database
		_, err = db.Exec(fmt.Sprintf("USE %s", req.DBName))
		if err != nil {
			log.Printf("Error using database: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Create table
		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", req.TableName, req.Schema)
		_, err = db.Exec(query)
		if err != nil {
			log.Printf("Error creating table: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		log.Printf("Table %s created successfully in %s", req.TableName, req.DBName)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "created"})
}

func createDatabaseHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Creating database %s on worker1", req.Name)

	if db != nil {
		_, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", req.Name))
		if err != nil {
			log.Printf("Error creating database: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "created"})
}

func storeChunkHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ChunkID string `json:"chunk_id"`
		Data    []byte `json:"data"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Storing chunk %s on worker1", req.ChunkID)

	if db != nil {
		// Create chunks table
		_, err := db.Exec(`
            CREATE TABLE IF NOT EXISTS chunks (
                chunk_id VARCHAR(255) PRIMARY KEY,
                data LONGTEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `)
		if err != nil {
			log.Printf("Error creating chunks table: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		_, err = db.Exec("INSERT INTO chunks (chunk_id, data) VALUES (?, ?) ON DUPLICATE KEY UPDATE data = ?",
			req.ChunkID, string(req.Data), string(req.Data))
		if err != nil {
			log.Printf("Error storing chunk: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "stored"})
}

func mapHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		JobID      string `json:"job_id"`
		ChunkID    string `json:"chunk_id"`
		ChunkData  string `json:"chunk_data"`
		MapFunc    string `json:"map_func"`
		ReducerURL string `json:"reducer_url"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Executing map function '%s' on chunk %s", req.MapFunc, req.ChunkID)

	var result interface{}

	if req.MapFunc == "wordcount" {
		// Simple word count
		wordCount := make(map[string]int)
		word := ""
		for _, ch := range req.ChunkData {
			if ch == ' ' || ch == '\n' || ch == '\t' {
				if word != "" {
					wordCount[word]++
					word = ""
				}
			} else {
				word += string(ch)
			}
		}
		if word != "" {
			wordCount[word]++
		}
		result = wordCount
	} else {
		result = map[string]string{"error": "unknown map function"}
	}

	// Send to reducer
	if req.ReducerURL != "" {
		go sendToReducer(req.ReducerURL, req.JobID, result)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "completed",
		"job_id": req.JobID,
		"result": result,
	})
}

func sendToReducer(reducerURL, jobID string, result interface{}) {
	payload := map[string]interface{}{
		"job_id":  jobID,
		"partial": result,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error marshaling: %v", err)
		return
	}

	resp, err := http.Post(reducerURL+"/reduce/add_partial", "application/json", bytes.NewReader(data))
	if err != nil {
		log.Printf("Error sending to reducer: %v", err)
		return
	}
	defer resp.Body.Close()

	// Add this - check response status
	if resp.StatusCode != http.StatusOK {
		log.Printf("Reducer returned non-200 status: %d", resp.StatusCode)
	}
}
