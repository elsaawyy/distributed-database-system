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
			// Create worker database - USING worker3_db
			_, err = db.Exec("CREATE DATABASE IF NOT EXISTS worker3_db")
			if err != nil {
				log.Printf("Error creating database: %v", err)
			}
			_, err = db.Exec("USE worker3_db")
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

	// LISTEN ON PORT 8083
	log.Println("Go Worker starting on port 8083")
	log.Fatal(http.ListenAndServe(":8083", nil))
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

	log.Printf("Executing select on worker3 for job %s: %s", req.JobID, req.Query)

	if db != nil {
		_, err := db.Exec("USE worker3_db")
		if err != nil {
			log.Printf("Database error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	var result interface{}

	// Check if it's a COUNT query
	upperQuery := strings.ToUpper(req.Query)
	if strings.Contains(upperQuery, "SELECT COUNT(") {
		// Handle COUNT query
		var count int
		var err error
		if db != nil {
			row := db.QueryRow(req.Query)
			err = row.Scan(&count)
			if err != nil {
				log.Printf("Count query error: %v", err)
				count = 0
			}
		} else {
			count = 42
		}
		result = map[string]interface{}{
			"worker_id": "worker3",
			"tech":      "go",
			"count":     count,
			"job_id":    req.JobID,
		}
	} else {
		// Handle SELECT * query - return all rows
		rows, err := db.Query(req.Query)
		if err != nil {
			log.Printf("Select query error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			log.Printf("Columns error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var results []map[string]interface{}

		for rows.Next() {
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				log.Printf("Scan error: %v", err)
				continue
			}

			row := make(map[string]interface{})
			for i, col := range columns {
				// Convert []byte to string for better display
				if val, ok := values[i].([]byte); ok {
					row[col] = string(val)
				} else {
					row[col] = values[i]
				}
			}
			results = append(results, row)
		}

		result = map[string]interface{}{
			"worker_id": "worker3",
			"tech":      "go",
			"rows":      results,
			"count":     len(results),
			"job_id":    req.JobID,
		}
	}

	// Send to reducer
	if req.ReducerURL != "" {
		go sendToReducer(req.ReducerURL, req.JobID, result)
		log.Printf("Sent result to reducer for job %s", req.JobID)
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

	log.Printf("Executing aggregate on worker3: %s", req.Query)

	var result float64
	var err error

	if db != nil {
		_, err = db.Exec("USE worker3_db")
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

	// ALWAYS use worker3_db regardless of what DBName is sent
	actualDB := "worker3_db"

	log.Printf("Inserting %d rows into %s.%s (shard %d)", len(req.Rows), actualDB, req.TableName, req.ShardID)

	if db == nil {
		http.Error(w, "database not connected", http.StatusInternalServerError)
		return
	}

	// Use worker3_db
	_, err := db.Exec(fmt.Sprintf("USE %s", actualDB))
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
		_, err := db.Exec("USE worker3_db")
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
		_, err := db.Exec("USE worker3_db")
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

	// ALWAYS use worker3_db
	actualDB := "worker3_db"

	log.Printf("Creating table %s.%s on worker3", actualDB, req.TableName)

	if db != nil {
		_, err := db.Exec(fmt.Sprintf("USE %s", actualDB))
		if err != nil {
			log.Printf("Error using database: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", req.TableName, req.Schema)
		_, err = db.Exec(query)
		if err != nil {
			log.Printf("Error creating table: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		log.Printf("Table %s created successfully in %s", req.TableName, actualDB)
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

	log.Printf("Creating database %s on worker3", req.Name)

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

	log.Printf("Storing chunk %s on worker3", req.ChunkID)

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

	log.Printf("Executing map function '%s' on chunk %s (worker3)", req.MapFunc, req.ChunkID)

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

	log.Printf("Sent partial result to reducer for job %s, response: %d", jobID, resp.StatusCode)
}
