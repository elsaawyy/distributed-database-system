package ui

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"mime/multipart"
	"net/http"
)

type UIHandler struct {
	masterURL string
	apiKey    string
}

func NewUIHandler(masterURL, apiKey string) *UIHandler {
	return &UIHandler{
		masterURL: masterURL,
		apiKey:    apiKey,
	}
}

func (h *UIHandler) ServeIndex(w http.ResponseWriter, r *http.Request) {
	tmpl := template.Must(template.ParseFiles("ui/templates/index.html"))
	tmpl.Execute(w, nil)
}

func (h *UIHandler) GetWorkers(w http.ResponseWriter, r *http.Request) {
	req, _ := http.NewRequest("GET", h.masterURL+"/v1/workers", nil)
	req.Header.Set("X-API-Key", h.apiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	io.Copy(w, resp.Body)
}

func (h *UIHandler) GetStats(w http.ResponseWriter, r *http.Request) {
	// Get databases count
	req, _ := http.NewRequest("GET", h.masterURL+"/v1/databases", nil)
	req.Header.Set("X-API-Key", h.apiKey)

	client := &http.Client{}
	resp, _ := client.Do(req)

	stats := map[string]interface{}{
		"databases":     0,
		"tables":        0,
		"workers_alive": 0,
	}

	if resp != nil {
		defer resp.Body.Close()
		var databases []interface{}
		json.NewDecoder(resp.Body).Decode(&databases)
		stats["databases"] = len(databases)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (h *UIHandler) CreateDatabase(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("name")

	body := map[string]string{"name": name}
	jsonBody, _ := json.Marshal(body)

	req, _ := http.NewRequest("POST", h.masterURL+"/v1/db/create", bytes.NewReader(jsonBody))
	req.Header.Set("X-API-Key", h.apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		w.Write([]byte(fmt.Sprintf("Error: %v", err)))
		return
	}
	defer resp.Body.Close()

	io.Copy(w, resp.Body)
}

func (h *UIHandler) CreateTable(w http.ResponseWriter, r *http.Request) {
	dbName := r.FormValue("db_name")
	tableName := r.FormValue("table_name")
	schema := r.FormValue("schema")
	shardKey := r.FormValue("shard_key")

	body := map[string]interface{}{
		"db_name":    dbName,
		"table_name": tableName,
		"schema":     schema,
		"shard_key":  shardKey,
	}
	jsonBody, _ := json.Marshal(body)

	req, _ := http.NewRequest("POST", h.masterURL+"/v1/table/create", bytes.NewReader(jsonBody))
	req.Header.Set("X-API-Key", h.apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		w.Write([]byte(fmt.Sprintf("Error: %v", err)))
		return
	}
	defer resp.Body.Close()

	io.Copy(w, resp.Body)
}

func (h *UIHandler) InsertData(w http.ResponseWriter, r *http.Request) {
	dbName := r.FormValue("db_name")
	tableName := r.FormValue("table_name")
	rowsStr := r.FormValue("rows")

	var rows []map[string]interface{}
	if err := json.Unmarshal([]byte(rowsStr), &rows); err != nil {
		w.Write([]byte(fmt.Sprintf("Invalid JSON: %v", err)))
		return
	}

	body := map[string]interface{}{
		"db_name":    dbName,
		"table_name": tableName,
		"rows":       rows,
	}
	jsonBody, _ := json.Marshal(body)

	req, _ := http.NewRequest("POST", h.masterURL+"/v1/insert", bytes.NewReader(jsonBody))
	req.Header.Set("X-API-Key", h.apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		w.Write([]byte(fmt.Sprintf("Error: %v", err)))
		return
	}
	defer resp.Body.Close()

	io.Copy(w, resp.Body)
}

func (h *UIHandler) ExecuteQuery(w http.ResponseWriter, r *http.Request) {
	dbName := r.FormValue("db_name")
	tableName := r.FormValue("table_name")
	query := r.FormValue("query")

	body := map[string]interface{}{
		"db_name":    dbName,
		"table_name": tableName,
		"query":      query,
	}
	jsonBody, _ := json.Marshal(body)

	req, _ := http.NewRequest("POST", h.masterURL+"/v1/select", bytes.NewReader(jsonBody))
	req.Header.Set("X-API-Key", h.apiKey)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		w.Write([]byte(fmt.Sprintf("Error: %v", err)))
		return
	}
	defer resp.Body.Close()

	var result interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	pretty, _ := json.MarshalIndent(result, "", "  ")
	w.Write(pretty)
}

func (h *UIHandler) RunMapReduce(w http.ResponseWriter, r *http.Request) {
	file, header, err := r.FormFile("file")
	if err != nil {
		w.Write([]byte(fmt.Sprintf("Error reading file: %v", err)))
		return
	}
	defer file.Close()

	// Create multipart request
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, _ := writer.CreateFormFile("file", header.Filename)
	io.Copy(part, file)
	writer.Close()

	req, _ := http.NewRequest("POST", h.masterURL+"/v1/mapreduce/wordcount", body)
	req.Header.Set("X-API-Key", h.apiKey)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		w.Write([]byte(fmt.Sprintf("Error: %v", err)))
		return
	}
	defer resp.Body.Close()

	var result interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	pretty, _ := json.MarshalIndent(result, "", "  ")
	w.Write(pretty)
}
