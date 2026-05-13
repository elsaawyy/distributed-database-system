package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"time"
)

type Client struct {
	MasterURL  string
	APIKey     string
	HTTPClient *http.Client
}

type CreateDBRequest struct {
	Name string `json:"name"`
}

type CreateTableRequest struct {
	DBName    string `json:"db_name"`
	TableName string `json:"table_name"`
	Schema    string `json:"schema"`
	ShardKey  string `json:"shard_key"`
}

type InsertRequest struct {
	DBName    string                   `json:"db_name"`
	TableName string                   `json:"table_name"`
	Rows      []map[string]interface{} `json:"rows"`
}

type SelectRequest struct {
	DBName    string `json:"db_name"`
	TableName string `json:"table_name"`
	Query     string `json:"query"`
}

func NewClient(masterURL, apiKey string) *Client {
	return &Client{
		MasterURL:  masterURL,
		APIKey:     apiKey,
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) doRequest(method, endpoint string, body interface{}) ([]byte, error) {
	var reqBody io.Reader
	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reqBody = bytes.NewReader(jsonData)
	}

	req, err := http.NewRequest(method, c.MasterURL+endpoint, reqBody)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-API-Key", c.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func (c *Client) Health() error {
	fmt.Println("\n🔍 Checking master health...")
	data, err := c.doRequest("GET", "/v1/health", nil)
	if err != nil {
		return err
	}

	var result map[string]interface{}
	json.Unmarshal(data, &result)
	fmt.Printf("✅ Master status: %v\n", result["status"])
	return nil
}

func (c *Client) CreateDatabase(name string) error {
	fmt.Printf("\n📊 Creating database: %s\n", name)
	req := CreateDBRequest{Name: name}
	data, err := c.doRequest("POST", "/v1/db/create", req)
	if err != nil {
		return err
	}

	var result map[string]interface{}
	json.Unmarshal(data, &result)
	fmt.Printf("✅ Database created: %v\n", result["status"])
	return nil
}

func (c *Client) CreateTable(dbName, tableName, schema, shardKey string) error {
	fmt.Printf("\n📋 Creating table: %s.%s\n", dbName, tableName)
	req := CreateTableRequest{
		DBName:    dbName,
		TableName: tableName,
		Schema:    schema,
		ShardKey:  shardKey,
	}
	data, err := c.doRequest("POST", "/v1/table/create", req)
	if err != nil {
		return err
	}

	var result map[string]interface{}
	json.Unmarshal(data, &result)
	fmt.Printf("✅ Table created: %v\n", result["status"])
	return nil
}

func (c *Client) InsertRows(dbName, tableName string, rows []map[string]interface{}) error {
	fmt.Printf("\n💾 Inserting %d rows into %s.%s\n", len(rows), dbName, tableName)
	req := InsertRequest{
		DBName:    dbName,
		TableName: tableName,
		Rows:      rows,
	}
	data, err := c.doRequest("POST", "/v1/insert", req)
	if err != nil {
		return err
	}

	var result map[string]interface{}
	json.Unmarshal(data, &result)
	fmt.Printf("✅ Insert result: %v\n", result["status"])
	return nil
}

func (c *Client) SelectQuery(dbName, tableName, query string) error {
	fmt.Printf("\n🔍 Executing distributed SELECT: %s\n", query)
	req := SelectRequest{
		DBName:    dbName,
		TableName: tableName,
		Query:     query,
	}
	data, err := c.doRequest("POST", "/v1/select", req)
	if err != nil {
		return err
	}

	var result interface{}
	json.Unmarshal(data, &result)
	fmt.Printf("✅ Query result: %v\n", result)
	return nil
}

func (c *Client) UploadFile(filePath string) error {
	fmt.Printf("\n📤 Uploading file: %s\n", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create multipart form
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", filePath)
	if err != nil {
		return err
	}

	_, err = io.Copy(part, file)
	if err != nil {
		return err
	}
	writer.Close()

	req, err := http.NewRequest("POST", c.MasterURL+"/v1/upload-file", body)
	if err != nil {
		return err
	}

	req.Header.Set("X-API-Key", c.APIKey)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(data, &result)
	fmt.Printf("✅ Upload result: %v\n", result)
	return nil
}

func (c *Client) WordCountMapReduce(filePath string) error {
	fmt.Printf("\n🗺️ Running WordCount MapReduce on: %s\n", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", filePath)
	if err != nil {
		return err
	}

	_, err = io.Copy(part, file)
	if err != nil {
		return err
	}
	writer.Close()

	req, err := http.NewRequest("POST", c.MasterURL+"/v1/mapreduce/wordcount", body)
	if err != nil {
		return err
	}

	req.Header.Set("X-API-Key", c.APIKey)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(data, &result)
	fmt.Printf("✅ MapReduce result: %v\n", result)
	return nil
}

func (c *Client) GetWorkers() error {
	fmt.Println("\n🖥️ Fetching worker status...")
	data, err := c.doRequest("GET", "/v1/workers", nil)
	if err != nil {
		return err
	}

	var workers []map[string]interface{}
	json.Unmarshal(data, &workers)
	fmt.Printf("✅ Active workers: %d\n", len(workers))
	for _, w := range workers {
		fmt.Printf("   - %v (%v)\n", w["id"], w["tech"])
	}
	return nil
}

func printMenu() {
	fmt.Println("\n" + string(repeat(50, '=')))
	fmt.Println("   DISTRIBUTED DATABASE SYSTEM - CLIENT")
	fmt.Println(string(repeat(50, '=')))
	fmt.Println("1. Check Master Health")
	fmt.Println("2. Create Database")
	fmt.Println("3. Create Table")
	fmt.Println("4. Insert Rows")
	fmt.Println("5. Run Distributed SELECT")
	fmt.Println("6. Upload File")
	fmt.Println("7. Run WordCount MapReduce")
	fmt.Println("8. Show Active Workers")
	fmt.Println("9. Run Demo Workflow")
	fmt.Println("0. Exit")
	fmt.Println(string(repeat(50, '=')))
	fmt.Print("Choose option: ")
}

func runDemoWorkflow(client *Client) {
	fmt.Println("\n🚀 RUNNING DEMO WORKFLOW")
	fmt.Println(string(repeat(40, '-')))

	// 1. Health check
	client.Health()
	time.Sleep(1 * time.Second)

	// 2. Create database
	client.CreateDatabase("demo_db")
	time.Sleep(1 * time.Second)

	// 3. Create table
	client.CreateTable("demo_db", "products",
		"id INT PRIMARY KEY, name VARCHAR(100), price DECIMAL(10,2), quantity INT",
		"id")
	time.Sleep(1 * time.Second)

	// 4. Insert sample data
	rows := []map[string]interface{}{
		{"id": 1, "name": "Laptop", "price": 999.99, "quantity": 10},
		{"id": 2, "name": "Mouse", "price": 29.99, "quantity": 50},
		{"id": 3, "name": "Keyboard", "price": 79.99, "quantity": 30},
		{"id": 4, "name": "Monitor", "price": 299.99, "quantity": 15},
		{"id": 5, "name": "USB Cable", "price": 9.99, "quantity": 100},
	}
	client.InsertRows("demo_db", "products", rows)
	time.Sleep(1 * time.Second)

	// 5. Run distributed queries
	client.SelectQuery("demo_db", "products", "SELECT COUNT(*) FROM products")
	time.Sleep(1 * time.Second)

	client.SelectQuery("demo_db", "products", "SELECT SUM(price * quantity) as total_value FROM products")
	time.Sleep(1 * time.Second)

	// 6. Show workers
	client.GetWorkers()

	fmt.Println("\n✅ Demo workflow completed successfully!")
}

func repeat(n int, c rune) []rune {
	result := make([]rune, n)
	for i := range result {
		result[i] = c
	}
	return result
}

func main() {
	fmt.Println("\n🌟 Distributed Database System Client")
	fmt.Println("Connecting to master at http://localhost:8080")

	client := NewClient("http://localhost:8080", "my-secret-key-123")

	// Test connection
	if err := client.Health(); err != nil {
		fmt.Printf("❌ Cannot connect to master: %v\n", err)
		fmt.Println("Make sure the master node is running on port 8080")
		return
	}

	for {
		printMenu()

		var choice int
		fmt.Scan(&choice)

		switch choice {
		case 1:
			client.Health()
		case 2:
			var dbName string
			fmt.Print("Enter database name: ")
			fmt.Scan(&dbName)
			client.CreateDatabase(dbName)
		case 3:
			var dbName, tableName, shardKey string
			fmt.Print("Enter database name: ")
			fmt.Scan(&dbName)
			fmt.Print("Enter table name: ")
			fmt.Scan(&tableName)
			fmt.Print("Enter shard key: ")
			fmt.Scan(&shardKey)
			schema := "id INT PRIMARY KEY, name VARCHAR(100), value TEXT"
			client.CreateTable(dbName, tableName, schema, shardKey)
		case 4:
			var dbName, tableName string
			var id int
			var name, value string
			fmt.Print("Enter database name: ")
			fmt.Scan(&dbName)
			fmt.Print("Enter table name: ")
			fmt.Scan(&tableName)
			fmt.Print("Enter id: ")
			fmt.Scan(&id)
			fmt.Print("Enter name: ")
			fmt.Scan(&name)
			fmt.Print("Enter value: ")
			fmt.Scan(&value)
			rows := []map[string]interface{}{
				{"id": id, "name": name, "value": value},
			}
			client.InsertRows(dbName, tableName, rows)
		case 5:
			var dbName, tableName, query string
			fmt.Print("Enter database name: ")
			fmt.Scan(&dbName)
			fmt.Print("Enter table name: ")
			fmt.Scan(&tableName)
			fmt.Print("Enter SQL query: ")
			fmt.Scan(&query)
			client.SelectQuery(dbName, tableName, query)
		case 6:
			var filePath string
			fmt.Print("Enter file path: ")
			fmt.Scan(&filePath)
			client.UploadFile(filePath)
		case 7:
			var filePath string
			fmt.Print("Enter file path: ")
			fmt.Scan(&filePath)
			client.WordCountMapReduce(filePath)
		case 8:
			client.GetWorkers()
		case 9:
			runDemoWorkflow(client)
		case 0:
			fmt.Println("Goodbye!")
			return
		default:
			fmt.Println("Invalid option")
		}
	}
}
