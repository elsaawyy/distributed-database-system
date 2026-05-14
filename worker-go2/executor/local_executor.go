package executor

import (
	"fmt"
	"strings"

	"worker-go2/storage"
)

type LocalExecutor struct {
	storage *storage.MySQLStorage
}

func NewLocalExecutor(storage *storage.MySQLStorage) *LocalExecutor {
	return &LocalExecutor{storage: storage}
}

type QueryResult struct {
	Columns []string                 `json:"columns"`
	Rows    []map[string]interface{} `json:"rows"`
	Count   int                      `json:"count"`
}

func (e *LocalExecutor) ExecuteSelect(query string) (*QueryResult, error) {
	results, err := e.storage.ExecuteQuery(query)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return &QueryResult{Count: 0, Rows: []map[string]interface{}{}}, nil
	}

	// Extract columns
	columns := make([]string, 0)
	for col := range results[0] {
		columns = append(columns, col)
	}

	return &QueryResult{
		Columns: columns,
		Rows:    results,
		Count:   len(results),
	}, nil
}

func (e *LocalExecutor) ExecuteAggregate(query string) (map[string]interface{}, error) {
	results, err := e.storage.ExecuteQuery(query)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return map[string]interface{}{}, nil
	}

	return results[0], nil
}

func (e *LocalExecutor) ExecuteInsert(tableName string, rows []map[string]interface{}) error {
	return e.storage.InsertRows(tableName, rows)
}

func (e *LocalExecutor) ExecuteUpdate(query string) (int64, error) {
	return e.storage.ExecuteUpdate(query)
}

func (e *LocalExecutor) ExecuteDelete(query string) (int64, error) {
	return e.storage.ExecuteDelete(query)
}

func (e *LocalExecutor) MapWordCount(chunkData []byte) (map[string]int, error) {
	text := string(chunkData)
	words := strings.Fields(text)
	wordCount := make(map[string]int)

	for _, word := range words {
		word = strings.ToLower(strings.Trim(word, ".,!?;:\"'()[]{}"))
		if word != "" {
			wordCount[word]++
		}
	}

	return wordCount, nil
}

func (e *LocalExecutor) CreateTable(dbName, tableName, schema string) error {
	// Switch to database and create table
	_, err := e.storage.ExecuteUpdate(fmt.Sprintf("USE %s", dbName))
	if err != nil {
		return err
	}
	return e.storage.CreateTable(tableName, schema)
}
