package storage

import (
	"database/sql"
	"fmt"
	"log"
)

type MySQLStorage struct {
	db *sql.DB
}

func NewMySQLStorage(db *sql.DB) *MySQLStorage {
	return &MySQLStorage{db: db}
}

func InitDatabase(db *sql.DB, dbName string) {
	// Create database if not exists
	_, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	if err != nil {
		log.Printf("Error creating database: %v", err)
		return
	}

	// Use the database
	_, err = db.Exec(fmt.Sprintf("USE %s", dbName))
	if err != nil {
		log.Printf("Error using database: %v", err)
	}
}

func (s *MySQLStorage) CreateTable(tableName, schema string) error {
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, schema)
	_, err := s.db.Exec(query)
	return err
}

func (s *MySQLStorage) InsertRows(tableName string, rows []map[string]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	// Build insert query dynamically
	columns := make([]string, 0, len(rows[0]))
	for col := range rows[0] {
		columns = append(columns, col)
	}

	placeholders := ""
	for i := range rows {
		if i > 0 {
			placeholders += ","
		}
		placeholders += "("
		for j := range columns {
			if j > 0 {
				placeholders += ","
			}
			placeholders += "?"
		}
		placeholders += ")"
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		tableName,
		joinColumns(columns),
		placeholders)

	// Flatten values
	values := make([]interface{}, 0)
	for _, row := range rows {
		for _, col := range columns {
			values = append(values, row[col])
		}
	}

	_, err := s.db.Exec(query, values...)
	return err
}

func (s *MySQLStorage) ExecuteQuery(query string) ([]map[string]interface{}, error) {
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	results := make([]map[string]interface{}, 0)
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}

	return results, nil
}

func (s *MySQLStorage) ExecuteUpdate(query string) (int64, error) {
	result, err := s.db.Exec(query)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *MySQLStorage) ExecuteDelete(query string) (int64, error) {
	result, err := s.db.Exec(query)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *MySQLStorage) StoreChunk(chunkID string, data []byte) error {
	// Create chunks table if not exists
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS chunks (
			chunk_id VARCHAR(255) PRIMARY KEY,
			data LONGBLOB,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return err
	}

	_, err = s.db.Exec("INSERT INTO chunks (chunk_id, data) VALUES (?, ?) ON DUPLICATE KEY UPDATE data = ?",
		chunkID, data, data)
	return err
}

func joinColumns(columns []string) string {
	result := ""
	for i, col := range columns {
		if i > 0 {
			result += ","
		}
		result += col
	}
	return result
}
