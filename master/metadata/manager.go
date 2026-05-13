package metadata

import (
	"database/sql"
	"errors"
)

type Manager struct {
	db *sql.DB
}

func NewManager(db *sql.DB) *Manager {
	return &Manager{db: db}
}

func (m *Manager) CreateDatabase(name string) error {
	// Use backticks to escape reserved keyword 'databases'
	_, err := m.db.Exec("INSERT INTO `databases` (name) VALUES (?)", name)
	return err
}

func (m *Manager) DropDatabase(name string) error {
	// Use backticks to escape reserved keyword
	_, err := m.db.Exec("DELETE FROM `databases` WHERE name = ?", name)
	return err
}

func (m *Manager) CreateTable(table *Table) error {
	// Insert table record
	_, err := m.db.Exec(`INSERT INTO tables (db_name, table_name, shard_key, shard_count, replication_factor)
		VALUES (?, ?, ?, ?, ?)`, table.DBName, table.TableName, table.ShardKey, table.ShardCount, table.ReplicaFact)
	return err
}

func (m *Manager) GetTable(dbName, tableName string) (*Table, error) {
	var t Table
	row := m.db.QueryRow(`SELECT db_name, table_name, shard_key, shard_count, replication_factor
		FROM tables WHERE db_name = ? AND table_name = ?`, dbName, tableName)
	err := row.Scan(&t.DBName, &t.TableName, &t.ShardKey, &t.ShardCount, &t.ReplicaFact)
	if err == sql.ErrNoRows {
		return nil, errors.New("table not found")
	}
	return &t, err
}

func (m *Manager) RegisterShard(dbName, tableName string, shardID int, workerID string, isPrimary bool) error {
	_, err := m.db.Exec(`INSERT INTO shards (db_name, table_name, shard_id, worker_id, is_primary)
		VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE worker_id=VALUES(worker_id), is_primary=VALUES(is_primary)`,
		dbName, tableName, shardID, workerID, isPrimary)
	return err
}

func (m *Manager) GetPrimaryWorkerForShard(dbName, tableName string, shardID int) (string, error) {
	var workerURL string
	row := m.db.QueryRow(`SELECT w.url FROM shards s JOIN workers w ON s.worker_id = w.worker_id
		WHERE s.db_name = ? AND s.table_name = ? AND s.shard_id = ? AND s.is_primary = TRUE`,
		dbName, tableName, shardID)
	err := row.Scan(&workerURL)
	return workerURL, err
}

func (m *Manager) GetReplicaWorkersForShard(dbName, tableName string, shardID int) ([]string, error) {
	rows, err := m.db.Query(`
        SELECT w.url FROM shards s 
        JOIN workers w ON s.worker_id = w.worker_id 
        WHERE s.db_name = ? AND s.table_name = ? AND s.shard_id = ? AND s.is_primary = FALSE
    `, dbName, tableName, shardID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var urls []string
	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err != nil {
			continue
		}
		urls = append(urls, url)
	}
	return urls, nil
}

func (m *Manager) ListDatabases() ([]string, error) {
	rows, err := m.db.Query("SELECT name FROM `databases` ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		databases = append(databases, name)
	}
	return databases, nil
}

func (m *Manager) ListTables(dbName string) ([]string, error) {
	rows, err := m.db.Query("SELECT table_name FROM tables WHERE db_name = ? ORDER BY table_name", dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, nil
}

func (m *Manager) GetTableSchema(dbName, tableName string) (string, error) {
	var schema string
	err := m.db.QueryRow("SELECT schema_sql FROM tables WHERE db_name = ? AND table_name = ?", dbName, tableName).Scan(&schema)
	if err != nil {
		return "", err
	}
	return schema, nil
}

func (m *Manager) DropTable(dbName, tableName string) error {
	// Delete from metadata
	_, err := m.db.Exec("DELETE FROM tables WHERE db_name = ? AND table_name = ?", dbName, tableName)
	if err != nil {
		return err
	}

	// Delete shards
	_, err = m.db.Exec("DELETE FROM shards WHERE db_name = ? AND table_name = ?", dbName, tableName)
	return err
}
