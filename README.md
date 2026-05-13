# Distributed Database & Processing System

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)](https://golang.org)
[![Python Version](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python)](https://python.org)
[![MySQL](https://img.shields.io/badge/MySQL-8.0-4479A1?logo=mysql)](https://mysql.com)
[![License](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Production_Ready-brightgreen)]()

## 📋 Overview

A **fully functional distributed database and processing system** inspired by Hadoop, Spark, and Google's MapReduce architecture. Built with **Go** as the primary orchestration language and **MySQL** as the underlying storage engine.

The system presents a **single logical database** to clients while physically partitioning and replicating data across multiple heterogeneous worker nodes. It supports distributed SQL queries, MapReduce workflows, automatic failover, and provides both a REST API and a web-based user interface.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────┐
│              CLIENT                 │
│   (REST API / Web UI / CLI)         │
└─────────────────┬───────────────────┘
                  │
                  ▼
┌─────────────────────────────────────┐
│         MASTER NODE (Go)            │
│  • API Gateway    • Coordinator     │
│  • Metadata Mgr   • Request Router  │
│  • Auth Layer     • Health Monitor  │
│  • Leader Election                  │
└─────────────────┬───────────────────┘
                  │
     ┌────────────┼────────────┐
     │            │            │
     ▼            ▼            ▼
┌──────────┐ ┌──────────┐ ┌──────────┐
│ WORKER 1 │ │ WORKER 2 │ │ WORKER 3 │
│   (Go)   │ │ (Python) │ │   (JS)   │
│ Port 8081│ │ Port 8082│ │ Port 8083│
│ MySQL w1 │ │ MySQL w2 │ │ MySQL w3 │
└────┬─────┘ └────┬─────┘ └────┬─────┘
     └────────────┼────────────┘
                  ▼
┌─────────────────────────────────────┐
│          REDUCER NODE (Go)          │
│  • Aggregates partial results       │
│  • Merges MapReduce outputs         │
│  • Returns final results            │
└─────────────────────────────────────┘
```

---

## ✨ Features

### Core Distributed Systems
- ✅ **Distributed Sharding** — Data partitioned across workers using consistent hashing
- ✅ **Parallel Query Execution** — SELECT queries run on all workers simultaneously
- ✅ **Reducer Aggregation** — Partial results combined automatically
- ✅ **MapReduce Workflow** — Word count fully implemented
- ✅ **Heterogeneous Workers** — Go and Python workers with the same API contract

### Fault Tolerance & High Availability
- ✅ **Health Monitoring** — Heartbeat system with 5-second intervals
- ✅ **Leader Election** — MySQL advisory lock-based master election
- ✅ **Backup Master** — Automatic failover when primary master dies
- ✅ **Worker Failover** — Reads automatically route to replicas
- ✅ **Replication** — Shard-level replication with configurable factor

### Data Management
- ✅ **Dynamic Schema** — Create databases and tables at runtime
- ✅ **Form-based INSERT** — No JSON required for data insertion
- ✅ **Visual Query Builder** — Build SELECT queries without writing SQL
- ✅ **Raw SQL Support** — Advanced users can write custom queries
- ✅ **Export Results** — CSV and JSON export for query results

### Security
- ✅ **API Key Authentication** — All requests require `X-API-Key` header
- ✅ **Protected Endpoints** — Workers reject unauthorized requests

### Web UI Dashboard
- ✅ **Real-time Monitoring** — Live worker status with visual indicators
- ✅ **Database Management** — Create/drop databases
- ✅ **Table Designer** — Visual column builder with data types
- ✅ **Data Operations** — Insert, query, update, delete
- ✅ **MapReduce Interface** — Drag-and-drop file upload for word count

---

## 🚀 Quick Start

### Prerequisites

| Software | Version | Purpose |
|---|---|---|
| Go | 1.21+ | Master, Reducer, Go Worker |
| Python | 3.8+ | Python Worker |
| MySQL / MariaDB | 10.4+ | Metadata and worker storage |
| Git | Latest | Version control |

### Installation

```bash
# Clone the repository
git clone https://github.com/elsaawyy/distributed-database-system.git
cd distributed-database-system

# Setup MySQL databases
mysql -u root -p < metadata-db/schema.sql

# Install Go dependencies
cd master && go mod tidy
cd ../reducer && go mod tidy
cd ../worker-go && go mod tidy

# Install Python dependencies
cd ../worker-py && pip install -r requirements.txt
```

### Configuration

Create `master/config.yaml`:

```yaml
server:
  port: ":8080"

metadata_db:
  host: "localhost"
  port: 3306
  user: "root"
  password: "your_password"
  dbname: "distributed_metadata"

workers:
  - id: "worker1"
    url: "http://localhost:8081"
    tech: "go"
  - id: "worker2"
    url: "http://localhost:8082"
    tech: "python"

reducer:
  url: "http://localhost:8090"

auth:
  api_key: "my-secret-key-123"

heartbeat_interval: 5
replication_factor: 2
shard_count: 64
chunk_size_kb: 64
```

### Running the System

Start each component in a separate terminal:

```bash
# Terminal 1: Reducer
cd reducer && go run main.go

# Terminal 2: Go Worker
cd worker-go && go run main.go

# Terminal 3: Python Worker
cd worker-py && python app.py

# Terminal 4: Master
cd master && go run main.go

# Terminal 5: Backup Master (optional)
cd master && PORT=8081 go run main.go
```

### Access

| Interface | URL |
|---|---|
| Web UI | http://localhost:8080/ui |
| REST API | http://localhost:8080 |
| API Key | `my-secret-key-123` |

---

## 📡 API Reference

### Database Operations

| Endpoint | Method | Description |
|---|---|---|
| `/v1/db/create` | POST | Create logical database |
| `/v1/db/drop` | POST | Drop logical database |
| `/v1/databases` | GET | List all databases |
| `/v1/table/create` | POST | Create table with shard key |
| `/v1/table/drop` | POST | Drop table |
| `/v1/tables/{db}` | GET | List tables in database |
| `/v1/schema/{db}/{table}` | GET | Get table schema |

### Data Operations

| Endpoint | Method | Description |
|---|---|---|
| `/v1/insert` | POST | Insert rows (auto-sharded) |
| `/v1/select` | POST | Distributed SELECT query |
| `/v1/update` | POST | Distributed UPDATE |
| `/v1/delete` | POST | Distributed DELETE |

### System Operations

| Endpoint | Method | Description |
|---|---|---|
| `/v1/health` | GET | Master health status |
| `/v1/workers` | GET | List workers with status |
| `/v1/mapreduce/wordcount` | POST | Run word count MapReduce |

### Example API Calls

```bash
# Create database
curl -X POST http://localhost:8080/v1/db/create \
  -H "X-API-Key: my-secret-key-123" \
  -H "Content-Type: application/json" \
  -d '{"name":"testdb"}'

# Create table
curl -X POST http://localhost:8080/v1/table/create \
  -H "X-API-Key: my-secret-key-123" \
  -H "Content-Type: application/json" \
  -d '{"db_name":"testdb","table_name":"users","schema":"id INT PRIMARY KEY, name VARCHAR(100), age INT","shard_key":"id"}'

# Insert data
curl -X POST http://localhost:8080/v1/insert \
  -H "X-API-Key: my-secret-key-123" \
  -H "Content-Type: application/json" \
  -d '{"db_name":"testdb","table_name":"users","rows":[{"id":1,"name":"Alice","age":30}]}'

# Distributed query
curl -X POST http://localhost:8080/v1/select \
  -H "X-API-Key: my-secret-key-123" \
  -H "Content-Type: application/json" \
  -d '{"db_name":"testdb","table_name":"users","query":"SELECT COUNT(*) FROM users"}'

# MapReduce word count
curl -X POST http://localhost:8080/v1/mapreduce/wordcount \
  -H "X-API-Key: my-secret-key-123" \
  -F "file=@sample.txt"
```

---

## 🗄️ Database Schema

### Metadata Database (`distributed_metadata`)

| Table | Description |
|---|---|
| `databases` | Logical database names |
| `tables` | Table schemas, shard keys, replication factors |
| `shards` | Shard-to-worker mappings with primary/replica status |
| `workers` | Worker registration, technology, health status |
| `mapreduce_jobs` | Job tracking and results |
| `chunks` | File chunk locations for MapReduce |

### Worker Databases

| Worker | Database | Port |
|---|---|---|
| Go Worker | `worker1_db` | 8081 |
| Python Worker | `worker2_db` | 8082 |

---

## 🧪 Testing

```bash
# Test master health
curl http://localhost:8080/v1/health

# Test worker status
curl http://localhost:8080/v1/workers -H "X-API-Key: my-secret-key-123"

# Run end-to-end demo
./client/demo_workflow.sh
```

### Expected Results

| Test | Expected Output |
|---|---|
| Master Health | `{"status":"active"}` |
| Create Database | `{"status":"created"}` |
| Create Table | `{"status":"table created"}` |
| Insert Data | `{"status":"insert accepted"}` |
| Distributed SELECT | `{"total":N}` |
| MapReduce | Word frequency map |

---

## 📁 Project Structure

```
distributed-database-system/
│
├── master/                    # Go Master Node
│   ├── main.go
│   ├── api/
│   │   ├── handlers.go
│   │   ├── router.go
│   │   └── auth.go
│   ├── coordinator/
│   ├── metadata/
│   ├── sharding/
│   ├── replication/
│   ├── health/
│   ├── election/
│   └── ui/
│       └── templates/
│           └── index.html
│
├── reducer/                   # Go Reducer Node
│   ├── main.go
│   ├── aggregator/
│   └── merge/
│
├── worker-go/                 # Go Worker Node
│   └── main.go
│
├── worker-py/                 # Python Worker Node
│   ├── app.py
│   ├── storage.py
│   ├── executor.py
│   └── requirements.txt
│
├── client/                    # Client examples
│   ├── client.go
│   └── demo_workflow.sh
│
├── metadata-db/
│   └── schema.sql
│
├── config.yaml.example
├── README.md
└── .gitignore
```

---

## 🔧 Troubleshooting

| Issue | Solution |
|---|---|
| Connection refused | Ensure MySQL is running on the configured port |
| Worker shows DEAD | Check worker process is running; increase timeout |
| Authentication failed | Verify API key matches `config.yaml` |
| Table not found | Create the table through master first |
| MapReduce returns error | Check reducer is running on port 8090 |

### Port Requirements

| Component | Port | Protocol |
|---|---|---|
| Master | 8080 | HTTP |
| Backup Master | 8081 | HTTP |
| Go Worker | 8081 | HTTP |
| Python Worker | 8082 | HTTP |
| Reducer | 8090 | HTTP |
| MySQL | 3306 | TCP |

---

## 📚 Academic Concepts Demonstrated

- ✅ Distributed Systems Architecture
- ✅ Sharding / Partitioning
- ✅ Consistent Hashing
- ✅ Replication & Fault Tolerance
- ✅ Leader Election
- ✅ MapReduce Pattern
- ✅ Parallel Processing
- ✅ Heterogeneous Computing
- ✅ Metadata Management
- ✅ Health Monitoring & Heartbeats

---

## 🤝 Contributing

This is an academic project. For suggestions or improvements, please open an issue or submit a pull request.

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

## 👨‍💻 Author

**Mohamed Mosbah**

- 📧 Email: [mohamedmosbah3017@gmail.com](mailto:mohamedmosbah3017@gmail.com)
- 🐙 GitHub: [@elsaawyy](https://github.com/elsaawyy)

---

## 🙏 Acknowledgments

Inspired by Google MapReduce, Apache Hadoop, and Apache Spark.
Built with Go, Python, Flask, MySQL, and HTMX.

---

> Built with ❤️ for Academic Excellence