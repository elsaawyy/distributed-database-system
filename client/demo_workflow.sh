#!/bin/bash

# Demo Workflow Script for Distributed Database System
# This script demonstrates the complete functionality

MASTER_URL="http://localhost:8080"
API_KEY="my-secret-key-123"

echo "========================================="
echo "  Distributed Database System Demo"
echo "========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to make API calls
call_api() {
    local method=$1
    local endpoint=$2
    local data=$3
    
    curl -s -X $method "$MASTER_URL$endpoint" \
        -H "X-API-Key: $API_KEY" \
        -H "Content-Type: application/json" \
        -d "$data"
}

echo -e "${BLUE}1. Checking master health...${NC}"
curl -s "$MASTER_URL/v1/health" | jq '.'
echo ""

echo -e "${BLUE}2. Creating database 'demo_db'...${NC}"
call_api "POST" "/v1/db/create" '{"name":"demo_db"}'
echo ""

echo -e "${BLUE}3. Creating table 'users'...${NC}"
call_api "POST" "/v1/table/create" '{
    "db_name": "demo_db",
    "table_name": "users",
    "schema": "id INT PRIMARY KEY, name VARCHAR(100), age INT, email VARCHAR(100)",
    "shard_key": "id"
}'
echo ""

echo -e "${BLUE}4. Inserting data into 'users'...${NC}"
call_api "POST" "/v1/insert" '{
    "db_name": "demo_db",
    "table_name": "users",
    "rows": [
        {"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "age": 25, "email": "bob@example.com"},
        {"id": 3, "name": "Charlie", "age": 35, "email": "charlie@example.com"},
        {"id": 4, "name": "Diana", "age": 28, "email": "diana@example.com"},
        {"id": 5, "name": "Eve", "age": 32, "email": "eve@example.com"}
    ]
}'
echo ""

echo -e "${BLUE}5. Running distributed SELECT query (COUNT)...${NC}"
call_api "POST" "/v1/select" '{
    "db_name": "demo_db",
    "table_name": "users",
    "query": "SELECT COUNT(*) FROM users"
}'
echo ""

echo -e "${BLUE}6. Creating table 'products'...${NC}"
call_api "POST" "/v1/table/create" '{
    "db_name": "demo_db",
    "table_name": "products",
    "schema": "product_id INT PRIMARY KEY, name VARCHAR(100), price DECIMAL(10,2), stock INT",
    "shard_key": "product_id"
}'
echo ""

echo -e "${BLUE}7. Inserting products data...${NC}"
call_api "POST" "/v1/insert" '{
    "db_name": "demo_db",
    "table_name": "products",
    "rows": [
        {"product_id": 101, "name": "Laptop", "price": 999.99, "stock": 10},
        {"product_id": 102, "name": "Mouse", "price": 29.99, "stock": 50},
        {"product_id": 103, "name": "Keyboard", "price": 79.99, "stock": 30},
        {"product_id": 104, "name": "Monitor", "price": 299.99, "stock": 15}
    ]
}'
echo ""

echo -e "${BLUE}8. Running distributed query (AVG price)...${NC}"
call_api "POST" "/v1/select" '{
    "db_name": "demo_db",
    "table_name": "products",
    "query": "SELECT AVG(price) as average_price FROM products"
}'
echo ""

echo -e "${BLUE}9. Running distributed query (total stock value)...${NC}"
call_api "POST" "/v1/select" '{
    "db_name": "demo_db",
    "table_name": "products",
    "query": "SELECT SUM(price * stock) as total_inventory_value FROM products"
}'
echo ""

echo -e "${BLUE}10. Checking active workers...${NC}"
curl -s "$MASTER_URL/v1/workers" -H "X-API-Key: $API_KEY" | jq '.'
echo ""

# Create a test file for MapReduce
echo -e "${YELLOW}11. Creating test file for MapReduce...${NC}"
cat > /tmp/sample.txt << EOF
hello world this is a distributed database system
hello from the master node
world of distributed systems
this is a map reduce example
hello hello hello
distributed systems are awesome
EOF

echo "Sample file created at /tmp/sample.txt"
echo ""

echo -e "${BLUE}12. Uploading file for MapReduce...${NC}"
curl -X POST "$MASTER_URL/v1/upload-file" \
    -H "X-API-Key: $API_KEY" \
    -F "file=@/tmp/sample.txt"
echo ""

echo -e "${GREEN}✅ Demo workflow completed successfully!${NC}"
echo ""
echo "========================================="
echo "  Distributed System Statistics"
echo "========================================="
echo "- Master Node: Running on port 8080"
echo "- Reducer: Running on port 8090"
echo "- Go Worker: Running on port 8081"
echo "- Python Worker: Running on port 8082"
echo "- Database: MySQL on port 3309"
echo "- Sharding: Consistent hashing with 64 virtual nodes"
echo "- Replication Factor: 2"
echo "========================================="