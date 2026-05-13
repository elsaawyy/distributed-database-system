#!/bin/bash

echo "=== Testing Distributed Database ==="

# Test 1: Create database
echo "Test 1: Create database"
curl -X POST http://localhost:8080/v1/db/create \
  -H "X-API-Key: my-secret-key-123" \
  -d '{"name":"testdb"}'

# Test 2: Create table
echo "Test 2: Create table"
curl -X POST http://localhost:8080/v1/table/create \
  -H "X-API-Key: my-secret-key-123" \
  -d '{"db_name":"testdb","table_name":"users","schema":"id INT PRIMARY KEY, name VARCHAR(100), age INT","shard_key":"id"}'

# Test 3: Insert multiple rows (should distribute across workers)
echo "Test 3: Insert 10 rows"
curl -X POST http://localhost:8080/v1/insert \
  -H "X-API-Key: my-secret-key-123" \
  -d '{"db_name":"testdb","table_name":"users","rows":[{"id":1,"name":"User1","age":20},{"id":2,"name":"User2","age":25},{"id":3,"name":"User3","age":30},{"id":4,"name":"User4","age":35},{"id":5,"name":"User5","age":40}]}'

# Test 4: Verify distribution
echo "Test 4: Check data distribution"
mysql -u root -P 3309 -e "SELECT COUNT(*) as worker1_count FROM worker1_db.users"
mysql -u root -P 3309 -e "SELECT COUNT(*) as worker2_count FROM worker2_db.users"

# Test 5: Distributed SELECT
echo "Test 5: Distributed COUNT query"
curl -X POST http://localhost:8080/v1/select \
  -H "X-API-Key: my-secret-key-123" \
  -d '{"db_name":"testdb","table_name":"users","query":"SELECT COUNT(*) FROM users"}'

# Test 6: Test failover (kill one worker)
echo "Test 6: Failover test - kill Python worker"
echo "Manually stop Python worker and run SELECT again"

# Test 7: MapReduce word count
echo "Test 7: MapReduce word count"
echo "hello world hello distributed system" > /tmp/test.txt
curl -X POST http://localhost:8080/v1/mapreduce/wordcount \
  -H "X-API-Key: my-secret-key-123" \
  -F "file=@/tmp/test.txt"