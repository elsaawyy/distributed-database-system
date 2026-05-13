from flask import Flask, request, jsonify
import yaml
import logging
import requests
import threading
import time
import mysql.connector

from storage import MySQLStorage
from executor import LocalExecutor

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Initialize storage and executor
storage = MySQLStorage(
    host=config['mysql']['host'],
    port=config['mysql']['port'],
    user=config['mysql']['user'],
    password=config['mysql']['password'],
    database=config['mysql']['database']
)
executor = LocalExecutor(storage)

worker_id = config['server']['worker_id']
technology = config['server']['technology']
reducer_url = config['reducer']['url']

def get_db_connection():
    """Get a fresh database connection"""
    return mysql.connector.connect(
        host=config['mysql']['host'],
        port=config['mysql']['port'],
        user=config['mysql']['user'],
        password=config['mysql']['password'],
        database='worker2_db'
    )

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'alive',
        'worker_id': worker_id,
        'technology': technology,
        'timestamp': int(time.time())
    })

@app.route('/execute_select', methods=['POST'])
def execute_select():
    data = request.json
    logger.info(f"Worker {worker_id} executing SELECT for job {data.get('job_id')}")
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        query = data.get('query')
        cursor.execute(query)
        
        # Check if it's a COUNT query or SELECT *
        if 'COUNT(' in query.upper():
            # Handle COUNT query
            result = cursor.fetchone()
            count = result.get('COUNT(*)', 0) if result else 0
            
            partial = {
                'worker_id': worker_id,
                'technology': technology,
                'count': count,
                'job_id': data.get('job_id')
            }
        else:
            # Handle SELECT * query - return all rows
            rows = cursor.fetchall()
            # Convert datetime objects to string for JSON serialization
            for row in rows:
                for key, value in row.items():
                    if hasattr(value, 'isoformat'):
                        row[key] = value.isoformat()
            
            partial = {
                'worker_id': worker_id,
                'technology': technology,
                'rows': rows,
                'count': len(rows),
                'job_id': data.get('job_id')
            }
        
        cursor.close()
        conn.close()
        
        reducer_endpoint = data.get('reducer_url', reducer_url)
        send_to_reducer(reducer_endpoint, data.get('job_id'), partial)
        
        return jsonify({'status': 'processing', 'job_id': data.get('job_id')})
    except Exception as e:
        logger.error(f"Error executing select: {e}")
        if conn:
            conn.close()
        return jsonify({'error': str(e)}), 500


@app.route('/execute_aggregate', methods=['POST'])
def execute_aggregate():
    data = request.json
    result = executor.execute_aggregate(data['query'])
    return jsonify(result)

@app.route('/insert', methods=['POST'])
def insert():
    data = request.json
    logger.info(f"Worker {worker_id} inserting {len(data.get('rows', []))} rows")
    
    # Force use worker2_db
    actual_db = 'worker2_db'
    
    try:
        # Switch to worker2_db
        storage.execute_update(f"USE {actual_db}")
        
        rows_inserted = executor.execute_insert(data['table_name'], data['rows'])
        return jsonify({
            'status': 'inserted',
            'count': rows_inserted,
            'worker': worker_id
        })
    except Exception as e:
        logger.error(f"Error inserting: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/update', methods=['POST'])
def update():
    data = request.json
    affected = executor.execute_update(data['query'])
    return jsonify({'status': 'updated', 'affected': affected})

@app.route('/delete', methods=['POST'])
def delete():
    data = request.json
    affected = executor.execute_delete(data['query'])
    return jsonify({'status': 'deleted', 'affected': affected})

@app.route('/store_chunk', methods=['POST'])
def store_chunk():
    data = request.json
    storage.store_chunk(data['chunk_id'], data['data'])
    return jsonify({'status': 'stored', 'chunk_id': data['chunk_id']})

@app.route('/map', methods=['POST'])
def map_function():
    data = request.json
    logger.info(f"Worker {worker_id} executing map function: {data.get('map_func')}")
    
    if data['map_func'] == 'wordcount':
        result = executor.map_word_count(data['chunk_data'])
    else:
        result = {'error': f'Unknown map function: {data["map_func"]}'}
    
    reducer_endpoint = data.get('reducer_url', reducer_url)
    send_to_reducer(reducer_endpoint, data['job_id'], result)
    
    return jsonify({'status': 'completed', 'job_id': data['job_id']})

@app.route('/create_table', methods=['POST'])
def create_table():
    data = request.json
    table_name = data.get('table_name')
    schema = data.get('schema')
    
    logger.info(f"Creating table {table_name}")
    
    # Force use worker2_db instead of the logical db_name
    actual_db = 'worker2_db'
    
    # Create database if not exists
    storage.execute_update(f"CREATE DATABASE IF NOT EXISTS {actual_db}")
    
    # Use the database
    storage.execute_update(f"USE {actual_db}")
    
    # Create table
    success = storage.create_table(table_name, schema)
    
    if success:
        return jsonify({"status": "created"})
    else:
        return jsonify({"error": "Failed to create table"}), 500

def send_to_reducer(reducer_url, job_id, partial):
    try:
        payload = {
            'job_id': job_id,
            'partial': partial
        }
        response = requests.post(f"{reducer_url}/reduce/add_partial", json=payload)
        if response.status_code != 200:
            logger.error(f"Failed to send to reducer: {response.status_code}")
    except Exception as e:
        logger.error(f"Error sending to reducer: {e}")

def start_heartbeat():
    """Send heartbeat to master every 5 seconds"""
    master_url = config['master']['url']
    api_key = config['master']['api_key']
    
    while True:
        try:
            headers = {'X-API-Key': api_key}
            response = requests.get(f"{master_url}/v1/health", headers=headers)
            if response.status_code == 200:
                logger.debug(f"Heartbeat sent to master")
        except Exception as e:
            logger.error(f"Heartbeat failed: {e}")
        time.sleep(5)

if __name__ == '__main__':
    # Start heartbeat in background thread
    heartbeat_thread = threading.Thread(target=start_heartbeat, daemon=True)
    heartbeat_thread.start()
    
    logger.info(f"Python Worker {worker_id} starting on port {config['server']['port']}")
    app.run(host='0.0.0.0', port=config['server']['port'], debug=False)