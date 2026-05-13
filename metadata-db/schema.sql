-- Track MapReduce jobs
CREATE TABLE IF NOT EXISTS mapreduce_jobs (
    job_id VARCHAR(128) PRIMARY KEY,
    job_type VARCHAR(64) NOT NULL,
    status ENUM('pending', 'running', 'completed', 'failed') DEFAULT 'pending',
    input_file VARCHAR(512),
    total_chunks INT DEFAULT 0,
    completed_chunks INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    result TEXT
);

-- Track chunks for MapReduce
CREATE TABLE IF NOT EXISTS chunks (
    chunk_id VARCHAR(128) PRIMARY KEY,
    job_id VARCHAR(128) NOT NULL,
    worker_id VARCHAR(128) NOT NULL,
    chunk_index INT NOT NULL,
    data_size INT,
    status ENUM('stored', 'processed', 'failed') DEFAULT 'stored',
    FOREIGN KEY (job_id) REFERENCES mapreduce_jobs(job_id),
    FOREIGN KEY (worker_id) REFERENCES workers(worker_id)
);

-- Track shard replicas with status
ALTER TABLE shards ADD COLUMN status ENUM('active', 'failed', 'recovering') DEFAULT 'active';
ALTER TABLE shards ADD COLUMN last_sync TIMESTAMP NULL;