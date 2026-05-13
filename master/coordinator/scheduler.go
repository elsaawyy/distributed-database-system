package coordinator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"distributed-db/master/health"
	"distributed-db/master/metadata"
	"distributed-db/master/replication"
	"distributed-db/master/sharding"
)

type Coordinator struct {
	metaManager    *metadata.Manager
	healthMon      *health.Monitor
	replicaMgr     *replication.ReplicaManager
	consistentHash *sharding.ConsistentHash
	reducerURL     string
	replicaFactor  int

	reduceResults map[string]interface{}
	mu            sync.RWMutex
}

func NewCoordinator(metaMgr *metadata.Manager, healthMon *health.Monitor,
	replicaMgr *replication.ReplicaManager, ch *sharding.ConsistentHash,
	reducerURL string, replicaFactor int) *Coordinator {
	return &Coordinator{
		metaManager:    metaMgr,
		healthMon:      healthMon,
		replicaMgr:     replicaMgr,
		consistentHash: ch,
		reducerURL:     reducerURL,
		replicaFactor:  replicaFactor,
		reduceResults:  make(map[string]interface{}),
	}
}

func (c *Coordinator) DistributedSelect(table *metadata.Table, query string, workers []health.WorkerInfo) string {
	jobID := fmt.Sprintf("select-%d", time.Now().UnixNano())
	initReq := map[string]interface{}{
		"job_id": jobID,
		"type":   "sql_aggregation",
	}
	c.callReducer("/reduce/init", initReq)

	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Add(1)
		go func(wkr health.WorkerInfo) {
			defer wg.Done()
			reqBody := map[string]interface{}{
				"db_name":     table.DBName,
				"table_name":  table.TableName,
				"query":       query,
				"job_id":      jobID,
				"reducer_url": c.reducerURL,
			}
			data, _ := json.Marshal(reqBody)
			http.Post(wkr.URL+"/execute_select", "application/json", bytes.NewReader(data))
		}(w)
	}
	wg.Wait()

	time.Sleep(2 * time.Second)
	return jobID
}

func (c *Coordinator) GetReduceResult(jobID string) interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.reduceResults[jobID]
}

func (c *Coordinator) callReducer(endpoint string, payload interface{}) {
	data, _ := json.Marshal(payload)
	http.Post(c.reducerURL+endpoint, "application/json", bytes.NewReader(data))
}

func (c *Coordinator) GetReducerURL() string {
	return c.reducerURL
}
