package health

import (
	"errors"
	"log"
	"net/http"
	"sync"
	"time"
)

// WorkerInfo represents the state and metadata of a worker node.
type WorkerInfo struct {
	ID       string    `json:"id"`
	URL      string    `json:"url"`
	Tech     string    `json:"tech"` // e.g., "go", "python", "nodejs"
	LastSeen time.Time `json:"last_seen"`
	Alive    bool      `json:"alive"`
}

// Monitor periodically checks health of all registered workers.
type Monitor struct {
	workers           map[string]WorkerInfo
	mu                sync.RWMutex
	heartbeatInterval time.Duration
	stopCh            chan struct{}
}

// NewMonitor creates and initialises a new health monitor.
func NewMonitor(workerMap map[string]WorkerInfo, interval time.Duration) *Monitor {
	// Create a deep copy of the worker map
	workersCopy := make(map[string]WorkerInfo)
	for id, w := range workerMap {
		w.Alive = true
		w.LastSeen = time.Now()
		workersCopy[id] = w
	}

	return &Monitor{
		workers:           workersCopy,
		heartbeatInterval: interval,
		stopCh:            make(chan struct{}),
	}
}

// Start begins the periodic health checking loop.
func (m *Monitor) Start() {
	ticker := time.NewTicker(m.heartbeatInterval)
	log.Printf("Health monitor started, checking every %v", m.heartbeatInterval)

	go func() {
		for {
			select {
			case <-ticker.C:
				m.checkAll()
			case <-m.stopCh:
				ticker.Stop()
				log.Println("Health monitor stopped")
				return
			}
		}
	}()
}

// Stop terminates the health monitor.
func (m *Monitor) Stop() {
	close(m.stopCh)
}

// checkAll checks the health of all workers concurrently.
func (m *Monitor) checkAll() {
	m.mu.RLock()
	workersCopy := make([]WorkerInfo, 0, len(m.workers))
	for _, w := range m.workers {
		workersCopy = append(workersCopy, w)
	}
	m.mu.RUnlock()

	var wg sync.WaitGroup
	for _, w := range workersCopy {
		wg.Add(1)
		go m.checkWorker(w, &wg)
	}
	wg.Wait()
}

// checkWorker performs a single health check on a given worker.
func (m *Monitor) checkWorker(w WorkerInfo, wg *sync.WaitGroup) {
	defer wg.Done()

	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(w.URL + "/health")
	alive := err == nil && resp != nil && resp.StatusCode == http.StatusOK
	if resp != nil {
		resp.Body.Close()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	oldStatus := m.workers[w.ID].Alive
	updated := m.workers[w.ID]
	updated.Alive = alive
	if alive {
		updated.LastSeen = time.Now()
	}
	m.workers[w.ID] = updated

	// **NEW: If worker came back from dead, trigger re-replication**
	if alive && !oldStatus {
		log.Printf("Worker %s recovered! Triggering re-replication...", w.ID)
		go m.triggerReReplication(w.ID)
	}
}

func (m *Monitor) triggerReReplication(workerID string) {
	// This would re-replicate missing shards to the recovered worker
	log.Printf("Re-replication triggered for worker %s", workerID)
	// Implementation: find shards where this worker should be replica but data is missing
}

// GetAliveWorkers returns a list of workers currently marked as alive.
func (m *Monitor) GetAliveWorkers() []WorkerInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var alive []WorkerInfo
	for _, w := range m.workers {
		if w.Alive {
			alive = append(alive, w)
		}
	}
	return alive
}

// GetAllWorkers returns all workers (both alive and dead).
func (m *Monitor) GetAllWorkers() []WorkerInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	all := make([]WorkerInfo, 0, len(m.workers))
	for _, w := range m.workers {
		all = append(all, w)
	}
	return all
}

// GetWorkerByID returns a worker by its ID, if it exists.
func (m *Monitor) GetWorkerByID(id string) (WorkerInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	w, ok := m.workers[id]
	return w, ok
}

// UpdateWorkerTech updates the technology field of a worker.
func (m *Monitor) UpdateWorkerTech(id, tech string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if w, ok := m.workers[id]; ok {
		w.Tech = tech
		m.workers[id] = w
		return nil
	}
	return errors.New("worker not found")
}

func (m *Monitor) IsWorkerAlive(workerURL string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, w := range m.workers {
		if w.URL == workerURL && w.Alive {
			return true
		}
	}
	return false
}

func (m *Monitor) IsWorkerAliveByID(workerID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if w, ok := m.workers[workerID]; ok {
		return w.Alive
	}
	return false
}
