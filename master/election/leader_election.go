package election

import (
	"database/sql"
	"log"
	"sync/atomic"
	"time"
)

// LeaderElection uses MySQL advisory locks to elect a single active master.
type LeaderElection struct {
	db       *sql.DB
	isActive atomic.Bool
	stopCh   chan struct{}
	lockName string
}

// NewLeaderElection creates a new leader election instance.
func NewLeaderElection(db *sql.DB) *LeaderElection {
	return &LeaderElection{
		db:       db,
		stopCh:   make(chan struct{}),
		lockName: "master_leader",
	}
}

// TryAcquire attempts to acquire the advisory lock.
// Returns true if the lock was obtained, false otherwise.
func (e *LeaderElection) TryAcquire() bool {
	var result int
	// GET_LOCK(name, timeout) returns 1 if lock acquired, 0 if timeout, NULL on error
	err := e.db.QueryRow("SELECT GET_LOCK(?, 10)", e.lockName).Scan(&result)
	if err != nil {
		log.Printf("Error acquiring lock: %v", err)
		return false
	}
	if result != 1 {
		return false
	}
	e.isActive.Store(true)
	log.Println("Acquired master leadership lock")
	return true
}

// KeepAliveLoop periodically renews the lock to maintain leadership.
// Must be called after becoming active.
func (e *LeaderElection) KeepAliveLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// Re-acquire lock to refresh its timeout
			var result int
			err := e.db.QueryRow("SELECT GET_LOCK(?, 10)", e.lockName).Scan(&result)
			if err != nil || result != 1 {
				log.Println("Lost leadership - could not renew lock")
				e.isActive.Store(false)
				return
			}
			log.Println("Leadership renewed")
		case <-e.stopCh:
			return
		}
	}
}

// AcquireLoop runs in a goroutine for a standby master.
// It periodically attempts to acquire the lock until it becomes active.
func (e *LeaderElection) AcquireLoop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if e.TryAcquire() {
				log.Println("Standby master became active")
				// Once active, start the keep‑alive loop
				go e.KeepAliveLoop()
				return
			}
		case <-e.stopCh:
			return
		}
	}
}

// IsActive returns true if this instance holds the leader lock.
func (e *LeaderElection) IsActive() bool {
	return e.isActive.Load()
}

// Stop releases resources and stops the election loops.
func (e *LeaderElection) Stop() {
	close(e.stopCh)
	// Optionally release the lock
	if e.IsActive() {
		_, _ = e.db.Exec("SELECT RELEASE_LOCK(?)", e.lockName)
		log.Println("Released master leadership lock")
	}
}
