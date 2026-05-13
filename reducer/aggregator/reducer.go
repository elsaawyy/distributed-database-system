package aggregator

import (
	"log"
	"sync"
)

type ReduceJob struct {
	JobID      string
	JobType    string
	Results    []interface{}
	Final      interface{}
	mu         sync.RWMutex
	IsComplete bool
}

type Aggregator struct {
	jobs     map[string]*ReduceJob
	jobsLock sync.RWMutex
}

func NewAggregator() *Aggregator {
	return &Aggregator{
		jobs: make(map[string]*ReduceJob),
	}
}

func (a *Aggregator) InitJob(jobID, jobType string) {
	a.jobsLock.Lock()
	defer a.jobsLock.Unlock()

	a.jobs[jobID] = &ReduceJob{
		JobID:      jobID,
		JobType:    jobType,
		Results:    []interface{}{},
		IsComplete: false,
	}
	log.Printf("Initialized reduce job: %s (type: %s)", jobID, jobType)
}

func (a *Aggregator) AddPartial(jobID string, partial interface{}) bool {
	a.jobsLock.RLock()
	job, exists := a.jobs[jobID]
	a.jobsLock.RUnlock()

	if !exists {
		log.Printf("Job %s not found", jobID)
		return false
	}

	job.mu.Lock()
	defer job.mu.Unlock()

	job.Results = append(job.Results, partial)
	log.Printf("Added partial to job %s (total: %d)", jobID, len(job.Results))
	return true
}

func (a *Aggregator) AggregateSQL(jobID string) interface{} {
	a.jobsLock.RLock()
	job, exists := a.jobs[jobID]
	a.jobsLock.RUnlock()

	if !exists {
		return nil
	}

	job.mu.RLock()
	defer job.mu.RUnlock()

	total := 0
	for _, result := range job.Results {
		if resMap, ok := result.(map[string]interface{}); ok {
			if count, ok := resMap["count"].(float64); ok {
				total += int(count)
			}
			if count, ok := resMap["count"].(int); ok {
				total += count
			}
		}
	}

	final := map[string]interface{}{
		"total": total,
		"type":  "sql_aggregation",
	}

	job.Final = final
	return final
}

func (a *Aggregator) AggregateWordCount(jobID string) interface{} {
	a.jobsLock.RLock()
	job, exists := a.jobs[jobID]
	a.jobsLock.RUnlock()

	if !exists {
		return nil
	}

	job.mu.RLock()
	defer job.mu.RUnlock()

	wordCount := make(map[string]int)
	for _, result := range job.Results {
		switch v := result.(type) {
		case map[string]interface{}:
			for word, count := range v {
				switch c := count.(type) {
				case float64:
					wordCount[word] += int(c)
				case int:
					wordCount[word] += c
				}
			}
		case map[string]int:
			for word, count := range v {
				wordCount[word] += count
			}
		}
	}

	job.Final = wordCount
	return wordCount
}

func (a *Aggregator) AggregateDefault(jobID string) interface{} {
	a.jobsLock.RLock()
	job, exists := a.jobs[jobID]
	a.jobsLock.RUnlock()

	if !exists {
		return nil
	}

	job.mu.RLock()
	defer job.mu.RUnlock()

	return job.Results
}

func (a *Aggregator) GetResult(jobID string) interface{} {
	a.jobsLock.RLock()
	job, exists := a.jobs[jobID]
	a.jobsLock.RUnlock()

	if !exists {
		return nil
	}

	job.mu.RLock()
	defer job.mu.RUnlock()

	if job.Final != nil {
		return job.Final
	}

	switch job.JobType {
	case "sql_aggregation":
		return a.AggregateSQL(jobID)
	case "sql_select":
		return a.AggregateSelect(jobID)
	case "wordcount":
		return a.AggregateWordCount(jobID)
	default:
		return a.AggregateDefault(jobID)
	}
}

func (a *Aggregator) GetAllJobs() []string {
	a.jobsLock.RLock()
	defer a.jobsLock.RUnlock()

	jobs := make([]string, 0, len(a.jobs))
	for jobID := range a.jobs {
		jobs = append(jobs, jobID)
	}
	return jobs
}

func (a *Aggregator) AggregateSelect(jobID string) interface{} {
	a.jobsLock.RLock()
	job, exists := a.jobs[jobID]
	a.jobsLock.RUnlock()

	if !exists {
		return nil
	}

	job.mu.RLock()
	defer job.mu.RUnlock()

	// Combine all rows from all workers
	allRows := make([]interface{}, 0)
	for _, result := range job.Results {
		if resMap, ok := result.(map[string]interface{}); ok {
			if rows, ok := resMap["rows"].([]interface{}); ok {
				allRows = append(allRows, rows...)
			}
		}
	}

	return map[string]interface{}{
		"rows":  allRows,
		"count": len(allRows),
		"type":  "sql_select",
	}
}
