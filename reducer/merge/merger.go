package merge

import (
	"sort"
	"sync"
)

type MergeWorker struct {
	ID       int
	JobID    string
	Data     []interface{}
	Result   interface{}
	IsActive bool
}

type Merger struct {
	workers     map[int]*MergeWorker
	workersLock sync.RWMutex
	maxWorkers  int
}

func NewMerger(maxWorkers int) *Merger {
	return &Merger{
		workers:    make(map[int]*MergeWorker),
		maxWorkers: maxWorkers,
	}
}

func (m *Merger) MergeResults(results []interface{}) interface{} {
	if len(results) == 0 {
		return nil
	}

	// Try to detect result type
	switch results[0].(type) {
	case map[string]interface{}:
		return m.mergeMaps(results)
	case []interface{}:
		return m.mergeSlices(results)
	default:
		return results
	}
}

func (m *Merger) mergeMaps(results []interface{}) map[string]interface{} {
	merged := make(map[string]interface{})

	for _, res := range results {
		if resMap, ok := res.(map[string]interface{}); ok {
			for key, value := range resMap {
				// If key already exists and both are numbers, sum them
				if existing, exists := merged[key]; exists {
					switch v := value.(type) {
					case float64:
						if e, ok := existing.(float64); ok {
							merged[key] = e + v
						}
					case int:
						if e, ok := existing.(int); ok {
							merged[key] = e + v
						}
					default:
						merged[key] = value
					}
				} else {
					merged[key] = value
				}
			}
		}
	}

	return merged
}

func (m *Merger) mergeSlices(results []interface{}) []interface{} {
	merged := make([]interface{}, 0)

	for _, res := range results {
		if resSlice, ok := res.([]interface{}); ok {
			merged = append(merged, resSlice...)
		} else {
			merged = append(merged, res)
		}
	}

	return merged
}

func (m *Merger) SortResults(results interface{}) interface{} {
	switch v := results.(type) {
	case map[string]int:
		// Sort word count by frequency
		type kv struct {
			Key   string
			Value int
		}

		var sorted []kv
		for key, value := range v {
			sorted = append(sorted, kv{key, value})
		}

		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Value > sorted[j].Value
		})

		return sorted
	default:
		return results
	}
}

func (m *Merger) ParallelMerge(jobID string, dataChunks [][]interface{}) []interface{} {
	var wg sync.WaitGroup
	results := make([]interface{}, len(dataChunks))

	for i, chunk := range dataChunks {
		wg.Add(1)
		go func(idx int, data []interface{}) {
			defer wg.Done()
			results[idx] = m.MergeResults(data)
		}(i, chunk)
	}

	wg.Wait()

	// Final merge
	return results
}
