package coordinator

import (
    "bytes"
    "encoding/json"
    "net/http"
)

type MapTask struct {
    JobID      string `json:"job_id"`
    ChunkID    string `json:"chunk_id"`
    ChunkData  []byte `json:"chunk_data"`
    MapFunc    string `json:"map_func"`
    ReducerURL string `json:"reducer_url"`
}

func (c *Coordinator) DispatchMapTask(workerURL string, task MapTask) error {
    payload, err := json.Marshal(task)
    if err != nil {
        return err
    }
    resp, err := http.Post(workerURL+"/map", "application/json", bytes.NewReader(payload))
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    return nil
}