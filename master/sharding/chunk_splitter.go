package sharding

import (
	"errors"
)

type DataChunk struct {
	ID       string
	FileName string
	Data     []byte
	Offset   int64
	Size     int
}

type ChunkSplitter struct {
	ChunkSize int // bytes
}

func NewChunkSplitter(chunkSize int) *ChunkSplitter {
	return &ChunkSplitter{ChunkSize: chunkSize}
}

func (cs *ChunkSplitter) Split(data []byte, fileName string) ([]DataChunk, error) {
	if len(data) == 0 {
		return nil, errors.New("empty data")
	}
	var chunks []DataChunk
	for offset := 0; offset < len(data); offset += cs.ChunkSize {
		end := offset + cs.ChunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := DataChunk{
			ID:       fileName + "-" + string(rune(offset)),
			FileName: fileName,
			Data:     data[offset:end],
			Offset:   int64(offset),
			Size:     end - offset,
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}
