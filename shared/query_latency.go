package shared

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type QueryLatency struct {
	key   string
	start time.Time
}

type QueryLatencyRecorder struct {
	file *os.File
	mtx  *sync.Mutex
}

func NewQueryLatencyRecorder(filename string) *QueryLatencyRecorder {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	mtx := &sync.Mutex{}

	return &QueryLatencyRecorder{
		file: file,
		mtx:  mtx,
	}
}

func (q *QueryLatencyRecorder) Record(latency QueryLatency) {
	text := fmt.Sprintf("%s,%s,%d\n",
		latency.start.Format(time.RFC3339),
		latency.key,
		time.Since(latency.start).Milliseconds(),
	)
	log.Printf(text)

	q.mtx.Lock()
	_, err := q.file.WriteString(text)
	q.mtx.Unlock()
	if err != nil {
		panic(err)
	}
}

func (q *QueryLatencyRecorder) Close() {
	q.file.Close()
}
