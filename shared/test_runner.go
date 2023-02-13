package shared

import (
	"fmt"
	"log"
	"time"
)

type InsertFunc func([]Packet) error

type DownSamplingFunc func(start time.Time, end time.Time) error

func RunTest(
	recorder *QueryLatencyRecorder,
	prefix string,
	insertFunc InsertFunc,
	downSamplingFunc DownSamplingFunc,
) {
	ticker1sec := time.NewTicker(1 * time.Second)
	ticker5min := time.NewTicker(5 * time.Minute)
	exitTimer := time.NewTimer(1 * time.Hour)

	for {
		select {
		case <-ticker1sec.C:
			go func() {
				performanceStart := time.Now()
				packets := GeneratePackets(time.Now(), 20000, 5)
				err := insertFunc(packets)
				if err != nil {
					log.Fatal(err)
				}
				go recorder.Record(QueryLatency{
					key:   fmt.Sprintf("%s.insert", prefix),
					start: performanceStart,
				})
			}()
		case <-ticker5min.C:
			go func() {
				performanceStart := time.Now()

				start := time.Unix((time.Now().Unix()/(15*60))*15*60, 0)
				end := start.Add(15 * time.Minute)

				err := downSamplingFunc(start, end)
				if err != nil {
					log.Fatal(err)
				}
				go recorder.Record(QueryLatency{
					key:   fmt.Sprintf("%s.downsampling", prefix),
					start: performanceStart,
				})
			}()
		case <-exitTimer.C:
			return
		}
	}
}
